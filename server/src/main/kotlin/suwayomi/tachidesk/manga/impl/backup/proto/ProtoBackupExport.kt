package suwayomi.tachidesk.manga.impl.backup.proto

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import android.app.Application
import android.content.Context
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow.DROP_OLDEST
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import okio.Buffer
import okio.Sink
import okio.buffer
import okio.gzip
import org.jetbrains.exposed.sql.transactions.transaction
import suwayomi.tachidesk.manga.impl.backup.BackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupCategoryHandler
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupGlobalMetaHandler
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupMangaHandler
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupPreferenceHandler
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupSettingsHandler
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupSourceHandler
import suwayomi.tachidesk.manga.impl.backup.proto.models.Backup
import suwayomi.tachidesk.server.ApplicationDirs
import suwayomi.tachidesk.server.serverConfig
import suwayomi.tachidesk.util.HAScheduler
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import uy.kohesive.injekt.injectLazy
import java.io.File
import java.io.InputStream
import java.util.Date
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.days

object ProtoBackupExport : ProtoBackupBase() {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private val logger = KotlinLogging.logger { }
    private val applicationDirs: ApplicationDirs by injectLazy()
    private var backupSchedulerJobId: String = ""
    private const val LAST_AUTOMATED_BACKUP_KEY = "lastAutomatedBackup"
    private val preferences = Injekt.get<Application>().getSharedPreferences("server_util", Context.MODE_PRIVATE)
    private const val AUTO_BACKUP_FILENAME = "auto"

    // small helper for timings
    private fun nowMs(): Long = System.nanoTime() / 1_000_000L

    // Progress/state machinery (mirrors import but with 'create' naming)
    sealed class BackupCreateState {
        data object Idle : BackupCreateState()

        data object Success : BackupCreateState()

        data object Failure : BackupCreateState()

        data class CreatingCategories(
            val current: Int,
            val totalManga: Int,
        ) : BackupCreateState()

        data class CreatingMeta(
            val current: Int,
            val totalManga: Int,
        ) : BackupCreateState()

        data class CreatingSettings(
            val current: Int,
            val totalManga: Int,
        ) : BackupCreateState()

        data class CreatingManga(
            val current: Int,
            val totalManga: Int,
            val title: String,
        ) : BackupCreateState()
    }

    private val backupCreateIdToState = ConcurrentHashMap<String, BackupCreateState>()

    val createNotifyFlow = MutableSharedFlow<Unit>(extraBufferCapacity = 1, onBufferOverflow = DROP_OLDEST)

    fun getCreateState(id: String): BackupCreateState? = backupCreateIdToState[id]

    private fun updateCreateState(
        id: String,
        state: BackupCreateState,
    ) {
        backupCreateIdToState[id] = state

        scope.launch {
            createNotifyFlow.emit(Unit)
        }
    }

    private fun cleanupCreateState(id: String) {
        val timer = Timer()
        val delay = 1000L * 60 // 60 seconds

        timer.schedule(
            object : TimerTask() {
                override fun run() {
                    logger.debug { "cleanupCreateState: $id (${getCreateState(id)})" }
                    backupCreateIdToState.remove(id)
                }
            },
            delay,
        )
    }

    init {
        serverConfig.subscribeTo(
            combine(serverConfig.backupInterval, serverConfig.backupTime) { interval, timeOfDay ->
                Pair(
                    interval,
                    timeOfDay,
                )
            },
            ::scheduleAutomatedBackupTask,
        )
    }

    @OptIn(DelicateCoroutinesApi::class)
    fun scheduleAutomatedBackupTask() {
        HAScheduler.descheduleCron(backupSchedulerJobId)

        val areAutomatedBackupsDisabled = serverConfig.backupInterval.value == 0
        if (areAutomatedBackupsDisabled) {
            return
        }

        val task = {
            try {
                cleanupAutomatedBackups()
                createAutomatedBackup()
                preferences.edit().putLong(LAST_AUTOMATED_BACKUP_KEY, System.currentTimeMillis()).apply()
            } catch (e: Exception) {
                logger.error(e) { "scheduleAutomatedBackupTask: failed due to" }
            }
        }

        val (backupHour, backupMinute) =
            serverConfig.backupTime.value
                .split(":")
                .map { it.toInt() }
        val backupInterval = serverConfig.backupInterval.value.days

        // trigger last backup in case the server wasn't running on the scheduled time
        val lastAutomatedBackup = preferences.getLong(LAST_AUTOMATED_BACKUP_KEY, 0)
        val wasPreviousBackupTriggered =
            (System.currentTimeMillis() - lastAutomatedBackup) < backupInterval.inWholeMilliseconds
        if (!wasPreviousBackupTriggered) {
            GlobalScope.launch(Dispatchers.IO) {
                task()
            }
        }

        backupSchedulerJobId = HAScheduler.scheduleCron(task, "$backupMinute $backupHour */${backupInterval.inWholeDays} * *", "backup")
    }

    private fun createAutomatedBackup() {
        logger.info { "Creating automated backup..." }

        // use async wrapper so the automated backup is tracked and does not block scheduler
        createBackupAsync(BackupFlags.fromServerConfig())
    }

    private fun cleanupAutomatedBackups() {
        logger.debug { "Cleanup automated backups (ttl= ${serverConfig.backupTTL.value})" }

        val isCleanupDisabled = serverConfig.backupTTL.value == 0
        if (isCleanupDisabled) {
            return
        }

        val automatedBackupDir = File(applicationDirs.automatedBackupRoot)
        if (!automatedBackupDir.isDirectory) {
            return
        }

        automatedBackupDir.listFiles { file -> file.name.startsWith(Backup.getBasename(AUTO_BACKUP_FILENAME)) }?.forEach { file ->
            try {
                cleanupAutomatedBackupFile(file)
            } catch (_: Exception) {
                // ignore, will be retried on next cleanup
            }
        }
    }

    private fun cleanupAutomatedBackupFile(file: File) {
        if (!file.isFile) {
            return
        }

        val lastAccessTime = file.lastModified()
        val isTTLReached =
            System.currentTimeMillis() - lastAccessTime >=
                serverConfig.backupTTL.value.days
                    .coerceAtLeast(1.days)
                    .inWholeMilliseconds
        if (isTTLReached) {
            file.delete()
        }
    }

    /**
     * Synchronous creation (kept for compatibility).
     * Returns InputStream with gzipped proto bytes — unchanged behavior.
     */
    fun createBackup(flags: BackupFlags): InputStream {
        // Create root object

        val backupMangas = BackupMangaHandler.backup(flags)
        val backupSourcePreferences = BackupPreferenceHandler.backup(flags)

        val backup: Backup =
            transaction {
                Backup(
                    BackupMangaHandler.backup(flags),
                    BackupCategoryHandler.backup(flags),
                    BackupSourceHandler.backup(backupMangas, flags),
                    emptyList(),
                    backupSourcePreferences,
                    BackupGlobalMetaHandler.backup(flags),
                    BackupSettingsHandler.backup(flags),
                )
            }

        val byteArray = parser.encodeToByteArray(Backup.serializer(), backup)

        val byteStream = Buffer()
        (byteStream as Sink)
            .gzip()
            .buffer()
            .use { it.write(byteArray) }

        return byteStream.inputStream()
    }

    /**
     * Asynchronous wrapper modeled after ProtoBackupImport pattern, but with 'create' naming.
     * Creates a tracked create job and writes it to a file. Returns an id for the creation job.
     */
    @OptIn(DelicateCoroutinesApi::class)
    fun createBackupAsync(
        flags: BackupFlags,
    ): String {
        val createId = System.currentTimeMillis().toString()

        logger.info { "createBackupAsync($createId): queued" }

        updateCreateState(createId, BackupCreateState.Idle)

        // Launch work on our scope; serialize via createMutex to mirror import
        scope.launch {
            createMutex.withLock {
                try {
                    logger.info { "createBackupAsync($createId): creating..." }
                    updateCreateState(createId, BackupCreateState.Idle)

                    performCreate(createId, flags)
                } catch (e: Exception) {
                    logger.error(e) { "createBackupAsync($createId): failed due to" }
                    updateCreateState(createId, BackupCreateState.Failure)
                } finally {
                    logger.info { "createBackupAsync($createId): finished with state ${getCreateState(createId)}" }
                    cleanupCreateState(createId)
                }
            }
        }

        return createId
    }

    // A private mutex to serialize create jobs (mirrors import)
    private val createMutex = Mutex()

    private fun performCreate(
        id: String,
        flags: BackupFlags,
    ) {
        // timing: measure end-to-end and per-stage times
        val totalStart = nowMs()

        // Stage: fetching / building backup mangas (we pass a progress lambda that updates per-manga)
        val fetchStart = nowMs()
        val backupMangas =
            BackupMangaHandler.backup(flags) { current, total, title ->
                updateCreateState(id, BackupCreateState.CreatingManga(current, total, title))
            }
        val fetchEnd = nowMs()
        val fetchMs = fetchEnd - fetchStart

        updateCreateState(id, BackupCreateState.CreatingCategories(0, backupMangas.size))

        // other pieces of client data
        if (flags.includeClientData) {
            updateCreateState(id, BackupCreateState.CreatingMeta(0, backupMangas.size))
        }

        // Build remaining parts and serialize; after this no DB connection is held during assembly/serialization
        val buildStart = nowMs()
        val backupSourcePreferences = BackupPreferenceHandler.backup(flags)

        val backup: Backup =
            transaction {
                Backup(
                    backupMangas,
                    BackupCategoryHandler.backup(flags),
                    BackupSourceHandler.backup(backupMangas, flags),
                    emptyList(),
                    backupSourcePreferences,
                    BackupGlobalMetaHandler.backup(flags),
                    BackupSettingsHandler.backup(flags),
                )
            }
        val buildEnd = nowMs()
        val buildMs = buildEnd - buildStart

        // serialize & gzip
        val serializeStart = nowMs()
        val byteArray = parser.encodeToByteArray(Backup.serializer(), backup)
        val serializeEnd = nowMs()
        val serializeMs = serializeEnd - serializeStart

        val gzipStart = nowMs()
        val byteStream = Buffer()
        (byteStream as Sink)
            .gzip()
            .buffer()
            .use { it.write(byteArray) }
        val gzipEnd = nowMs()
        val gzipMs = gzipEnd - gzipStart

        // write to file
        val writeStart = nowMs()
        try {
            val automatedBackupDir = File(applicationDirs.automatedBackupRoot)
            automatedBackupDir.mkdirs()

            val backupFile = File(applicationDirs.automatedBackupRoot, Backup.getFilename(AUTO_BACKUP_FILENAME + "_$id"))

            updateCreateState(id, BackupCreateState.CreatingSettings(0, backupMangas.size))
            backupFile.outputStream().use { output -> byteStream.inputStream().copyTo(output) }

            updateCreateState(id, BackupCreateState.Success)
        } catch (e: Exception) {
            logger.error(e) { "performCreate($id): failed writing file" }
            updateCreateState(id, BackupCreateState.Failure)
        }
        val writeEnd = nowMs()
        val writeMs = writeEnd - writeStart

        val totalEnd = nowMs()
        val totalMs = totalEnd - totalStart

        logger.info {
            "createBackup timing (id=$id): fetch=${fetchMs}ms, build=${buildMs}ms, serialize=${serializeMs}ms, gzip=${gzipMs}ms, write=${writeMs}ms, total=${totalMs}ms"
        }
    }
}

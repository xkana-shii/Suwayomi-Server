package suwayomi.tachidesk.manga.impl.backup.proto.handlers

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import suwayomi.tachidesk.manga.impl.chapter.SmartMergeOverrideChange
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorSettings
import suwayomi.tachidesk.manga.impl.chapter.ChapterVisibilityService
import suwayomi.tachidesk.manga.impl.chapter.BookmarkColor
import eu.kanade.tachiyomi.source.model.UpdateStrategy
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.greaterEq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.inSubQuery
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.batchUpsert
import org.jetbrains.exposed.v1.jdbc.insertAndGetId
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.jetbrains.exposed.v1.jdbc.upsert
import suwayomi.tachidesk.graphql.server.primitives.inIds
import suwayomi.tachidesk.manga.impl.CategoryManga
import suwayomi.tachidesk.manga.impl.Chapter.modifyChaptersMetas
import suwayomi.tachidesk.manga.impl.Manga.clearThumbnail
import suwayomi.tachidesk.manga.impl.Manga.modifyMangasMetas
import suwayomi.tachidesk.manga.impl.backup.BackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.SyncRestoreMode
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupChapter
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupHistory
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupManga
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupTracking
import suwayomi.tachidesk.manga.impl.track.tracker.TrackerManager
import suwayomi.tachidesk.manga.impl.track.tracker.model.toTrack
import suwayomi.tachidesk.manga.impl.track.tracker.model.toTrackRecordDataClass
import suwayomi.tachidesk.manga.model.dataclass.TrackRecordDataClass
import suwayomi.tachidesk.manga.model.table.CategoryMangaTable
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.manga.model.table.ChapterMetaTable
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ChapterUserTable
import suwayomi.tachidesk.manga.model.table.MangaMetaTable
import suwayomi.tachidesk.manga.model.table.MangaStatus
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.manga.model.table.TrackRecordTable
import suwayomi.tachidesk.manga.model.table.getWithUserData
import suwayomi.tachidesk.manga.model.table.toDataClass
import suwayomi.tachidesk.server.database.dbTransaction
import java.util.Date
import kotlin.math.max
import kotlin.system.measureTimeMillis
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import suwayomi.tachidesk.manga.impl.track.Track as Tracker

object BackupMangaHandler {
    private enum class RestoreMode {
        NEW,
        EXISTING,
    }

    private val logger = KotlinLogging.logger { }
    private val json = Json

    // fork: Mihon chapter-filter flags <-> WebUI manga meta
    private const val CHAPTER_FLAGS_UNREAD_MASK = 0x00000006L
    private const val CHAPTER_FLAGS_DOWNLOADED_MASK = 0x00000018L
    private const val CHAPTER_FLAGS_BOOKMARKED_MASK = 0x00000060L
    private const val CHAPTER_FLAGS_FILLERMARKED_MASK = 0x00000180L
    private const val META_EXCLUDED_SCANLATORS = "webUI_excludedScanlators"
    private val chapterFilterMetaKeys = listOf("webUI_unread", "webUI_bookmarked", "webUI_downloaded", "webUI_fillermarked")

    /** @return the flags derived from the meta and the mask of the flag bits the meta defined */
    private fun chapterFlagsFromMeta(meta: Map<String, String>): Pair<Long, Long> {
        var flags = 0L
        var mask = 0L
        // UNREAD
        when (meta["webUI_unread"]) {
            "true" -> flags = flags or 0x00000002L
            "false" -> flags = flags or 0x00000004L
        }
        if (meta["webUI_unread"] != null) mask = mask or CHAPTER_FLAGS_UNREAD_MASK
        // DOWNLOADED
        when (meta["webUI_downloaded"]) {
            "true" -> flags = flags or 0x00000008L
            "false" -> flags = flags or 0x00000010L
        }
        if (meta["webUI_downloaded"] != null) mask = mask or CHAPTER_FLAGS_DOWNLOADED_MASK
        // BOOKMARKED
        when (meta["webUI_bookmarked"]) {
            "true" -> flags = flags or 0x00000020L
            "false" -> flags = flags or 0x00000040L
        }
        if (meta["webUI_bookmarked"] != null) mask = mask or CHAPTER_FLAGS_BOOKMARKED_MASK
        // FILLERMARKED
        when (meta["webUI_fillermarked"]) {
            "true" -> flags = flags or 0x00000080L
            "false" -> flags = flags or 0x00000100L
        }
        if (meta["webUI_fillermarked"] != null) mask = mask or CHAPTER_FLAGS_FILLERMARKED_MASK
        return flags to mask
    }

    private fun metaFromChapterFlags(chapterFlags: Long): Map<String, String> {
        val meta = mutableMapOf<String, String>()
        // UNREAD
        when (chapterFlags and CHAPTER_FLAGS_UNREAD_MASK) {
            0x00000002L -> meta["webUI_unread"] = "true"
            0x00000004L -> meta["webUI_unread"] = "false"
        }
        // DOWNLOADED
        when (chapterFlags and CHAPTER_FLAGS_DOWNLOADED_MASK) {
            0x00000008L -> meta["webUI_downloaded"] = "true"
            0x00000010L -> meta["webUI_downloaded"] = "false"
        }
        // BOOKMARKED
        when (chapterFlags and CHAPTER_FLAGS_BOOKMARKED_MASK) {
            0x00000020L -> meta["webUI_bookmarked"] = "true"
            0x00000040L -> meta["webUI_bookmarked"] = "false"
        }
        // FILLERMARKED
        when (chapterFlags and CHAPTER_FLAGS_FILLERMARKED_MASK) {
            0x00000080L -> meta["webUI_fillermarked"] = "true"
            0x00000100L -> meta["webUI_fillermarked"] = "false"
        }
        return meta
    }

    private data class Prefetched(
        val mangaRows: List<ResultRow>,
        val mangaMetaByMangaId: Map<Int, Map<String, String>>,
        val chaptersByMangaId: Map<Int, List<ResultRow>>,
        val chapterMetaByChapterId: Map<Int, Map<String, String>>,
        val categoriesByMangaId: Map<Int, List<Int>>,
        val tracksByMangaId: Map<Int, List<TrackRecordDataClass>>,
    )

    /**
     * fork: bulk-prefetches the related tables in one short transaction and assembles the
     * BackupManga list afterwards, instead of querying every manga separately.
     *
     * The optional progress callback is invoked per-manga during assembly:
     *   progress(currentIndex, totalMangaCount, title)
     */
    fun backup(
        userId: Int,
        flags: BackupFlags,
        since: Long? = null,
        progress: ((Int, Int, String) -> Unit)? = null,
    ): List<BackupManga> {
        if (!flags.includeManga) {
            return emptyList()
        }

        logger.debug { "backup: starting bulk prefetch (flags=$flags)" }
        val t0 = System.currentTimeMillis()
        val prefetched =
            dbTransaction {
                val changed =
                    since?.let {
                        val chapterChanged =
                            ChapterTable
                                .getWithUserData(userId)
                                .select(ChapterTable.manga)
                                .where { ChapterUserTable.lastModifiedAt greaterEq it }
                        (MangaUserTable.lastModifiedAt greaterEq it) or (MangaTable.id inSubQuery chapterChanged)
                    } ?: Op.TRUE
                val mangaRows: List<ResultRow>
                val mangaFetchMs =
                    measureTimeMillis {
                        mangaRows =
                            MangaTable
                                .getWithUserData(userId)
                                .selectAll()
                                .where { (MangaUserTable.inLibrary eq true) and changed }
                                .toList()
                    }

                val mangaIds = mangaRows.map { it[MangaTable.id].value }

                val mangaMetaByMangaId: Map<Int, Map<String, String>>
                val mangaMetaFetchMs =
                    measureTimeMillis {
                        mangaMetaByMangaId =
                            if (flags.includeClientData && mangaIds.isNotEmpty()) {
                                MangaMetaTable
                                    .selectAll()
                                    .where { (MangaMetaTable.ref inIds mangaIds) and (MangaMetaTable.user eq userId) }
                                    .groupBy { it[MangaMetaTable.ref].value }
                                    .mapValues { (_, rows) -> rows.associate { it[MangaMetaTable.key] to it[MangaMetaTable.value] } }
                            } else {
                                emptyMap()
                            }
                    }

                val chapterRows: List<ResultRow>
                val chaptersFetchMs =
                    measureTimeMillis {
                        chapterRows =
                            if ((flags.includeChapters || flags.includeHistory) && mangaIds.isNotEmpty()) {
                                ChapterTable
                                    .getWithUserData(userId)
                                    .selectAll()
                                    .where { ChapterTable.manga inIds mangaIds }
                                    .orderBy(ChapterTable.sourceOrder to SortOrder.DESC)
                                    .toList()
                            } else {
                                emptyList()
                            }
                    }
                val chaptersByMangaId = chapterRows.groupBy { it[ChapterTable.manga].value }

                val chapterMetaByChapterId: Map<Int, Map<String, String>>
                val chapterMetaFetchMs =
                    measureTimeMillis {
                        chapterMetaByChapterId =
                            if (flags.includeChapters && flags.includeClientData && chapterRows.isNotEmpty()) {
                                ChapterMetaTable
                                    .selectAll()
                                    .where {
                                        (ChapterMetaTable.ref inIds chapterRows.map { it[ChapterTable.id].value }) and
                                            (ChapterMetaTable.user eq userId)
                                    }.groupBy { it[ChapterMetaTable.ref].value }
                                    .mapValues { (_, rows) -> rows.associate { it[ChapterMetaTable.key] to it[ChapterMetaTable.value] } }
                            } else {
                                emptyMap()
                            }
                    }

                val categoriesByMangaId: Map<Int, List<Int>>
                val categoriesFetchMs =
                    measureTimeMillis {
                        categoriesByMangaId =
                            if (flags.includeCategories && mangaIds.isNotEmpty()) {
                                CategoryMangaTable
                                    .innerJoin(CategoryTable)
                                    .selectAll()
                                    .where { (CategoryMangaTable.manga inIds mangaIds) and (CategoryTable.user eq userId) }
                                    .orderBy(CategoryTable.order to SortOrder.ASC)
                                    .groupBy({ it[CategoryMangaTable.manga].value }, { CategoryTable.toDataClass(it).order })
                            } else {
                                emptyMap()
                            }
                    }

                val tracksByMangaId: Map<Int, List<TrackRecordDataClass>>
                val tracksFetchMs =
                    measureTimeMillis {
                        tracksByMangaId =
                            if (flags.includeTracking && mangaIds.isNotEmpty()) {
                                TrackRecordTable
                                    .selectAll()
                                    .where { (TrackRecordTable.mangaId inIds mangaIds) and (TrackRecordTable.user eq userId) }
                                    .map { it.toTrackRecordDataClass() }
                                    .groupBy { it.mangaId }
                            } else {
                                emptyMap()
                            }
                    }

                logger.debug {
                    "backup: prefetch breakdown — " +
                        "manga=${mangaFetchMs}ms(${mangaRows.size}), " +
                        "mangaMeta=${mangaMetaFetchMs}ms, " +
                        "chapters=${chaptersFetchMs}ms(${chapterRows.size}), " +
                        "chapterMeta=${chapterMetaFetchMs}ms, " +
                        "categories=${categoriesFetchMs}ms, " +
                        "tracks=${tracksFetchMs}ms"
                }

                Prefetched(
                    mangaRows = mangaRows,
                    mangaMetaByMangaId = mangaMetaByMangaId,
                    chaptersByMangaId = chaptersByMangaId,
                    chapterMetaByChapterId = chapterMetaByChapterId,
                    categoriesByMangaId = categoriesByMangaId,
                    tracksByMangaId = tracksByMangaId,
                )
            }
        logger.debug { "backup: prefetch done in ${System.currentTimeMillis() - t0}ms (${prefetched.mangaRows.size} manga)" }

        val total = prefetched.mangaRows.size
        return prefetched.mangaRows.mapIndexed { index, mangaRow ->
            val mangaId = mangaRow[MangaTable.id].value

            var meta = prefetched.mangaMetaByMangaId[mangaId].orEmpty()
            var excludedScanlators: List<String> = emptyList()
            meta[META_EXCLUDED_SCANLATORS]?.let { jsonStr ->
                excludedScanlators =
                    try {
                        json.decodeFromString(ListSerializer(String.serializer()), jsonStr)
                    } catch (e: Exception) {
                        logger.warn(e) { "backup: invalid $META_EXCLUDED_SCANLATORS of manga $mangaId" }
                        emptyList()
                    }
                meta = meta - META_EXCLUDED_SCANLATORS
            }
            // the WebUI filter settings override the matching bits of the flags stored by Mihon/sync
            val (metaChapterFlags, metaChapterFlagsMask) = chapterFlagsFromMeta(meta)
            val chapterFlags = (mangaRow[MangaUserTable.chapterFlags].toLong() and metaChapterFlagsMask.inv()) or metaChapterFlags
            meta = meta - chapterFilterMetaKeys.toSet()

            val backupManga =
                BackupManga(
                    source = mangaRow[MangaTable.sourceReference],
                    url = mangaRow[MangaTable.url],
                    title = mangaRow[MangaTable.title],
                    artist = mangaRow[MangaTable.artist],
                    author = mangaRow[MangaTable.author],
                    description = mangaRow[MangaTable.description],
                    genre = mangaRow[MangaTable.genre]?.split(", ") ?: emptyList(),
                    status = MangaStatus.valueOf(mangaRow[MangaTable.status]).value,
                    thumbnailUrl = mangaRow[MangaTable.thumbnail_url],
                    dateAdded = mangaRow[MangaUserTable.inLibraryAt].seconds.inWholeMilliseconds,
                    viewer = mangaRow[MangaUserTable.viewer],
                    viewer_flags = mangaRow[MangaUserTable.viewerFlags],
                    chapterFlags = chapterFlags.toInt(),
                    updateStrategy = UpdateStrategy.valueOf(mangaRow[MangaTable.updateStrategy]),
                    lastModifiedAt = mangaRow[MangaUserTable.lastModifiedAt],
                    version = mangaRow[MangaUserTable.version],
                    initialized = mangaRow[MangaTable.initialized],
                    memo = Json.encodeToString(mangaRow[MangaTable.memo]).encodeToByteArray(),
                    excludedScanlators = excludedScanlators,
                ).apply {
                    // fork (Komikku port): per-user scanlator rules
                    val scanlatorSettings = ScanlatorSettings.fromRow(mangaRow)
                    scanlatorPriority = scanlatorSettings.scanlatorPriority
                    blacklistedChapters = scanlatorSettings.blacklistedChapters
                    scanlatorRangeRules = scanlatorSettings.scanlatorRangeRules
                    scanlatorSettingsPresent = scanlatorSettings.present
                    smartScanlatorMergeOverride =
                        when (scanlatorSettings.smartMergeOverride) {
                            true -> "enabled"
                            false -> "disabled"
                            null -> "global"
                        }
                }

            if (flags.includeClientData) {
                backupManga.meta = meta
            }

            if (flags.includeChapters || flags.includeHistory) {
                val chapters = prefetched.chaptersByMangaId[mangaId].orEmpty()

                if (flags.includeChapters) {
                    backupManga.chapters =
                        chapters.map {
                            BackupChapter(
                                url = it[ChapterTable.url],
                                name = it[ChapterTable.name],
                                scanlator = it[ChapterTable.scanlator],
                                read = it.getOrNull(ChapterUserTable.isRead) ?: false,
                                bookmark = it.getOrNull(ChapterUserTable.isBookmarked) ?: false,
                                fillermark = it.getOrNull(ChapterUserTable.isFillermarked) ?: false,
                                bookmarkColor = it.getOrNull(ChapterUserTable.bookmarkColor) ?: BookmarkColor.DEFAULT,
                                lastPageRead = it.getOrNull(ChapterUserTable.lastPageRead) ?: 0,
                                dateFetch = it[ChapterTable.fetchedAt].seconds.inWholeMilliseconds,
                                dateUpload = it[ChapterTable.date_upload],
                                chapterNumber = it[ChapterTable.chapter_number],
                                sourceOrder = chapters.size - it[ChapterTable.sourceOrder],
                                lastModifiedAt = it.getOrNull(ChapterUserTable.lastModifiedAt) ?: 0,
                                version = it.getOrNull(ChapterUserTable.version) ?: 0,
                                memo = Json.encodeToString(it[ChapterTable.memo]).encodeToByteArray(),
                            ).apply {
                                if (flags.includeClientData) {
                                    this.meta = prefetched.chapterMetaByChapterId[it[ChapterTable.id].value].orEmpty()
                                }
                            }
                        }
                }
                if (flags.includeHistory) {
                    backupManga.history =
                        chapters.mapNotNull {
                            val lastReadAt = it.getOrNull(ChapterUserTable.lastReadAt) ?: 0
                            if (lastReadAt > 0) {
                                BackupHistory(
                                    url = it[ChapterTable.url],
                                    lastRead = lastReadAt.seconds.inWholeMilliseconds,
                                )
                            } else {
                                null
                            }
                        }
                }
            }

            if (flags.includeCategories) {
                backupManga.categories = prefetched.categoriesByMangaId[mangaId].orEmpty()
            }

            if (flags.includeTracking) {
                val tracks =
                    prefetched.tracksByMangaId[mangaId].orEmpty().map { record ->
                        BackupTracking(
                            syncId = record.trackerId,
                            // forced not null so its compatible with 1.x backup system
                            libraryId = record.libraryId ?: 0,
                            mediaId = record.remoteId,
                            title = record.title,
                            lastChapterRead = record.lastChapterRead.toFloat(),
                            totalChapters = record.totalChapters,
                            score = record.score.toFloat(),
                            status = record.status,
                            startedReadingDate = record.startDate,
                            finishedReadingDate = record.finishDate,
                            trackingUrl = record.remoteUrl,
                            private = record.private,
                        )
                    }
                if (tracks.isNotEmpty()) {
                    backupManga.tracking = tracks
                }
            }

            progress?.invoke(index + 1, total, backupManga.title)

            backupManga
        }
    }

    fun restore(
        userId: Int,
        backupManga: BackupManga,
        categoryMapping: Map<Int, Int>,
        sourceMapping: Map<Long, String>,
        errors: MutableList<Pair<Date, String>>,
        flags: BackupFlags,
        syncMode: SyncRestoreMode = SyncRestoreMode.NONE,
    ) {
        val chapters = backupManga.chapters
        val categories = backupManga.categories
        val history = backupManga.history
        val tracking = backupManga.tracking

        val dbCategoryIds = categories.mapNotNull { categoryMapping[it] }

        try {
            restoreMangaData(userId, backupManga, chapters, dbCategoryIds, history, tracking, flags, syncMode)
        } catch (e: Exception) {
            val sourceName = sourceMapping[backupManga.source] ?: backupManga.source.toString()
            errors.add(Date() to "${backupManga.title} [$sourceName]: ${e.message}")
        }
    }

    private fun restoreMangaData(
        userId: Int,
        manga: BackupManga,
        chapters: List<BackupChapter>,
        categoryIds: List<Int>,
        history: List<BackupHistory>,
        tracks: List<BackupTracking>,
        flags: BackupFlags,
        syncMode: SyncRestoreMode,
    ) {
        val dbManga =
            transaction {
                MangaTable
                    .getWithUserData(userId)
                    .selectAll()
                    .where { (MangaTable.url eq manga.url) and (MangaTable.sourceReference eq manga.source) }
                    .firstOrNull()
            }
        val restoreMode = if (dbManga != null) RestoreMode.EXISTING else RestoreMode.NEW
        // a newer local copy wins the next upload; categories and tracking are part of the manga's version
        val keepLocalManga =
            syncMode == SyncRestoreMode.ADOPT && dbManga != null && manga.version < dbManga[MangaUserTable.version]

        val mangaId =
            transaction {
                val mangaId =
                    if (dbManga == null) {
                        // insert manga to database
                        MangaTable
                            .insertAndGetId {
                                it[url] = manga.url
                                it[title] = manga.title

                                it[artist] = manga.artist
                                it[author] = manga.author
                                it[description] = manga.description
                                it[genre] = manga.genre.joinToString()
                                it[status] = manga.status
                                it[thumbnail_url] = manga.thumbnailUrl
                                it[updateStrategy] = manga.updateStrategy.name

                                it[sourceReference] = manga.source

                                it[initialized] = manga.description != null

                                it[memo] = Json.decodeFromString<JsonObject>(manga.memo.decodeToString())
                            }.value
                    } else if (keepLocalManga) {
                        dbManga[MangaTable.id].value
                    } else {
                        val dbMangaId = dbManga[MangaTable.id].value

                        // Merge manga data
                        MangaTable.update({ MangaTable.id eq dbMangaId }) {
                            it[artist] = manga.artist ?: dbManga[artist]
                            it[author] = manga.author ?: dbManga[author]
                            it[description] = manga.description ?: dbManga[description]
                            it[genre] = manga.genre.ifEmpty { null }?.joinToString() ?: dbManga[genre]
                            it[status] = manga.status
                            it[thumbnail_url] = manga.thumbnailUrl ?: dbManga[thumbnail_url]
                            it[updateStrategy] = manga.updateStrategy.name

                            it[initialized] = dbManga[initialized] || manga.description != null

                            it[memo] = Json.decodeFromString<JsonObject>(manga.memo.decodeToString())
                        }

                        dbMangaId
                    }

                if (!keepLocalManga) {
                    // a newer local copy wins: keep its MANGAUSER row untouched
                    MangaUserTable.upsert(MangaUserTable.user, MangaUserTable.manga) {
                        it[MangaUserTable.user] = userId
                        it[MangaUserTable.manga] = mangaId
                        it[inLibrary] =
                            if (syncMode == SyncRestoreMode.ADOPT) {
                                manga.favorite
                            } else {
                                dbManga?.get(MangaUserTable.inLibrary) == true || manga.favorite
                            }
                        it[inLibraryAt] = manga.dateAdded.milliseconds.inWholeSeconds

                        // outside ADOPT a zeroed backup must not wipe stored flags
                        if (syncMode == SyncRestoreMode.ADOPT || manga.viewer != 0) it[viewer] = manga.viewer
                        if (syncMode == SyncRestoreMode.ADOPT || manga.viewer_flags != null) {
                            it[viewerFlags] = manga.viewer_flags
                        }
                        if (syncMode == SyncRestoreMode.ADOPT || manga.chapterFlags != 0) {
                            it[chapterFlags] = manga.chapterFlags
                        }

                        if (syncMode == SyncRestoreMode.CONVERGE) {
                            it[lastModifiedAt] = Clock.System.now().epochSeconds
                            it[version] = max(manga.version, dbManga?.get(MangaUserTable.version) ?: 0) + 1
                        } else {
                            it[lastModifiedAt] = manga.lastModifiedAt
                            it[version] = manga.version
                        }
                        it[isSyncing] = syncMode.isSync
                    }
                }

                // delete thumbnail in case cached data still exists
                clearThumbnail(mangaId)

                if (!keepLocalManga) restoreScanlatorSettings(userId, mangaId, manga)

                // fork: Mihon excluded scanlators and chapter filter flags are stored as WebUI meta
                val mutableMeta = manga.meta.toMutableMap()
                if (manga.excludedScanlators.isNotEmpty()) {
                    mutableMeta[META_EXCLUDED_SCANLATORS] =
                        json.encodeToString(ListSerializer(String.serializer()), manga.excludedScanlators)
                }
                mutableMeta.putAll(metaFromChapterFlags(manga.chapterFlags.toLong()))

                if (flags.includeClientData && mutableMeta.isNotEmpty()) {
                    modifyMangasMetas(userId, mapOf(mangaId to mutableMeta))
                }

                // merge chapter data
                if (flags.includeChapters || flags.includeHistory) {
                    restoreMangaChapterData(userId, mangaId, restoreMode, chapters, history, flags, syncMode)
                }

                // update categories
                if (flags.includeCategories && !keepLocalManga) {
                    restoreMangaCategoryData(userId, mangaId, categoryIds, syncMode)
                }

                mangaId
            }

        if (flags.includeTracking && !keepLocalManga) {
            restoreMangaTrackerData(userId, mangaId, tracks)
        }

        // fork: the restored rules and chapters decide which chapters are hidden
        ChapterVisibilityService.recompute(userId, listOf(mangaId))

        // TODO: insert/merge history
    }

    /**
     * fork (Komikku port): restores the scanlator rules when the backup states them. Fillermarks (proto 15) stay
     * fillermarks: unlike Komikku, this fork keeps them as their own feature, so they are not turned into blacklist
     * entries.
     */
    private fun restoreScanlatorSettings(
        userId: Int,
        mangaId: Int,
        manga: BackupManga,
    ) {
        val listsPresent =
            manga.scanlatorSettingsPresent || manga.scanlatorPriority.isNotEmpty() || manga.blacklistedChapters.isNotEmpty() ||
                manga.scanlatorRangeRules.isNotEmpty()
        val override =
            when (manga.smartScanlatorMergeOverride) {
                "enabled" -> SmartMergeOverrideChange(true)
                "disabled" -> SmartMergeOverrideChange(false)
                "global" -> SmartMergeOverrideChange(null)
                else -> null
            }
        ScanlatorSettings.update(
            userId,
            mangaId,
            blacklistedChapters = manga.blacklistedChapters.takeIf { listsPresent },
            scanlatorPriority = manga.scanlatorPriority.takeIf { listsPresent },
            scanlatorRangeRules = manga.scanlatorRangeRules.takeIf { listsPresent },
            smartMergeOverride = override,
        )
    }

    private fun restoredBookmarkColor(
        backupChapter: BackupChapter,
        bookmarked: Boolean,
        localColor: Int?,
    ): Int {
        if (!bookmarked) return BookmarkColor.DEFAULT
        val backupColor = backupChapter.bookmarkColor?.takeIf { backupChapter.bookmark && BookmarkColor.isValid(it) }
        // a backup without a color (or with the default color) does not erase a local color
        return backupColor?.takeIf { it != BookmarkColor.DEFAULT } ?: localColor ?: backupColor ?: BookmarkColor.DEFAULT
    }

    private fun getMangaChapterToRestoreInfo(
        mangaId: Int,
        restoreMode: RestoreMode,
        chapters: List<BackupChapter>,
    ): Pair<List<BackupChapter>, List<Pair<BackupChapter, ResultRow>>> {
        val uniqueChapters = chapters.distinctBy { it.url }

        if (restoreMode == RestoreMode.NEW) {
            return Pair(uniqueChapters, emptyList())
        }

        val dbChaptersByUrl = ChapterTable.selectAll().where { ChapterTable.manga eq mangaId }.associateBy { it[ChapterTable.url] }

        val (chaptersToUpdate, chaptersToInsert) = uniqueChapters.partition { dbChaptersByUrl.contains(it.url) }
        val chaptersToUpdateToDbChapter = chaptersToUpdate.map { it to dbChaptersByUrl[it.url]!! }

        return chaptersToInsert to chaptersToUpdateToDbChapter
    }

    private fun restoreMangaChapterData(
        userId: Int,
        mangaId: Int,
        restoreMode: RestoreMode,
        chapters: List<BackupChapter>,
        history: List<BackupHistory>,
        flags: BackupFlags,
        syncMode: SyncRestoreMode,
    ) = dbTransaction {
        val (chaptersToInsert, allChaptersToUpdate) = getMangaChapterToRestoreInfo(mangaId, restoreMode, chapters)
        val historyByChapter = history.groupBy({ it.url }, { it.lastRead })

        val existingUserData =
            ChapterUserTable
                .selectAll()
                .where {
                    (ChapterUserTable.user eq userId) and
                        (ChapterUserTable.chapter inList allChaptersToUpdate.map { it.second[ChapterTable.id].value })
                }.associate { it[ChapterUserTable.chapter].value to it }

        val chaptersToUpdateToDbChapter =
            if (syncMode == SyncRestoreMode.ADOPT) {
                allChaptersToUpdate.filter { (backupChapter, dbChapter) ->
                    backupChapter.version >=
                        (existingUserData[dbChapter[ChapterTable.id].value]?.get(ChapterUserTable.version) ?: 0)
                }
            } else {
                allChaptersToUpdate
            }

        val insertedChapterIds =
            if (flags.includeChapters) {
                val insertedIds =
                    ChapterTable
                        .batchInsert(chaptersToInsert) { chapter ->
                            this[ChapterTable.url] = chapter.url
                            this[ChapterTable.name] = chapter.name
                            if (chapter.dateUpload == 0L) {
                                this[ChapterTable.date_upload] = chapter.dateFetch
                            } else {
                                this[ChapterTable.date_upload] = chapter.dateUpload
                            }
                            this[ChapterTable.chapter_number] = chapter.chapterNumber
                            this[ChapterTable.scanlator] = chapter.scanlator

                            this[ChapterTable.sourceOrder] = chaptersToInsert.size - chapter.sourceOrder
                            this[ChapterTable.manga] = mangaId

                            this[ChapterTable.fetchedAt] = chapter.dateFetch.milliseconds.inWholeSeconds

                            this[ChapterTable.memo] = Json.decodeFromString<JsonObject>(chapter.memo.decodeToString())
                        }.map { it[ChapterTable.id].value }

                ChapterUserTable.batchUpsert(
                    insertedIds.zip(chaptersToInsert),
                    ChapterUserTable.user,
                    ChapterUserTable.chapter,
                ) { (dbChapterId, chapter) ->
                    this[ChapterUserTable.chapter] = dbChapterId
                    this[ChapterUserTable.user] = userId
                    this[ChapterUserTable.isRead] = chapter.read
                    this[ChapterUserTable.lastPageRead] = chapter.lastPageRead.coerceAtLeast(0)
                    this[ChapterUserTable.isBookmarked] = chapter.bookmark
                    this[ChapterUserTable.bookmarkColor] = restoredBookmarkColor(chapter, chapter.bookmark, null)
                    this[ChapterUserTable.isFillermarked] = chapter.fillermark

                    this[ChapterUserTable.version] = chapter.version
                    this[ChapterUserTable.lastModifiedAt] = chapter.lastModifiedAt
                    this[ChapterUserTable.isSyncing] = syncMode.isSync

                    if (flags.includeHistory) {
                        this[ChapterUserTable.lastReadAt] =
                            historyByChapter[chapter.url]?.maxOrNull()?.milliseconds?.inWholeSeconds ?: 0
                    }
                }

                insertedIds
            } else {
                emptyList()
            }

        if (chaptersToUpdateToDbChapter.isNotEmpty()) {
            ChapterUserTable.batchUpsert(
                chaptersToUpdateToDbChapter,
                ChapterUserTable.user,
                ChapterUserTable.chapter,
            ) { (backupChapter, dbChapter) ->
                val dbChapterId = dbChapter[ChapterTable.id].value
                val userData = existingUserData[dbChapterId]

                this[ChapterUserTable.chapter] = dbChapterId
                this[ChapterUserTable.user] = userId
                if (flags.includeChapters) {
                    val localColor = userData?.get(ChapterUserTable.bookmarkColor)
                    if (syncMode == SyncRestoreMode.ADOPT) {
                        this[ChapterUserTable.isRead] = backupChapter.read
                        this[ChapterUserTable.lastPageRead] = backupChapter.lastPageRead.coerceAtLeast(0)
                        this[ChapterUserTable.isBookmarked] = backupChapter.bookmark
                        this[ChapterUserTable.bookmarkColor] =
                            if (backupChapter.bookmark) {
                                backupChapter.bookmarkColor?.takeIf(BookmarkColor::isValid) ?: localColor ?: BookmarkColor.DEFAULT
                            } else {
                                BookmarkColor.DEFAULT
                            }
                        this[ChapterUserTable.isFillermarked] = backupChapter.fillermark
                    } else {
                        val bookmarked = backupChapter.bookmark || (userData?.get(ChapterUserTable.isBookmarked) ?: false)
                        this[ChapterUserTable.bookmarkColor] = restoredBookmarkColor(backupChapter, bookmarked, localColor)
                        this[ChapterUserTable.isRead] =
                            backupChapter.read || (userData?.get(ChapterUserTable.isRead) ?: false)
                        this[ChapterUserTable.lastPageRead] =
                            max(backupChapter.lastPageRead, userData?.get(ChapterUserTable.lastPageRead) ?: 0)
                                .coerceAtLeast(0)
                        this[ChapterUserTable.isBookmarked] =
                            backupChapter.bookmark || (userData?.get(ChapterUserTable.isBookmarked) ?: false)
                        this[ChapterUserTable.isFillermarked] =
                            backupChapter.fillermark || (userData?.get(ChapterUserTable.isFillermarked) ?: false)
                    }
                }

                if (flags.includeHistory) {
                    this[ChapterUserTable.lastReadAt] =
                        (historyByChapter[backupChapter.url]?.maxOrNull()?.milliseconds?.inWholeSeconds ?: 0)
                            .coerceAtLeast(userData?.get(ChapterUserTable.lastReadAt) ?: 0)
                }

                when (syncMode) {
                    SyncRestoreMode.ADOPT -> {
                        this[ChapterUserTable.lastModifiedAt] = backupChapter.lastModifiedAt
                        this[ChapterUserTable.version] = backupChapter.version
                        this[ChapterUserTable.isSyncing] = true
                    }

                    SyncRestoreMode.CONVERGE -> {
                        this[ChapterUserTable.lastModifiedAt] = Clock.System.now().epochSeconds
                        this[ChapterUserTable.version] =
                            max(backupChapter.version, userData?.get(ChapterUserTable.version) ?: 0) + 1
                        this[ChapterUserTable.isSyncing] = true
                    }

                    SyncRestoreMode.NONE -> {
                        this[ChapterUserTable.lastModifiedAt] = backupChapter.lastModifiedAt
                        this[ChapterUserTable.version] =
                            max(backupChapter.version, userData?.get(ChapterUserTable.version) ?: 0)
                        this[ChapterUserTable.isSyncing] = true
                    }
                }
            }
        }

        if (flags.includeClientData) {
            val chaptersToInsertByChapterId = insertedChapterIds.zip(chaptersToInsert)
            val chapterToUpdateByChapterId =
                chaptersToUpdateToDbChapter.map { (backupChapter, dbChapter) ->
                    dbChapter[ChapterTable.id].value to
                        backupChapter
                }
            val metaEntryByChapterId =
                (chaptersToInsertByChapterId + chapterToUpdateByChapterId)
                    .associate { (chapterId, backupChapter) ->
                        chapterId to backupChapter.meta
                    }

            modifyChaptersMetas(userId, metaEntryByChapterId)
        }
    }

    private fun restoreMangaCategoryData(
        userId: Int,
        mangaId: Int,
        categoryIds: List<Int>,
        syncMode: SyncRestoreMode,
    ) {
        // CONVERGE keeps the union so a local-only link survives and wins the next upload
        if (syncMode != SyncRestoreMode.CONVERGE) {
            CategoryManga.removeMangaFromAllCategories(userId, mangaId)
        }
        CategoryManga.addMangaToCategories(userId, mangaId, categoryIds)
    }

    private fun restoreMangaTrackerData(
        userId: Int,
        mangaId: Int,
        tracks: List<BackupTracking>,
    ) {
        val dbTrackRecordsByTrackerId =
            Tracker
                .getTrackRecordsByMangaId(userId, mangaId)
                .mapNotNull { it.record?.toTrack() }
                .associateBy { it.tracker_id }

        val (existingTracks, newTracks) =
            tracks
                .mapNotNull { backupTrack ->
                    val track = backupTrack.toTrack(mangaId)

                    val isUnsupportedTracker = TrackerManager.getTracker(track.tracker_id) == null
                    if (isUnsupportedTracker) {
                        return@mapNotNull null
                    }

                    val dbTrack =
                        dbTrackRecordsByTrackerId[backupTrack.syncId]
                            ?: // new track
                            return@mapNotNull track

                    if (track.toTrackRecordDataClass().forComparison() == dbTrack.toTrackRecordDataClass().forComparison()) {
                        return@mapNotNull null
                    }

                    dbTrack.also {
                        it.remote_id = track.remote_id
                        it.library_id = track.library_id
                        it.last_chapter_read = max(dbTrack.last_chapter_read, track.last_chapter_read)
                    }
                }.partition { (it.id ?: -1) > 0 }

        Tracker.updateTrackRecords(userId, existingTracks)
        Tracker.insertTrackRecords(userId, newTracks)
    }

    private fun TrackRecordDataClass.forComparison() = this.copy(id = 0, mangaId = 0)
}

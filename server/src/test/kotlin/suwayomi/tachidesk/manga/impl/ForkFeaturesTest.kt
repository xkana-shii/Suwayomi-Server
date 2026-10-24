package suwayomi.tachidesk.manga.impl

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInstance
import suwayomi.tachidesk.manga.impl.backup.BackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupMangaHandler
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupChapter
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupManga
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorSettings
import suwayomi.tachidesk.manga.impl.util.getChapterDownloadPath
import suwayomi.tachidesk.manga.impl.util.lang.EMPTY
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterDownloadFolder
import suwayomi.tachidesk.manga.impl.util.source.StubSource
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ChapterUserTable
import suwayomi.tachidesk.manga.model.table.MangaMetaTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.server.database.ForkSchema
import suwayomi.tachidesk.test.ApplicationTest
import suwayomi.tachidesk.test.clearTables
import suwayomi.tachidesk.test.createLibraryManga
import java.io.File
import java.security.MessageDigest
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Regression tests for functionality that only exists in this fork.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ForkFeaturesTest : ApplicationTest() {
    private val source = StubSource(1L)

    private val allBackupFlags =
        BackupFlags(
            includeManga = true,
            includeCategories = true,
            includeChapters = true,
            includeTracking = true,
            includeHistory = true,
            includeClientData = true,
            includeServerSettings = false,
            includeUserSettings = false,
        )

    @AfterEach
    fun tearDown() {
        clearTables(MangaUserTable, ChapterUserTable, ChapterTable, MangaMetaTable, MangaTable)
    }

    private fun insertChapters(
        mangaId: Int,
        urls: List<String>,
    ): Map<String, Int> =
        transaction {
            ChapterTable
                .batchInsert(urls) { url ->
                    this[ChapterTable.url] = url
                    this[ChapterTable.name] = "Chapter $url"
                    this[ChapterTable.chapter_number] = url.toFloat()
                    this[ChapterTable.sourceOrder] = url.toInt()
                    this[ChapterTable.manga] = mangaId
                    this[ChapterTable.memo] = JsonObject.EMPTY
                }.associate { it[ChapterTable.url] to it[ChapterTable.id].value }
        }

    private fun chapterUserRow(chapterId: Int) =
        transaction {
            ChapterUserTable
                .selectAll()
                .where { (ChapterUserTable.chapter eq chapterId) and (ChapterUserTable.user eq 1) }
                .singleOrNull()
        }

    private fun columnExists(
        table: String,
        column: String,
    ): Boolean =
        transaction {
            (
                TransactionManager.current().exec(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$table' AND COLUMN_NAME = '$column'",
                ) { rs ->
                    rs.next()
                    rs.getInt(1)
                } ?: 0
            ) > 0
        }

    @Test
    fun `fork schema turns legacy chapter fillermarks into blacklist entries of the user that owns the old data`() {
        val mangaId = createLibraryManga("FORK_SCHEMA_FILLERMARK")
        val chapterIds = insertChapters(mangaId, listOf("1", "2", "3"))

        // simulate a database of the old fork: the fillermark used to be a column of CHAPTER
        transaction {
            exec("ALTER TABLE CHAPTER ADD COLUMN FILLERMARK BOOLEAN DEFAULT FALSE NOT NULL")
            exec("UPDATE CHAPTER SET FILLERMARK = TRUE WHERE ID IN (${chapterIds.getValue("1")}, ${chapterIds.getValue("2")})")
            // chapter 1 already got a CHAPTERUSER row during the user accounts migration, chapter 2 did not
            ChapterUserTable.batchInsert(listOf(chapterIds.getValue("1"))) {
                this[ChapterUserTable.chapter] = it
                this[ChapterUserTable.user] = 1
                this[ChapterUserTable.isRead] = true
                this[ChapterUserTable.lastPageRead] = 7
            }
        }

        ForkSchema.ensure()
        // idempotent
        ForkSchema.ensure()

        val chapter1 = assertNotNull(chapterUserRow(chapterIds.getValue("1")))
        assertTrue(chapter1[ChapterUserTable.isBlacklisted])
        assertTrue(chapter1[ChapterUserTable.isRead], "existing per-user state must be kept")
        assertEquals(7, chapter1[ChapterUserTable.lastPageRead])

        val chapter2 = assertNotNull(chapterUserRow(chapterIds.getValue("2")), "fillermark-only chapter needs a user row")
        assertTrue(chapter2[ChapterUserTable.isBlacklisted])
        assertFalse(chapter2[ChapterUserTable.isRead])

        val chapter3 = chapterUserRow(chapterIds.getValue("3"))
        assertTrue(chapter3 == null || !chapter3[ChapterUserTable.isBlacklisted])

        assertEquals(2, ScanlatorSettings.get(1, mangaId).blacklistedChapters.size)
        assertFalse(columnExists("CHAPTER", "FILLERMARK"), "legacy column is dropped after the move")
        assertFalse(columnExists("CHAPTERUSER", "FILLERMARK"), "the fillermark column is removed once migrated")
    }

    @Test
    fun `legacy backup fillermarks become blacklist entries and the 0x100 sort bit is kept as Mihon's`() {
        val errors = mutableListOf<Pair<Date, String>>()
        val manga =
            BackupManga(
                source = 1,
                url = "/fork-backup",
                title = "Fork Backup",
                favorite = true,
                excludedScanlators = listOf("Group A", "Group B"),
                // unread only + bookmarked only + Mihon "sort by chapter number" (0x100)
                chapterFlags = 0x2 or 0x20 or 0x100,
                chapters =
                    listOf(
                        BackupChapter(url = "/c1", name = "c1", chapterNumber = 1f, sourceOrder = 1, fillermark = true),
                        BackupChapter(url = "/c2", name = "c2", chapterNumber = 2f, sourceOrder = 0, bookmark = true),
                    ),
            )

        BackupMangaHandler.restore(1, manga, emptyMap(), emptyMap(), errors, allBackupFlags)
        assertTrue(errors.isEmpty(), errors.joinToString())

        val mangaId =
            transaction {
                MangaTable
                    .selectAll()
                    .where { MangaTable.url eq "/fork-backup" }
                    .single()[MangaTable.id]
                    .value
            }
        val meta = Manga.getMangaMetaMap(1, mangaId)
        assertEquals("""["Group A","Group B"]""", meta["webUI_excludedScanlators"])
        assertEquals("true", meta["webUI_unread"])
        assertEquals("true", meta["webUI_bookmarked"])
        assertFalse("webUI_fillermarked" in meta, "0x100 is Mihon's sort bit, not a fillermark filter")

        val c1 =
            transaction {
                ChapterTable
                    .selectAll()
                    .where { ChapterTable.url eq "/c1" }
                    .single()[ChapterTable.id]
                    .value
            }
        assertTrue(chapterUserRow(c1)!![ChapterUserTable.isBlacklisted], "a legacy fillermark is restored as a blacklist entry")

        val exported = BackupMangaHandler.backup(1, allBackupFlags).single()
        assertEquals(listOf("Group A", "Group B"), exported.excludedScanlators)
        assertEquals(0x2 or 0x20 or 0x100, exported.chapterFlags)
        assertFalse(exported.meta.keys.any { it.startsWith("webUI_unread") || it == "webUI_excludedScanlators" })
        assertTrue(exported.chapters.none { it.fillermark }, "the removed feature is not exported anymore")
        assertEquals(1, exported.blacklistedChapters.size)
        assertTrue(exported.chapters.single { it.url == "/c2" }.bookmark)

        // restoring the legacy backup again neither duplicates the entry nor replaces other entries
        BackupMangaHandler.restore(1, manga, emptyMap(), emptyMap(), errors, allBackupFlags)
        assertTrue(errors.isEmpty(), errors.joinToString())
        assertEquals(1, ScanlatorSettings.get(1, mangaId).blacklistedChapters.size)
    }

    @Test
    fun `backup reports progress per manga`() {
        createLibraryManga("FORK_PROGRESS_1")
        createLibraryManga("FORK_PROGRESS_2")

        val progress = mutableListOf<Triple<Int, Int, String>>()
        BackupMangaHandler.backup(1, allBackupFlags) { current, total, title -> progress += Triple(current, total, title) }

        assertEquals(listOf(1, 2), progress.map { it.first })
        assertTrue(progress.all { it.second == 2 })
        assertEquals(setOf("FORK_PROGRESS_1", "FORK_PROGRESS_2"), progress.map { it.third }.toSet())
    }

    @Test
    fun `chapter downloads use hashed names and still find legacy downloads`() =
        runTest {
            val mangaId = createLibraryManga("FORK_DOWNLOAD_NAMES")
            val chapterId = insertChapters(mangaId, listOf("5")).getValue("5")

            val md5 =
                MessageDigest
                    .getInstance("MD5")
                    .digest("5".toByteArray())
                    .joinToString("") { "%02x".format(it) }
            val downloadPath = getChapterDownloadPath(mangaId, chapterId)
            assertTrue(downloadPath.endsWith("Chapter 5_${md5.take(6)}"), downloadPath)

            val legacyFolder = File(downloadPath.substringBeforeLast("/") + "/Chapter 5")
            try {
                legacyFolder.mkdirs()
                assertEquals(legacyFolder.absolutePath, resolveExistingChapterDownloadFolder(mangaId, chapterId))

                assertTrue(ChapterDownloadHelper.migrateDownloadedChapterFilename(chapterId))
                assertFalse(legacyFolder.exists())
                assertEquals(File(downloadPath).absolutePath, resolveExistingChapterDownloadFolder(mangaId, chapterId))
            } finally {
                legacyFolder.deleteRecursively()
                File(downloadPath).deleteRecursively()
            }
        }
}

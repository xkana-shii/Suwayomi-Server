package suwayomi.tachidesk.manga.impl.backup

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.neq
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insertAndGetId
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInstance
import suwayomi.tachidesk.manga.impl.Category
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupCategoryHandler
import suwayomi.tachidesk.manga.impl.backup.proto.handlers.BackupMangaHandler
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupChapter
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupManga
import suwayomi.tachidesk.manga.impl.chapter.BookmarkColor
import suwayomi.tachidesk.manga.impl.chapter.ChapterBlacklist
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorSettings
import suwayomi.tachidesk.manga.impl.track.UnifiedTrackStatus
import suwayomi.tachidesk.manga.model.table.CategoryMangaTable
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ChapterUserTable
import suwayomi.tachidesk.manga.model.table.MangaMetaTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.test.ApplicationTest
import suwayomi.tachidesk.test.clearTables
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KomikkuPortBackupTest : ApplicationTest() {
    private val flags =
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
        clearTables(CategoryMangaTable, MangaUserTable, ChapterUserTable, ChapterTable, MangaMetaTable, MangaTable)
        transaction { CategoryTable.deleteWhere { CategoryTable.isDefaultCategory neq true } }
    }

    private fun restore(manga: BackupManga) {
        val errors = mutableListOf<Pair<Date, String>>()
        BackupMangaHandler.restore(1, manga, emptyMap(), emptyMap(), errors, flags)
        assertTrue(errors.isEmpty(), errors.joinToString())
    }

    private fun chapterRows(url: String) =
        transaction {
            ChapterTable
                .innerJoin(ChapterUserTable)
                .selectAll()
                .where { (ChapterTable.url eq url) and (ChapterUserTable.user eq 1) }
                .single()
        }

    private val komikkuManga =
        BackupManga(
            source = 1,
            url = "/komikku-port",
            title = "Komikku Port",
            favorite = true,
            chapters =
                listOf(
                    BackupChapter(url = "/c1", name = "c1", chapterNumber = 1f, sourceOrder = 2, scanlator = "A"),
                    BackupChapter(url = "/c1b", name = "c1", chapterNumber = 1f, sourceOrder = 1, scanlator = "B"),
                    BackupChapter(
                        url = "/c2",
                        name = "c2",
                        chapterNumber = 2f,
                        sourceOrder = 0,
                        scanlator = "B",
                        bookmark = true,
                        bookmarkColor = BookmarkColor.GREEN,
                        fillermark = true,
                    ),
                ),
            scanlatorPriority = listOf("B", "A"),
            blacklistedChapters = listOf(ChapterBlacklist.urlKey("/c1")),
            scanlatorRangeRules = listOf("1.0:1.0:A"),
            smartScanlatorMergeOverride = "enabled",
            scanlatorSettingsPresent = true,
        )

    @Test
    fun `scanlator rules and bookmark colors restore and export with Komikku's fields`() {
        restore(komikkuManga)
        val mangaId = chapterRows("/c1")[ChapterTable.manga].value

        val settings = ScanlatorSettings.get(1, mangaId)
        assertEquals(listOf("B", "A"), settings.scanlatorPriority)
        assertEquals(listOf("1.0:1.0:A"), settings.scanlatorRangeRules)
        assertEquals(true, settings.smartMergeOverride)
        assertTrue(chapterRows("/c1")[ChapterUserTable.isBlacklisted])
        // the range rule prefers A for chapter 1, but A's chapter is blacklisted: B's stays visible
        assertFalse(chapterRows("/c1b")[ChapterUserTable.isMergeHidden])

        val c2 = chapterRows("/c2")
        assertEquals(BookmarkColor.GREEN, c2[ChapterUserTable.bookmarkColor])
        assertTrue(c2[ChapterUserTable.isFillermarked], "fillermarks stay fillermarks")
        assertFalse(c2[ChapterUserTable.isBlacklisted], "fillermarks are not turned into blacklist entries")

        val exported = BackupMangaHandler.backup(1, flags).single()
        assertEquals(komikkuManga.scanlatorPriority, exported.scanlatorPriority)
        assertEquals(komikkuManga.blacklistedChapters, exported.blacklistedChapters)
        assertEquals(komikkuManga.scanlatorRangeRules, exported.scanlatorRangeRules)
        assertEquals("enabled", exported.smartScanlatorMergeOverride)
        assertTrue(exported.scanlatorSettingsPresent)
        assertEquals(BookmarkColor.GREEN, exported.chapters.single { it.url == "/c2" }.bookmarkColor)
    }

    @Test
    fun `a backup without the fields keeps the local rules and colors`() {
        restore(komikkuManga)
        val mangaId = chapterRows("/c1")[ChapterTable.manga].value

        restore(
            komikkuManga.copy(
                scanlatorPriority = emptyList(),
                blacklistedChapters = emptyList(),
                scanlatorRangeRules = emptyList(),
                smartScanlatorMergeOverride = null,
                scanlatorSettingsPresent = false,
                chapters = komikkuManga.chapters.map { it.copy(bookmarkColor = null) },
            ),
        )

        val settings = ScanlatorSettings.get(1, mangaId)
        assertEquals(listOf("B", "A"), settings.scanlatorPriority)
        assertEquals(1, settings.blacklistedChapters.size)
        assertEquals(true, settings.smartMergeOverride)
        assertEquals(BookmarkColor.GREEN, chapterRows("/c2")[ChapterUserTable.bookmarkColor])
    }

    @Test
    fun `category hierarchy and smart bindings survive a backup round trip, same names in other branches included`() {
        fun create(
            name: String,
            parentId: Int?,
            trackStatus: Int? = null,
        ) = transaction {
            CategoryTable
                .insertAndGetId {
                    it[CategoryTable.name] = name
                    it[CategoryTable.user] = 1
                    it[CategoryTable.order] = Int.MAX_VALUE
                    it[CategoryTable.parentId] = parentId
                    it[CategoryTable.trackStatus] = trackStatus
                }.value
        }
        val shounen = create("Shounen", null)
        create("Reading", shounen, UnifiedTrackStatus.READING.value)
        val josei = create("Josei", null)
        create("Reading", josei, UnifiedTrackStatus.READING.value)
        Category.normalizeCategories(1)

        val backup = BackupCategoryHandler.backup(1, flags)
        transaction { CategoryTable.deleteWhere { CategoryTable.isDefaultCategory neq true } }

        BackupCategoryHandler.restore(1, backup)
        // restoring again must match every category instead of creating duplicates
        BackupCategoryHandler.restore(1, backup)

        val categories = Category.getCategoryList(1).filterNot { it.isDefaultCategory }
        assertEquals(4, categories.size)
        val byId = categories.associateBy { it.id }
        val readings = categories.filter { it.name == "Reading" }
        assertEquals(setOf("Shounen", "Josei"), readings.map { byId.getValue(it.parentId!!).name }.toSet())
        assertTrue(readings.all { it.trackStatus == UnifiedTrackStatus.READING.value })
        assertTrue(categories.filter { it.name != "Reading" }.all { it.parentId == null })
    }
}

package suwayomi.tachidesk.manga.impl.chapter

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import eu.kanade.tachiyomi.source.model.SChapter
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.insertAndGetId
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInstance
import suwayomi.tachidesk.global.model.table.UserAccountTable
import suwayomi.tachidesk.global.model.table.UserSettingsTable
import suwayomi.tachidesk.manga.impl.Chapter
import suwayomi.tachidesk.manga.impl.Manga
import suwayomi.tachidesk.manga.impl.util.lang.EMPTY
import suwayomi.tachidesk.manga.impl.util.source.StubSource
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ChapterUserTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.server.settings.UserSettings
import suwayomi.tachidesk.server.settings.userConfig
import suwayomi.tachidesk.test.ApplicationTest
import suwayomi.tachidesk.test.clearTables
import suwayomi.tachidesk.test.createLibraryManga
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChapterVisibilityServiceTest : ApplicationTest() {
    private val source = StubSource(1L)
    private val createdUserIds = mutableListOf<Int>()

    @AfterEach
    fun tearDown() {
        UserSettings.resetAll(1)
        createdUserIds.forEach { UserSettings.resetAll(it) }
        clearTables(MangaUserTable, ChapterUserTable, ChapterTable, MangaTable)
        transaction {
            UserSettingsTable.deleteWhere { UserSettingsTable.user eq 1 }
            createdUserIds.forEach { id -> UserAccountTable.deleteWhere { UserAccountTable.id eq id } }
        }
        createdUserIds.clear()
    }

    private fun createUser(username: String): Int =
        transaction {
            UserAccountTable
                .insertAndGetId {
                    it[UserAccountTable.username] = username
                    it[password] = "password"
                }.value
                .also(createdUserIds::add)
        }

    /** chapters as (url, number, scanlator); source order follows the list (first = newest) */
    private fun insertChapters(
        mangaId: Int,
        chapters: List<Triple<String, Float, String?>>,
    ): Map<String, Int> =
        transaction {
            ChapterTable
                .batchInsert(chapters.withIndex().toList()) { (index, chapter) ->
                    this[ChapterTable.url] = chapter.first
                    this[ChapterTable.name] = "Chapter ${chapter.second}"
                    this[ChapterTable.chapter_number] = chapter.second
                    this[ChapterTable.scanlator] = chapter.third
                    this[ChapterTable.sourceOrder] = chapters.size - index
                    this[ChapterTable.manga] = mangaId
                    this[ChapterTable.memo] = JsonObject.EMPTY
                }.associate { it[ChapterTable.url] to it[ChapterTable.id].value }
        }

    private fun flags(
        chapterId: Int,
        userId: Int = 1,
    ): Pair<Boolean, Boolean> =
        transaction {
            ChapterUserTable
                .selectAll()
                .where { (ChapterUserTable.chapter eq chapterId) and (ChapterUserTable.user eq userId) }
                .singleOrNull()
                ?.let { it[ChapterUserTable.isBlacklisted] to it[ChapterUserTable.isMergeHidden] }
                ?: (false to false)
        }

    private fun unreadIds(
        mangaId: Int,
        userId: Int = 1,
    ) = Manga.getUnreadChapters(userId, mangaId).map { it.id }.toSet()

    @Test
    fun `blacklisting is per user, hides the chapter from unread counts and is undone by unblacklisting`() {
        val user2 = createUser("visibility-user-2")
        val mangaId = createLibraryManga("VISIBILITY_BLACKLIST")
        transaction {
            MangaUserTable.insert {
                it[manga] = mangaId
                it[user] = user2
                it[inLibrary] = true
            }
        }
        val ids = insertChapters(mangaId, listOf(Triple("/2", 2f, "A"), Triple("/1", 1f, "A")))

        ChapterVisibilityService.setBlacklisted(1, listOf(ids.getValue("/1")), true)

        assertEquals(true to false, flags(ids.getValue("/1")))
        assertEquals(setOf(ids.getValue("/2")), unreadIds(mangaId))
        assertEquals(false to false, flags(ids.getValue("/1"), user2), "other users are not affected")
        assertEquals(ids.values.toSet(), unreadIds(mangaId, user2))

        ChapterVisibilityService.setBlacklisted(1, listOf(ids.getValue("/1")), false)
        assertEquals(false to false, flags(ids.getValue("/1")))
        assertEquals(emptyList(), ScanlatorSettings.get(1, mangaId).blacklistedChapters)
    }

    @Test
    fun `global smart merge, per manga override and priority are applied and survive a refresh`() =
        runTest {
            val mangaId = createLibraryManga("VISIBILITY_MERGE")
            val ids =
                insertChapters(
                    mangaId,
                    listOf(Triple("/b2", 2f, "B"), Triple("/a1", 1f, "A"), Triple("/b1", 1f, "B")),
                )
            ScanlatorSettings.update(1, mangaId, scanlatorPriority = listOf("B", "A"))
            ChapterVisibilityService.recompute(1, listOf(mangaId))
            assertEquals(false to false, flags(ids.getValue("/a1")), "merge is off by default")

            UserSettings.set(1, userConfig.smartScanlatorMerge, true)
            assertEquals(false to true, flags(ids.getValue("/a1")))
            assertEquals(false to false, flags(ids.getValue("/b1")))

            ScanlatorSettings.update(1, mangaId, smartMergeOverride = SmartMergeOverrideChange(false))
            ChapterVisibilityService.recompute(1, listOf(mangaId))
            assertEquals(false to false, flags(ids.getValue("/a1")), "the manga override wins over the global setting")
            ScanlatorSettings.update(1, mangaId, smartMergeOverride = SmartMergeOverrideChange(null))
            ChapterVisibilityService.recompute(1, listOf(mangaId))

            // refresh: a new scanlator "C" appears for chapter 3 and chapter 1 of A gets a new URL
            val mangaEntry = transaction { MangaTable.selectAll().where { MangaTable.id eq mangaId }.first() }
            val fetched =
                listOf(Triple("/c3", 3f, "C"), Triple("/b2", 2f, "B"), Triple("/a1-new", 1f, "A"), Triple("/b1", 1f, "B"))
                    .map { (url, number, scanlator) ->
                        SChapter.create().apply {
                            this.url = url
                            this.name = "Chapter $number"
                            this.chapter_number = number
                            this.scanlator = scanlator
                        }
                    }
            Chapter.updateChapterListDatabase(mangaEntry, fetched, source)

            val newIds =
                transaction {
                    ChapterTable.selectAll().where { ChapterTable.manga eq mangaId }.associate {
                        it[ChapterTable.url] to it[ChapterTable.id].value
                    }
                }
            assertEquals(false to true, flags(newIds.getValue("/a1-new")), "the refreshed duplicate is still merged")
            assertEquals(false to false, flags(newIds.getValue("/c3")))
            assertEquals(listOf("B", "A"), ScanlatorSettings.get(1, mangaId).scanlatorPriority, "the priority is kept")

            UserSettings.set(1, userConfig.smartScanlatorMerge, false)
            assertEquals(false to false, flags(newIds.getValue("/a1-new")))
        }

    @Test
    fun `a blacklisted chapter stays blacklisted after its url changes and new chapters are not hidden`() =
        runTest {
            val mangaId = createLibraryManga("VISIBILITY_URL_CHANGE")
            val ids = insertChapters(mangaId, listOf(Triple("/2", 2f, "A"), Triple("/1", 1f, "A")))
            ChapterVisibilityService.setBlacklisted(1, listOf(ids.getValue("/2")), true)

            val mangaEntry = transaction { MangaTable.selectAll().where { MangaTable.id eq mangaId }.first() }
            val fetched =
                listOf("/3" to 3f, "/2-new" to 2f, "/1" to 1f).map { (url, number) ->
                    SChapter.create().apply {
                        this.url = url
                        this.name = "Chapter $number"
                        this.chapter_number = number
                        this.scanlator = "A"
                    }
                }
            Chapter.updateChapterListDatabase(mangaEntry, fetched, source)
            val newIds =
                transaction {
                    ChapterTable.selectAll().where { ChapterTable.manga eq mangaId }.associate {
                        it[ChapterTable.url] to it[ChapterTable.id].value
                    }
                }
            assertEquals(true to false, flags(newIds.getValue("/2-new")))
            assertEquals(false to false, flags(newIds.getValue("/3")))
            assertTrue(newIds.getValue("/2-new") !in unreadIds(mangaId))
            assertEquals(1, ScanlatorSettings.get(1, mangaId).blacklistedChapters.size, "the entry is never duplicated or lost")
        }

    @Test
    fun `a re-uploaded chapter keeps its bookmark color, other users keep theirs`() =
        runTest {
            val user2 = createUser("color-user-2")
            val mangaId = createLibraryManga("VISIBILITY_COLOR")
            val ids = insertChapters(mangaId, listOf(Triple("/2", 2f, "A"), Triple("/1", 1f, "A")))
            transaction {
                listOf(1 to BookmarkColor.RED, user2 to BookmarkColor.BLUE).forEach { (userId, color) ->
                    ChapterUserTable.insert {
                        it[chapter] = ids.getValue("/2")
                        it[user] = userId
                        it[isBookmarked] = true
                        it[bookmarkColor] = color
                    }
                }
            }

            val mangaEntry = transaction { MangaTable.selectAll().where { MangaTable.id eq mangaId }.first() }
            val fetched =
                listOf("/2-new" to 2f, "/1" to 1f).map { (url, number) ->
                    SChapter.create().apply {
                        this.url = url
                        this.name = "Chapter $number"
                        this.chapter_number = number
                        this.scanlator = "A"
                    }
                }
            Chapter.updateChapterListDatabase(mangaEntry, fetched, source)
            val newId =
                transaction {
                    ChapterTable
                        .selectAll()
                        .where { ChapterTable.url eq "/2-new" }
                        .single()[ChapterTable.id]
                        .value
                }

            fun color(userId: Int) =
                transaction {
                    ChapterUserTable
                        .selectAll()
                        .where { (ChapterUserTable.chapter eq newId) and (ChapterUserTable.user eq userId) }
                        .single()[ChapterUserTable.bookmarkColor]
                }
            assertEquals(BookmarkColor.RED, color(1))
            assertEquals(BookmarkColor.BLUE, color(user2))
        }
}

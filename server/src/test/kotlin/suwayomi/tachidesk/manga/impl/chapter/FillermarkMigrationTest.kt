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
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInstance
import suwayomi.tachidesk.global.model.table.GlobalMetaTable
import suwayomi.tachidesk.global.model.table.UserAccountTable
import suwayomi.tachidesk.manga.impl.Chapter
import suwayomi.tachidesk.manga.impl.Manga
import suwayomi.tachidesk.manga.impl.util.lang.EMPTY
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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** The removed fillermark feature migrates into the chapter blacklist (see ForkSchema). */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FillermarkMigrationTest : ApplicationTest() {
    private val createdUserIds = mutableListOf<Int>()

    @AfterEach
    fun tearDown() {
        clearTables(MangaUserTable, ChapterUserTable, ChapterTable, MangaMetaTable, MangaTable)
        transaction {
            exec("ALTER TABLE CHAPTERUSER DROP COLUMN IF EXISTS FILLERMARK")
            GlobalMetaTable.deleteWhere { GlobalMetaTable.key eq "webUI_deleteChaptersWithFillermark" }
            createdUserIds.forEach { id -> UserAccountTable.deleteWhere { UserAccountTable.id eq id } }
        }
        createdUserIds.clear()
    }

    private fun createUser(name: String): Int =
        transaction {
            UserAccountTable
                .insertAndGetId {
                    it[username] = name
                    it[password] = "password"
                }.value
        }.also(createdUserIds::add)

    /** chapters as (url, number, scanlator); every chapter is named "Chapter <number>" */
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

    /** simulates a database of the previous fork version: fillermarks in CHAPTERUSER.FILLERMARK */
    private fun fillermark(
        userId: Int,
        vararg chapterIds: Int,
    ) {
        transaction {
            exec("ALTER TABLE CHAPTERUSER ADD COLUMN IF NOT EXISTS FILLERMARK BOOLEAN DEFAULT FALSE NOT NULL")
            chapterIds.forEach { chapterId ->
                val hasRow =
                    ChapterUserTable
                        .selectAll()
                        .where { (ChapterUserTable.chapter eq chapterId) and (ChapterUserTable.user eq userId) }
                        .any()
                if (!hasRow) {
                    ChapterUserTable.insert {
                        it[chapter] = chapterId
                        it[user] = userId
                    }
                }
                exec("UPDATE CHAPTERUSER SET FILLERMARK = TRUE WHERE CHAPTER = $chapterId AND USER_ID = $userId")
            }
        }
    }

    private fun isBlacklisted(
        chapterId: Int,
        userId: Int,
    ): Boolean =
        transaction {
            ChapterUserTable
                .selectAll()
                .where { (ChapterUserTable.chapter eq chapterId) and (ChapterUserTable.user eq userId) }
                .singleOrNull()
                ?.get(ChapterUserTable.isBlacklisted) == true
        }

    private fun fillermarkColumnExists(): Boolean =
        transaction {
            (
                TransactionManager.current().exec(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'CHAPTERUSER' AND COLUMN_NAME = 'FILLERMARK'",
                ) { rs ->
                    rs.next()
                    rs.getInt(1)
                } ?: 0
            ) > 0
        }

    @Test
    fun `fillermarks become per-user blacklist entries without false matches, merged with existing entries`() {
        val user2 = createUser("fillermark-user-2")
        val mangaId = createLibraryManga("FILLERMARK_MIGRATION")
        transaction {
            MangaUserTable.insert {
                it[manga] = mangaId
                it[user] = user2
                it[inLibrary] = true
            }
        }
        // a special and a regular chapter with the same number and name, two scanlators, a chapter without number
        val ids =
            insertChapters(
                mangaId,
                listOf(
                    Triple("/5-special", 5f, "A"),
                    Triple("/5", 5f, "A"),
                    Triple("/5-b", 5f, "B"),
                    Triple("/extra", -1f, null),
                    Triple("/6", 6f, "A"),
                ),
            )
        // user 1 already blacklisted chapter 6
        ChapterVisibilityService.setBlacklisted(1, listOf(ids.getValue("/6")), true)
        fillermark(1, ids.getValue("/5-special"), ids.getValue("/extra"), ids.getValue("/6"))
        fillermark(user2, ids.getValue("/5-b"))
        transaction {
            GlobalMetaTable.insert {
                it[key] = "webUI_deleteChaptersWithFillermark"
                it[value] = "true"
                it[user] = 1
            }
        }

        ForkSchema.ensure()
        ForkSchema.ensure()

        assertTrue(isBlacklisted(ids.getValue("/5-special"), 1))
        assertTrue(isBlacklisted(ids.getValue("/extra"), 1))
        assertTrue(isBlacklisted(ids.getValue("/6"), 1), "existing entries survive")
        assertFalse(isBlacklisted(ids.getValue("/5"), 1), "same number and name, different URL: not matched")
        assertFalse(isBlacklisted(ids.getValue("/5-b"), 1), "another user's fillermark")
        assertTrue(isBlacklisted(ids.getValue("/5-b"), user2))
        assertFalse(isBlacklisted(ids.getValue("/5-special"), user2))
        assertEquals(3, ScanlatorSettings.get(1, mangaId).blacklistedChapters.size, "no duplicate for chapter 6")
        assertEquals(1, ScanlatorSettings.get(user2, mangaId).blacklistedChapters.size)
        assertEquals(setOf(ids.getValue("/5"), ids.getValue("/5-b")), Manga.getUnreadChapters(1, mangaId).map { it.id }.toSet())

        assertFalse(fillermarkColumnExists(), "the fillermark column is removed after the verified migration")
        val leftoverSetting =
            transaction { GlobalMetaTable.selectAll().where { GlobalMetaTable.key eq "webUI_deleteChaptersWithFillermark" }.count() }
        assertEquals(0, leftoverSetting, "settings of the removed feature are cleaned up")
    }

    @Test
    fun `an interrupted migration resumes without duplicating entries`() {
        val mangaId = createLibraryManga("FILLERMARK_RESUME")
        val ids = insertChapters(mangaId, listOf(Triple("/1", 1f, "A"), Triple("/2", 2f, "A")))
        fillermark(1, ids.getValue("/1"))
        // the entry was written by an earlier, interrupted start: the column still exists
        ScanlatorSettings.update(
            1,
            mangaId,
            blacklistedChapters =
                listOf(
                    ChapterBlacklist.hybridKey(
                        ChapterVisibilityService.loadChapters(listOf(mangaId)).getValue(mangaId).first {
                            it.url ==
                                "/1"
                        },
                    ),
                ),
        )

        ForkSchema.ensure()

        assertEquals(1, ScanlatorSettings.get(1, mangaId).blacklistedChapters.size)
        assertTrue(isBlacklisted(ids.getValue("/1"), 1))
        assertFalse(fillermarkColumnExists())
    }

    @Test
    fun `a migrated entry follows its chapter through a source url change`() =
        runTest {
            val mangaId = createLibraryManga("FILLERMARK_URL_CHANGE")
            val ids = insertChapters(mangaId, listOf(Triple("2", 2f, null), Triple("1", 1f, null)))
            fillermark(1, ids.getValue("2"))
            ForkSchema.ensure()

            val mangaEntry = transaction { MangaTable.selectAll().where { MangaTable.id eq mangaId }.first() }
            val fetchedChapters =
                listOf("1", "2-new").map { url ->
                    SChapter.create().apply {
                        this.url = url
                        this.name = "Chapter ${url.removeSuffix("-new")}.0"
                        this.chapter_number = url.removeSuffix("-new").toFloat()
                    }
                }
            Chapter.updateChapterListDatabase(mangaEntry, fetchedChapters, StubSource(1L))

            val newChapterId =
                transaction {
                    ChapterTable
                        .selectAll()
                        .where { (ChapterTable.manga eq mangaId) and (ChapterTable.url eq "2-new") }
                        .single()[ChapterTable.id]
                        .value
                }
            assertTrue(isBlacklisted(newChapterId, 1))
        }
}

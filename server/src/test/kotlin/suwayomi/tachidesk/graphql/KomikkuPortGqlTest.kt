package suwayomi.tachidesk.graphql

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import kotlinx.serialization.json.JsonObject
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.neq
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.AfterEach
import suwayomi.tachidesk.global.model.table.UserAccountTable
import suwayomi.tachidesk.global.model.table.UserSettingsTable
import suwayomi.tachidesk.manga.impl.chapter.ChapterVisibilityService
import suwayomi.tachidesk.manga.impl.util.lang.EMPTY
import suwayomi.tachidesk.manga.model.table.CategoryMangaTable
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ChapterUserTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.server.settings.UserSettings
import suwayomi.tachidesk.server.settings.userConfig
import suwayomi.tachidesk.server.user.UserType
import suwayomi.tachidesk.test.GraphQLTest
import suwayomi.tachidesk.test.clearTables
import suwayomi.tachidesk.test.createLibraryManga
import kotlin.test.Test
import kotlin.test.assertEquals

/** fork (Komikku port): the new API end to end, as two different users */
class KomikkuPortGqlTest : GraphQLTest() {
    private val createdUsers = mutableListOf<Int>()

    @AfterEach
    fun tearDown() {
        (createdUsers + 1).forEach { UserSettings.resetAll(it) }
        clearTables(CategoryMangaTable, ChapterUserTable, ChapterTable, MangaUserTable, MangaTable)
        transaction {
            CategoryTable.deleteWhere { CategoryTable.isDefaultCategory neq true }
            UserSettingsTable.deleteWhere { UserSettingsTable.user eq 1 }
            createdUsers.forEach { id -> UserAccountTable.deleteWhere { UserAccountTable.id eq id } }
        }
        createdUsers.clear()
    }

    private fun secondUser(): Pair<Int, UserType> {
        val id = createTestUser("komikku-port-user-${System.nanoTime()}")
        createdUsers += id
        return id to UserType.Admin(id)
    }

    /** chapters (url, number, scanlator) of a manga in the library of [userIds] */
    private fun manga(
        title: String,
        chapters: List<Triple<String, Float, String>>,
        userIds: List<Int>,
    ): Pair<Int, Map<String, Int>> {
        val mangaId = createLibraryManga(title)
        transaction {
            userIds.filter { it != 1 }.forEach { userId ->
                MangaUserTable.insert {
                    it[manga] = mangaId
                    it[user] = userId
                    it[inLibrary] = true
                }
            }
        }
        val ids =
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
        // what a chapter list refresh does
        ChapterVisibilityService.recomputeForManga(mangaId)
        return mangaId to ids
    }

    private fun updateChapters(
        ids: List<Int>,
        patch: String,
        user: UserType,
    ) = graphql(
        "mutation { updateChapters(input: { ids: ${ids.joinToString(
            prefix = "[",
            postfix = "]",
        )}, patch: { $patch } }) { chapters { id } } }",
        user = user,
    ).assertNoErrors()

    @Test
    fun `bookmark colors, blacklist and the Updates filters are per user`() {
        val (user2, asUser2) = secondUser()
        val (mangaId, ids) =
            manga("GQL_COLORS", listOf(Triple("/1", 1f, "A"), Triple("/2", 2f, "A"), Triple("/3", 3f, "A")), listOf(1, user2))

        updateChapters(listOf(ids.getValue("/1")), "bookmarkColor: 1", admin)
        updateChapters(listOf(ids.getValue("/2")), "bookmarkColor: 5", admin)
        updateChapters(listOf(ids.getValue("/3")), "isBookmarked: true", admin)
        updateChapters(listOf(ids.getValue("/1")), "bookmarkColor: 4", asUser2)
        // a hidden chapter's color does not count for the library filter
        updateChapters(listOf(ids.getValue("/2")), "isBlacklisted: true", admin)

        val colors = "{ manga(id: $mangaId) { bookmarkColors bookmarkCount unreadCount } }"
        val adminManga = graphql(colors).also { it.assertNoErrors() }
        assertEquals(listOf(0, 1), adminManga.dataPath("manga", "bookmarkColors"))
        assertEquals(2, adminManga.dataPath("manga", "bookmarkCount"))
        assertEquals(2, adminManga.dataPath("manga", "unreadCount"))
        val user2Manga = graphql(colors, user = asUser2).also { it.assertNoErrors() }
        assertEquals(listOf(4), user2Manga.dataPath("manga", "bookmarkColors"))
        assertEquals(3, user2Manga.dataPath("manga", "unreadCount"))

        // Updates: bookmarked chapters in red or default, hidden chapters left out
        val updates =
            graphql(
                """
                { chapters(filter: { mangaId: { equalTo: $mangaId }, isHidden: { equalTo: false },
                    isBookmarked: { equalTo: true }, bookmarkColor: { in: [0, 1] } }) { nodes { id bookmarkColor } } }
                """.trimIndent(),
            ).also { it.assertNoErrors() }

        @Suppress("UNCHECKED_CAST")
        val nodes = updates.dataPath("chapters", "nodes") as List<Map<String, Any?>>
        assertEquals(setOf(ids.getValue("/1"), ids.getValue("/3")), nodes.map { it["id"] }.toSet())

        // removing the bookmark removes its color
        updateChapters(listOf(ids.getValue("/1")), "isBookmarked: false", admin)
        assertEquals(listOf(0), graphql(colors).dataPath("manga", "bookmarkColors"))
    }

    @Test
    fun `the global smart merge setting is a per-user setting`() {
        val (user2, asUser2) = secondUser()
        UserSettings.set(1, userConfig.smartScanlatorMerge, false)
        val (mangaId, _) = manga("GQL_MERGE", listOf(Triple("/a1", 1f, "A"), Triple("/b1", 1f, "B")), listOf(1, user2))

        graphql(
            "mutation { setUserSettings(input: { userSettings: { smartScanlatorMerge: true } }) { userSettings { smartScanlatorMerge } } }",
        ).assertNoErrors()
        assertEquals(true, graphql("{ userSettings { smartScanlatorMerge } }").dataPath("userSettings", "smartScanlatorMerge"))
        assertEquals(
            false,
            graphql("{ userSettings { smartScanlatorMerge } }", user = asUser2).dataPath("userSettings", "smartScanlatorMerge"),
        )
        assertEquals(1, graphql("{ manga(id: $mangaId) { unreadCount } }").dataPath("manga", "unreadCount"))
        assertEquals(2, graphql("{ manga(id: $mangaId) { unreadCount } }", user = asUser2).dataPath("manga", "unreadCount"))
    }

    @Test
    fun `rapid scanlator edits end in the last state and survive new scanlators`() {
        val (mangaId, ids) =
            manga("GQL_RAPID", listOf(Triple("/a1", 1f, "A"), Triple("/b1", 1f, "B"), Triple("/c2", 2f, "C")), listOf(1))
        val update = { input: String ->
            graphql(
                "mutation { updateMangaScanlatorSettings(input: { mangaId: $mangaId, $input }) { manga { id } } }",
            ).assertNoErrors()
        }
        update("smartMergeMode: ENABLED")
        update("scanlatorPriority: [\"B\", \"A\", \"C\"]")
        update("scanlatorPriority: [\"A\", \"B\", \"C\"]")
        update("scanlatorRangeRules: [{ from: 1, to: 9999, scanlator: \"B\" }, { from: 1, to: 1, scanlator: \"A\" }]")

        val settings =
            graphql(
                "{ manga(id: $mangaId) { scanlatorSettings { scanlatorPriority scanlatorRangeRules { from to scanlator } } } }",
            ).also { it.assertNoErrors() }
        assertEquals(listOf("A", "B", "C"), settings.dataPath("manga", "scanlatorSettings", "scanlatorPriority"))
        val chapters = "{ chapters(filter: { mangaId: { equalTo: $mangaId }, isHidden: { equalTo: false } }) { nodes { id } } }"

        @Suppress("UNCHECKED_CAST")
        fun visible() = (graphql(chapters).dataPath("chapters", "nodes") as List<Map<String, Any?>>).map { it["id"] }.toSet()
        // the later rule (1-1 → A) wins over the earlier one for chapter 1
        assertEquals(setOf(ids.getValue("/a1"), ids.getValue("/c2")), visible())

        // a new scanlator appears for chapter 2: the saved order is unchanged, the new scanlator is appended
        transaction {
            ChapterTable.insert {
                it[url] = "/d2"
                it[name] = "Chapter 2"
                it[chapter_number] = 2f
                it[scanlator] = "D"
                it[sourceOrder] = 0
                it[manga] = mangaId
                it[memo] = JsonObject.EMPTY
            }
        }
        ChapterVisibilityService.recomputeForManga(mangaId)
        assertEquals(
            listOf("A", "B", "C", "D"),
            graphql("{ manga(id: $mangaId) { scanlatorSettings { scanlatorPriority } } }")
                .dataPath("manga", "scanlatorSettings", "scanlatorPriority"),
        )
        // chapter 2: range rule 1-9999 prefers B, B has no chapter 2, so the priority picks C over D
        assertEquals(setOf(ids.getValue("/a1"), ids.getValue("/c2")), visible())
    }

    @Test
    fun `hidden categories and update intervals are settings of the user's own categories`() {
        val (_, asUser2) = secondUser()
        val created =
            graphql("mutation { createCategory(input: { name: \"Hidden\" }) { category { id } } }").also { it.assertNoErrors() }
        val id = created.dataPath("createCategory", "category", "id")
        graphql(
            "mutation { updateCategory(input: { id: $id, patch: { hidden: true, updateInterval: 7 } }) { category { id hidden updateInterval } } }",
        ).also {
            it.assertNoErrors()
            assertEquals(true, it.dataPath("updateCategory", "category", "hidden"))
            assertEquals(7, it.dataPath("updateCategory", "category", "updateInterval"))
        }
        graphql("mutation { updateCategory(input: { id: $id, patch: { updateInterval: 0 } }) { category { id } } }").assertHasError()

        // a hidden subcategory does not count for the library tab of its parent
        val parent =
            graphql(
                "mutation { createCategory(input: { name: \"Parent\" }) { category { id } } }",
            ).dataPath("createCategory", "category", "id")
        val child =
            graphql("mutation { createCategory(input: { name: \"Child\", parentId: $parent }) { category { id } } }")
                .dataPath("createCategory", "category", "id")
        val (mangaId, _) = manga("GQL_HIDDEN", listOf(Triple("/1", 1f, "A")), listOf(1))
        graphql("mutation { updateMangaCategories(input: { id: $mangaId, patch: { addToCategories: [$child] } }) { manga { id } } }")
            .assertNoErrors()
        val counts = "{ category(id: $parent) { all: subtreeMangaCount visible: subtreeMangaCount(includeHidden: false) } }"
        assertEquals(1, graphql(counts).dataPath("category", "visible"))
        graphql("mutation { updateCategory(input: { id: $child, patch: { hidden: true } }) { category { id } } }").assertNoErrors()
        assertEquals(1, graphql(counts).dataPath("category", "all"))
        assertEquals(0, graphql(counts).dataPath("category", "visible"))

        // another user cannot change it
        graphql("mutation { updateCategory(input: { id: $id, patch: { hidden: false } }) { category { id } } }", user = asUser2)
        assertEquals(true, graphql("{ category(id: $id) { hidden } }").dataPath("category", "hidden"))
    }
}

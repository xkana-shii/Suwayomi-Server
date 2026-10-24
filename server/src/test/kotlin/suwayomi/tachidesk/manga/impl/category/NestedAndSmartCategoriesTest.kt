package suwayomi.tachidesk.manga.impl.category

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
import org.jetbrains.exposed.v1.jdbc.update
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import suwayomi.tachidesk.global.model.table.UserAccountTable
import suwayomi.tachidesk.manga.impl.Category
import suwayomi.tachidesk.manga.impl.CategoryManga
import suwayomi.tachidesk.manga.impl.track.Track
import suwayomi.tachidesk.manga.impl.track.UnifiedTrackStatus
import suwayomi.tachidesk.manga.impl.track.tracker.TrackerManager
import suwayomi.tachidesk.manga.impl.track.tracker.anilist.Anilist
import suwayomi.tachidesk.manga.impl.track.tracker.model.TrackImpl
import suwayomi.tachidesk.manga.impl.track.tracker.myanimelist.MyAnimeList
import suwayomi.tachidesk.manga.model.table.CategoryMangaTable
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.manga.model.table.TrackRecordTable
import suwayomi.tachidesk.test.ApplicationTest
import suwayomi.tachidesk.test.clearTables
import suwayomi.tachidesk.test.createLibraryManga
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NestedAndSmartCategoriesTest : ApplicationTest() {
    private val createdUserIds = mutableListOf<Int>()

    @AfterEach
    fun tearDown() {
        clearTables(TrackRecordTable, CategoryMangaTable, MangaUserTable, MangaTable)
        transaction {
            CategoryTable.deleteWhere { CategoryTable.isDefaultCategory neq true }
            createdUserIds.forEach { id -> UserAccountTable.deleteWhere { UserAccountTable.id eq id } }
        }
        createdUserIds.clear()
    }

    private fun category(
        name: String,
        parentId: Int? = null,
        trackStatus: UnifiedTrackStatus? = null,
        userId: Int = 1,
    ): Int {
        Category.requireValidPlacement(userId, null, name, parentId)
        return transaction {
            CategoryTable
                .insertAndGetId {
                    it[CategoryTable.name] = name
                    it[CategoryTable.user] = userId
                    it[CategoryTable.order] = Int.MAX_VALUE
                    it[CategoryTable.parentId] = parentId
                    it[CategoryTable.trackStatus] = trackStatus?.value
                }.value
        }.also { Category.normalizeCategories(userId) }
    }

    private fun membership(
        mangaId: Int,
        userId: Int = 1,
    ) = CategoryManga.getMangaCategories(userId, mangaId).map { it.id }.toSet()

    private fun track(
        mangaId: Int,
        trackerId: Int,
        status: Int,
        userId: Int = 1,
    ) = Track.upsertTrackRecord(
        userId,
        TrackImpl().apply {
            manga_id = mangaId
            tracker_id = trackerId
            remote_id = mangaId.toLong()
            title = "t"
            this.status = status
        },
    )

    // -- pure tree rules --

    @Test
    fun `tree walks are cycle safe and moves into the own subtree are rejected`() {
        val tree = CategoryTree(mapOf(1 to null, 2 to 1, 3 to 2, 4 to null, 5 to 6, 6 to 5, 7 to 99))
        assertEquals(listOf(2, 1), tree.ancestors(3))
        assertEquals(setOf(2, 3), tree.descendants(1))
        assertEquals(setOf(3, 2, 1, 4), tree.withAncestors(listOf(3, 4)))
        assertFalse(tree.canMove(1, 3))
        assertFalse(tree.canMove(1, 1))
        assertTrue(tree.canMove(3, 4))
        assertTrue(tree.canMove(3, null))
        // the cycle 5 <-> 6 terminates, the orphan 7 is top level
        assertEquals(listOf(6), tree.ancestors(5))
        assertEquals(null, tree.parentOf(7))
        assertEquals(setOf(1, 2, 3, 4, 5, 6, 7), tree.flattened { it }.toSet())
        assertEquals(listOf(1, 2, 3, 4, 7), tree.flattened { it }.take(5))
    }

    @Test
    fun `smart membership is scoped by reachability and keeps the branch anchored`() {
        // Shounen(1) / Reading(2, READING) / Completed(3, COMPLETED); Josei(4) / Completed(5, COMPLETED); Reading(6, READING) top level
        val tree = CategoryTree(mapOf(1 to null, 2 to 1, 3 to 1, 4 to null, 5 to 4, 6 to null, 7 to null))
        val bound =
            listOf(
                SmartCategories.BoundCategory(2, 1, UnifiedTrackStatus.READING.value),
                SmartCategories.BoundCategory(3, 1, UnifiedTrackStatus.COMPLETED.value),
                SmartCategories.BoundCategory(5, 4, UnifiedTrackStatus.COMPLETED.value),
                SmartCategories.BoundCategory(6, null, UnifiedTrackStatus.READING.value),
            )
        // a manga in Shounen/Reading (and a manual category 7) completes: only the Shounen branch changes
        assertEquals(setOf(3, 7), SmartCategories.updatedMembership(setOf(2, 6, 7), bound, tree, UnifiedTrackStatus.COMPLETED.value))
        // a manga in no branch only gets the unscoped top level binding
        assertEquals(setOf(6), SmartCategories.updatedMembership(emptySet(), bound, tree, UnifiedTrackStatus.READING.value))
        // dropping it removes the bound categories but anchors the manga in its branch
        assertEquals(setOf(1, 7), SmartCategories.updatedMembership(setOf(2, 7), bound, tree, UnifiedTrackStatus.DROPPED.value))
    }

    @Test
    fun `unified status is deterministic across trackers`() {
        val records = listOf(TrackerManager.ANILIST to Anilist.COMPLETED, TrackerManager.MYANIMELIST to MyAnimeList.READING)
        assertEquals(UnifiedTrackStatus.READING, UnifiedTrackStatus.ofRecords(records))
        assertEquals(UnifiedTrackStatus.READING, UnifiedTrackStatus.ofRecords(records.reversed()))
        assertEquals(MyAnimeList.PLAN_TO_READ, UnifiedTrackStatus.toTracker(TrackerManager.MYANIMELIST, UnifiedTrackStatus.PLAN_TO_READ))
    }

    // -- database --

    @Test
    fun `names are unique among siblings only and deleting a parent keeps its children`() {
        val shounen = category("Shounen")
        val josei = category("Josei")
        val reading = category("Reading", shounen)
        category("Reading", josei)
        assertThrows<IllegalArgumentException> { category("reading", shounen) }
        assertThrows<IllegalArgumentException> { Category.setParent(1, listOf(shounen), reading) }
        assertThrows<IllegalArgumentException> { Category.setParent(1, listOf(shounen), Category.getDefaultCategoryId(1)!!) }

        Category.removeCategory(1, shounen)
        assertEquals(null, Category.getCategoryById(1, reading)!!.parentId, "children move up to the deleted category's parent")
    }

    @Test
    fun `tracker changes file library manga into smart categories without touching manual ones`() {
        val user2 =
            transaction {
                UserAccountTable.insertAndGetId {
                    it[username] = "smart-categories-user-2"
                    it[password] = "password"
                }.value
            }.also(createdUserIds::add)
        val shounen = category("Shounen")
        val reading = category("Reading", shounen, UnifiedTrackStatus.READING)
        val completed = category("Completed", shounen, UnifiedTrackStatus.COMPLETED)
        val manual = category("Favourites")
        val user2Reading = category("Reading", null, UnifiedTrackStatus.READING, user2)

        val mangaId = createLibraryManga("SMART_CATEGORIES")
        CategoryManga.addMangasToCategories(1, listOf(mangaId), listOf(reading, manual))

        track(mangaId, TrackerManager.MYANIMELIST, MyAnimeList.COMPLETED)
        assertEquals(setOf(completed, manual), membership(mangaId))
        assertEquals(emptySet(), membership(mangaId, user2), "other users are not affected")

        // a manga that is not in the library is never filed
        val notInLibrary = createLibraryManga("SMART_CATEGORIES_NOT_IN_LIBRARY")
        transaction { MangaUserTable.update({ MangaUserTable.manga eq notInLibrary and (MangaUserTable.user eq 1) }) { it[inLibrary] = false } }
        track(notInLibrary, TrackerManager.MYANIMELIST, MyAnimeList.READING)
        assertEquals(emptySet(), membership(notInLibrary))

        // unbinding the last tracker leaves the membership as it is (no usable status)
        val recordId = transaction { TrackRecordTable.selectAll().where { TrackRecordTable.mangaId eq mangaId }.single()[TrackRecordTable.id].value }
        kotlinx.coroutines.runBlocking { Track.unbind(1, recordId) }
        assertEquals(setOf(completed, manual), membership(mangaId))
        assertTrue(user2Reading > 0)
    }
}

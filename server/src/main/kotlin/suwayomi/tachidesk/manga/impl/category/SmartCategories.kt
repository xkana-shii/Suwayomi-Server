package suwayomi.tachidesk.manga.impl.category

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.isNotNull
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.graphql.server.primitives.inIds
import suwayomi.tachidesk.manga.impl.Category
import suwayomi.tachidesk.manga.impl.CategoryManga
import suwayomi.tachidesk.manga.impl.track.UnifiedTrackStatus
import suwayomi.tachidesk.manga.model.table.CategoryMangaTable
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.manga.model.table.TrackRecordTable
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * fork: Smart Categories (Komikku port of SyncCategoriesWithTrackStatus): keeps the tracker-status-bound categories of
 * library manga in line with their tracker status.
 *
 * Only bound categories are ever added or removed, and only when in scope; manual memberships and unrelated branches
 * are never touched. Syncing only writes category memberships, so it cannot trigger itself through the tracker hooks.
 */
object SmartCategories {
    private val logger = KotlinLogging.logger {}
    private val lock = ReentrantLock()

    data class BoundCategory(
        val id: Int,
        val parentId: Int?,
        val trackStatus: Int,
    )

    /**
     * The membership after applying [trackStatus]:
     * - a bound category without a parent is always in scope;
     * - a bound subcategory is in scope only when its parent is reached by the membership (one of the manga's
     *   categories or an ancestor of one), at any depth: a manga in `Shounen / Reading` reaches `Shounen`, so
     *   `Shounen / Completed` is in scope, `Josei / Completed` is not;
     * - in-scope bound categories are added when their status matches and removed otherwise (a null status removes);
     * - when a removal leaves the parent unreached, the parent is kept as membership so the manga stays in its branch.
     */
    fun updatedMembership(
        current: Set<Int>,
        bound: List<BoundCategory>,
        tree: CategoryTree,
        trackStatus: Int?,
    ): Set<Int> {
        if (bound.isEmpty()) return current
        val reachable = tree.withAncestors(current)
        val inScope = bound.filter { it.parentId == null || it.parentId !in tree.ids || it.parentId in reachable }

        val result = current.toMutableSet()
        val removed = mutableListOf<BoundCategory>()
        inScope.forEach { category ->
            if (trackStatus != null && category.trackStatus == trackStatus) {
                result += category.id
            } else if (result.remove(category.id)) {
                removed += category
            }
        }
        removed.forEach { category ->
            val parentId = category.parentId?.takeIf { it in tree.ids } ?: return@forEach
            if (parentId !in tree.withAncestors(result)) result += parentId
        }
        return result
    }

    /** Syncs the given manga of [userId]; manga that are not in the library are left alone. */
    fun syncManga(
        userId: Int,
        mangaIds: Collection<Int>,
    ) {
        if (mangaIds.isEmpty()) return
        lock.withLock {
            transaction {
                val bound =
                    CategoryTable
                        .select(CategoryTable.id, CategoryTable.parentId, CategoryTable.trackStatus)
                        .where {
                            (CategoryTable.user eq userId) and (CategoryTable.trackStatus.isNotNull()) and
                                (CategoryTable.isDefaultCategory eq false)
                        }.map { BoundCategory(it[CategoryTable.id].value, it[CategoryTable.parentId], it[CategoryTable.trackStatus]!!) }
                if (bound.isEmpty()) return@transaction

                val libraryMangaIds =
                    MangaUserTable
                        .select(MangaUserTable.manga)
                        .where { (MangaUserTable.manga inIds mangaIds) and (MangaUserTable.user eq userId) and (MangaUserTable.inLibrary eq true) }
                        .map { it[MangaUserTable.manga].value }
                if (libraryMangaIds.isEmpty()) return@transaction

                val tree = Category.getCategoryTree(userId)
                val statusByManga =
                    TrackRecordTable
                        .select(TrackRecordTable.mangaId, TrackRecordTable.trackerId, TrackRecordTable.status)
                        .where { (TrackRecordTable.mangaId inIds libraryMangaIds) and (TrackRecordTable.user eq userId) }
                        .groupBy({ it[TrackRecordTable.mangaId].value }, { it[TrackRecordTable.trackerId] to it[TrackRecordTable.status] })
                        .mapValues { (_, records) -> UnifiedTrackStatus.ofRecords(records)?.value }
                val membershipByManga =
                    CategoryMangaTable
                        .select(CategoryMangaTable.manga, CategoryMangaTable.category)
                        .where { (CategoryMangaTable.manga inIds libraryMangaIds) and (CategoryMangaTable.user eq userId) }
                        .groupBy({ it[CategoryMangaTable.manga].value }, { it[CategoryMangaTable.category].value })

                libraryMangaIds.forEach { mangaId ->
                    // like Komikku: without a usable tracker status (untracked, or no mappable status) nothing changes
                    val status = statusByManga[mangaId] ?: return@forEach
                    val current = membershipByManga[mangaId].orEmpty().toSet()
                    val updated = updatedMembership(current, bound, tree, status)
                    if (updated == current) return@forEach
                    logger.debug { "smart categories: manga $mangaId of user $userId $current -> $updated" }
                    (current - updated).forEach { CategoryManga.removeMangaFromCategory(userId, mangaId, it) }
                    (updated - current).takeIf { it.isNotEmpty() }?.let {
                        CategoryManga.addMangasToCategories(userId, listOf(mangaId), it.toList())
                    }
                }
            }
        }
    }

    /** Syncs every library manga of [userId], e.g. after a category's binding changed. */
    fun syncUser(userId: Int) {
        val mangaIds =
            transaction {
                MangaUserTable
                    .select(MangaUserTable.manga)
                    .where { (MangaUserTable.user eq userId) and (MangaUserTable.inLibrary eq true) }
                    .map { it[MangaUserTable.manga].value }
            }
        mangaIds.chunked(500).forEach { syncManga(userId, it) }
    }
}

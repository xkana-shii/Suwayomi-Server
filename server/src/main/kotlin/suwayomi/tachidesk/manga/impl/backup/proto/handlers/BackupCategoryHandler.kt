package suwayomi.tachidesk.manga.impl.backup.proto.handlers

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.insertAndGetId
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import suwayomi.tachidesk.manga.impl.Category
import suwayomi.tachidesk.manga.impl.Category.modifyCategoriesMetas
import suwayomi.tachidesk.manga.impl.backup.BackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.SyncRestoreMode
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupCategory
import suwayomi.tachidesk.manga.impl.category.CategoryTree
import suwayomi.tachidesk.manga.impl.category.SmartCategories
import suwayomi.tachidesk.manga.impl.track.UnifiedTrackStatus
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.server.database.dbTransaction

object BackupCategoryHandler {
    fun backup(
        userId: Int,
        flags: BackupFlags,
    ): List<BackupCategory> =
        dbTransaction {
            val categories =
                CategoryTable
                    .selectAll()
                    .where { CategoryTable.user eq userId }
                    .orderBy(CategoryTable.order to SortOrder.ASC)
                    .toList()

            val categoryToMeta =
                if (flags.includeClientData) {
                    Category.getCategoriesMetaMaps(userId, categories.map { it[CategoryTable.id].value })
                } else {
                    emptyMap()
                }

            val uidById = categories.associate { it[CategoryTable.id].value to it[CategoryTable.uid] }

            categories.map {
                BackupCategory(
                    it[CategoryTable.name],
                    it[CategoryTable.order],
                    it[CategoryTable.flags],
                    it[CategoryTable.version],
                    it[CategoryTable.uid],
                    it[CategoryTable.lastModifiedAt],
                ).apply {
                    this.meta = categoryToMeta[it[CategoryTable.id].value] ?: emptyMap()
                    // fork (Komikku port): hierarchy and Smart Category binding
                    id = it[CategoryTable.id].value.toLong()
                    parentId = it[CategoryTable.parentId]?.toLong() ?: 0L
                    parentUid = it[CategoryTable.parentId]?.let(uidById::get) ?: 0L
                    trackStatus = it[CategoryTable.trackStatus]
                    hierarchyKnown = true
                }
            }
        }

    fun restore(
        userId: Int,
        backupCategories: List<BackupCategory>,
        syncMode: SyncRestoreMode = SyncRestoreMode.NONE,
    ): Map<Int, Int> {
        val dbCategories = Category.getCategoryList(userId, includeDefault = true)
        val dbCategoriesByUid = dbCategories.associateBy { it.uid }

        var nextOrder = dbCategories.maxOfOrNull { it.order }?.plus(1) ?: 0

        // the wire is 0-based (Mihon/SY convention); store 1-based ranks instead of raw orders
        val ranks = IntArray(backupCategories.size)
        backupCategories
            .withIndex()
            .filter { (_, backupCategory) -> !backupCategory.name.equals(Category.DEFAULT_CATEGORY_NAME, true) }
            .sortedBy { (_, backupCategory) -> backupCategory.order }
            .forEachIndexed { rank, (index, _) -> ranks[index] = rank + 1 }

        // fork (Komikku port): parents are resolved before their children, so a nested category is matched by name
        // among the children of its (restored) parent only; names only have to be unique among siblings
        val backupIndexById = backupCategories.withIndex().filter { it.value.id != 0L }.associate { it.value.id to it.index }
        val backupIndexByUid = backupCategories.withIndex().filter { it.value.uid != 0L }.associate { it.value.uid to it.index }

        fun backupParentIndex(backupCategory: BackupCategory): Int? =
            when {
                backupCategory.parentUid != 0L -> backupIndexByUid[backupCategory.parentUid]
                backupCategory.parentId != 0L -> backupIndexById[backupCategory.parentId]
                else -> null
            } ?: backupCategory.parentId.takeIf { it != 0L }?.let(backupIndexById::get)

        val backupTree =
            CategoryTree(backupCategories.indices.associateWith { index -> backupParentIndex(backupCategories[index]) })
        val processingOrder = backupCategories.indices.sortedBy { backupTree.depth(it) }
        val restoredIds = IntArray(backupCategories.size)

        fun localParentOf(index: Int): Int? = backupTree.parentOf(index)?.let { restoredIds[it].takeIf { id -> id != 0 } }

        transaction {
            processingOrder.forEach { index ->
                val backupCategory = backupCategories[index]
                restoredIds[index] =
                    run restoreCategory@{
                        var dbCategory =
                            if (backupCategory.uid != 0L) {
                                dbCategoriesByUid[backupCategory.uid]
                            } else {
                                null
                            }

                        if (dbCategory == null) {
                            val sameName = dbCategories.filter { it.name == backupCategory.name }
                            dbCategory =
                                if (backupCategory.hierarchyKnown) {
                                    val parent = localParentOf(index)
                                    sameName.firstOrNull { it.parentId == parent }
                                } else {
                                    // a backup without hierarchy (Mihon, older versions): prefer a top level category
                                    sameName.firstOrNull { it.parentId == null } ?: sameName.singleOrNull()
                                }
                        }

                        if (dbCategory != null) {
                            // a newer local copy (pending reorder/rename) wins the next upload
                            if (syncMode == SyncRestoreMode.ADOPT && backupCategory.version < dbCategory.version) {
                                return@restoreCategory dbCategory.id
                            }
                            CategoryTable.update({ CategoryTable.id eq dbCategory.id }) {
                                it[name] = backupCategory.name
                                it[order] = ranks[index]
                                it[version] = backupCategory.version
                                it[uid] = if (backupCategory.uid != 0L) backupCategory.uid else dbCategory.uid
                                it[lastModifiedAt] = backupCategory.lastModifiedAt
                                // outside ADOPT a zeroed backup must not wipe stored flags
                                if (syncMode == SyncRestoreMode.ADOPT || backupCategory.flags != 0) {
                                    it[flags] = backupCategory.flags
                                }
                                it[isSyncing] = true
                            }
                            return@restoreCategory dbCategory.id
                        }

                        // sync mirrors the server list; a plain restore appends new categories at the end
                        val currentOrder = if (syncMode.isSync) ranks[index] else nextOrder++
                        CategoryTable
                            .insertAndGetId {
                                it[name] = backupCategory.name
                                it[order] = currentOrder
                                it[version] = backupCategory.version
                                it[uid] = backupCategory.uid
                                it[lastModifiedAt] = backupCategory.lastModifiedAt
                                it[user] = userId
                                it[flags] = backupCategory.flags
                            }.value
                    }
            }
        }
        val categoryIds = restoredIds.toList()

        restoreHierarchy(userId, backupCategories, categoryIds, backupTree)

        Category.normalizeCategories(userId)

        transaction {
            CategoryTable.update({ CategoryTable.user eq userId and (CategoryTable.isSyncing eq true) }) {
                it[isSyncing] = false
            }
        }

        val metaEntryByCategoryId =
            categoryIds
                .zip(backupCategories)
                .associate { (categoryId, backupCategory) ->
                    categoryId to backupCategory.meta
                }

        modifyCategoriesMetas(userId, metaEntryByCategoryId)

        return backupCategories.withIndex().associate { (index, backupCategory) ->
            backupCategory.order to categoryIds[index]
        }
    }

    /**
     * fork (Komikku port): applies parents and Smart Category bindings of a backup that knows about them. A backup
     * without hierarchy leaves the local hierarchy alone; a parent that would create a cycle is ignored.
     */
    private fun restoreHierarchy(
        userId: Int,
        backupCategories: List<BackupCategory>,
        categoryIds: List<Int>,
        backupTree: CategoryTree,
    ) {
        val defaultCategoryId = Category.getDefaultCategoryId(userId)
        var changedBindings = false
        transaction {
            backupCategories.forEachIndexed { index, backupCategory ->
                val categoryId = categoryIds[index]
                if (categoryId == defaultCategoryId) return@forEachIndexed

                if (backupCategory.hierarchyKnown) {
                    val parentId =
                        backupTree.parentOf(index)?.let { categoryIds[it] }?.takeIf { it != defaultCategoryId && it != categoryId }
                    if (Category.getCategoryTree(userId).canMove(categoryId, parentId)) {
                        CategoryTable.update({ CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }) {
                            it[CategoryTable.parentId] = parentId
                        }
                    }
                }
                val trackStatus = backupCategory.trackStatus?.takeIf { UnifiedTrackStatus.fromValue(it) != null }
                if (backupCategory.hierarchyKnown || trackStatus != null) {
                    CategoryTable.update({ CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }) {
                        it[CategoryTable.trackStatus] = trackStatus
                    }
                    changedBindings = changedBindings || trackStatus != null
                }
            }
        }
        if (changedBindings) SmartCategories.syncUser(userId)
    }
}

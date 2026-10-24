package suwayomi.tachidesk.manga.impl

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.isNull
import org.jetbrains.exposed.v1.core.leftJoin
import org.jetbrains.exposed.v1.core.statements.BatchUpdateStatement
import org.jetbrains.exposed.v1.jdbc.andWhere
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.statements.toExecutable
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import suwayomi.tachidesk.global.impl.sync.SyncYomiSyncService
import suwayomi.tachidesk.manga.impl.category.CategoryTree
import suwayomi.tachidesk.manga.impl.track.UnifiedTrackStatus
import suwayomi.tachidesk.manga.model.dataclass.CategoryDataClass
import suwayomi.tachidesk.manga.model.table.CategoryMangaTable
import suwayomi.tachidesk.manga.model.table.CategoryMetaTable
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.manga.model.table.getWithUserData
import suwayomi.tachidesk.manga.model.table.toDataClass

object Category {
    /**
     * The new category will be placed at the end of the list
     */
    fun createCategory(
        userId: Int,
        name: String,
    ): Int = createCategories(userId, listOf(name)).first()

    fun createCategories(
        userId: Int,
        names: List<String>,
    ): List<Int> =
        transaction {
            // fork: names only have to be unique among siblings, these are created at the top level
            val categoryIdToName = getCategoryList(userId).filter { it.parentId == null }.associate { it.id to it.name.lowercase() }

            val categoriesToCreate =
                names
                    .filter {
                        !it.equals(DEFAULT_CATEGORY_NAME, true)
                    }.filter { !categoryIdToName.values.contains(it.lowercase()) }

            val newCategoryIdsByName =
                CategoryTable
                    .batchInsert(categoriesToCreate) {
                        this[CategoryTable.name] = it
                        this[CategoryTable.order] = Int.MAX_VALUE
                        this[CategoryTable.user] = userId
                    }.associate { it[CategoryTable.name] to it[CategoryTable.id].value }

            normalizeCategories(userId)

            names.map {
                // creating a category named Default is illegal
                if (it.equals(DEFAULT_CATEGORY_NAME, true)) {
                    getDefaultCategoryId(userId)!!
                } else {
                    newCategoryIdsByName[it] ?: categoryIdToName.entries.find { (_, name) -> name.equals(it, true) }!!.key
                }
            }
        }

    fun updateCategory(
        userId: Int,
        categoryId: Int,
        name: String?,
        isDefault: Boolean?,
        includeInUpdate: Int?,
        includeInDownload: Int?,
    ) {
        val defaultCategoryId = getDefaultCategoryId(userId)
        if (categoryId == defaultCategoryId && includeInDownload == null && includeInUpdate == null) {
            return
        }
        transaction {
            CategoryTable.update({ CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }) {
                if (
                    categoryId != defaultCategoryId &&
                    name != null &&
                    !name.equals(DEFAULT_CATEGORY_NAME, ignoreCase = true)
                ) {
                    it[CategoryTable.name] = name
                }
                if (categoryId != defaultCategoryId && isDefault != null) it[CategoryTable.isDefault] = isDefault
                if (includeInUpdate != null) it[CategoryTable.includeInUpdate] = includeInUpdate
                if (includeInDownload != null) it[CategoryTable.includeInDownload] = includeInDownload
            }
        }
    }

    /**
     * Move the category from order number `from` to `to`
     */
    fun reorderCategory(
        userId: Int,
        from: Int,
        to: Int,
    ) {
        if (from == 0 || to == 0) return
        transaction {
            val categories =
                CategoryTable
                    .selectAll()
                    .where {
                        CategoryTable.isDefaultCategory eq false and (CategoryTable.user eq userId)
                    }.orderBy(CategoryTable.order to SortOrder.ASC)
                    .toMutableList()
            categories.add(to - 1, categories.removeAt(from - 1))
            categories.forEachIndexed { index, cat ->
                CategoryTable.update({ CategoryTable.id eq cat[CategoryTable.id].value and (CategoryTable.user eq userId) }) {
                    it[CategoryTable.order] = index + 1
                }
            }
            normalizeCategories(userId)
        }
    }

    /** Move the category to 1-based [position] among the non-default categories, ignoring raw order values. */
    fun moveCategoryToPosition(
        userId: Int,
        categoryId: Int,
        position: Int,
    ) {
        require(position > 0) { "'position' must be > 0" }
        if (categoryId == getDefaultCategoryId(userId)) return
        transaction {
            val categories =
                CategoryTable
                    .selectAll()
                    .where { CategoryTable.isDefaultCategory eq false and (CategoryTable.user eq userId) }
                    .orderBy(CategoryTable.order to SortOrder.ASC, CategoryTable.id to SortOrder.ASC)
                    .toMutableList()
            val from = categories.indexOfFirst { it[CategoryTable.id].value == categoryId }
            if (from == -1) return@transaction
            categories.add((position - 1).coerceAtMost(categories.size - 1), categories.removeAt(from))
            categories.forEachIndexed { index, cat ->
                if (cat[CategoryTable.order] != index + 1) {
                    CategoryTable.update({
                        CategoryTable.id eq cat[CategoryTable.id].value and (CategoryTable.user eq userId)
                    }) {
                        it[CategoryTable.order] = index + 1
                    }
                }
            }
            normalizeCategories(userId)
        }
    }

    fun removeCategory(
        userId: Int,
        categoryId: Int,
    ) {
        if (categoryId == getDefaultCategoryId(userId)) return
        transaction {
            val uid =
                CategoryTable
                    .selectAll()
                    .where { CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }
                    .firstOrNull()
                    ?.get(CategoryTable.uid)
            reparentChildrenOf(userId, categoryId)
            CategoryTable.deleteWhere { CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }
            normalizeCategories(userId)
            if (uid != null) {
                SyncYomiSyncService.rememberDeletedCategory(userId, uid)
            }
        }
    }

    // fork (Komikku port): nested categories and Smart Category tracker status bindings

    fun getCategoryTree(userId: Int): CategoryTree =
        transaction {
            CategoryTree(
                CategoryTable
                    .select(CategoryTable.id, CategoryTable.parentId)
                    .where { CategoryTable.user eq userId }
                    .associate { it[CategoryTable.id].value to it[CategoryTable.parentId] },
            )
        }

    /** The children of a deleted category move up to its parent, so no branch disappears with it. */
    fun reparentChildrenOf(
        userId: Int,
        categoryId: Int,
    ) {
        transaction {
            val parentId =
                CategoryTable
                    .select(CategoryTable.parentId)
                    .where { CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }
                    .firstOrNull()
                    ?.get(CategoryTable.parentId)
            CategoryTable.update({ CategoryTable.parentId eq categoryId and (CategoryTable.user eq userId) }) {
                it[CategoryTable.parentId] = parentId
            }
        }
    }

    /**
     * Validates that [categoryId] (null = a new category) named [name] can live below [parentId] (null = top level):
     * the parent belongs to the user and is not the default category, no cycle is created and no sibling has the
     * same name.
     */
    fun requireValidPlacement(
        userId: Int,
        categoryId: Int?,
        name: String,
        parentId: Int?,
    ) {
        transaction {
            val defaultCategoryId = getDefaultCategoryId(userId)
            if (parentId != null) {
                require(parentId != defaultCategoryId) { "The default category cannot have subcategories" }
                require(categoryId != defaultCategoryId) { "The default category cannot be a subcategory" }
                require(
                    CategoryTable.select(CategoryTable.id).where { CategoryTable.id eq parentId and (CategoryTable.user eq userId) }.any(),
                ) { "Parent category $parentId not found" }
                if (categoryId != null) {
                    require(getCategoryTree(userId).canMove(categoryId, parentId)) { "A category cannot be moved into its own subtree" }
                }
            }
            val siblingNames =
                CategoryTable
                    .select(CategoryTable.id, CategoryTable.name)
                    .where {
                        (CategoryTable.user eq userId) and
                            (if (parentId == null) CategoryTable.parentId.isNull() else CategoryTable.parentId eq parentId)
                    }.filter { it[CategoryTable.id].value != categoryId }
                    .map { it[CategoryTable.name] }
            require(siblingNames.none { it.equals(name, ignoreCase = true) }) { "'name' must be unique among its sibling categories" }
        }
    }

    fun setParent(
        userId: Int,
        categoryIds: List<Int>,
        parentId: Int?,
    ) {
        transaction {
            categoryIds.forEach { categoryId ->
                val name =
                    CategoryTable
                        .select(CategoryTable.name)
                        .where { CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }
                        .firstOrNull()
                        ?.get(CategoryTable.name) ?: return@forEach
                requireValidPlacement(userId, categoryId, name, parentId)
                CategoryTable.update({ CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }) {
                    it[CategoryTable.parentId] = parentId
                }
            }
        }
    }

    fun requireValidTrackStatus(trackStatus: Int?) {
        require(trackStatus == null || UnifiedTrackStatus.fromValue(trackStatus) != null) { "Unknown tracker status $trackStatus" }
    }

    /** make sure category order numbers starts from 1 and is consecutive */
    fun normalizeCategories(userId: Int) {
        transaction {
            CategoryTable
                .selectAll()
                .where { (CategoryTable.user eq userId) }
                .orderBy(CategoryTable.order to SortOrder.ASC)
                .sortedWith(compareBy({ !it[CategoryTable.isDefaultCategory] }, { it[CategoryTable.order] }))
                .forEachIndexed { index, cat ->
                    CategoryTable.update({ CategoryTable.id eq cat[CategoryTable.id].value }) {
                        it[CategoryTable.order] = index
                    }
                }
        }
    }

    private fun needsDefaultCategory(userId: Int) =
        transaction {
            MangaTable
                .getWithUserData(userId)
                .leftJoin(
                    CategoryMangaTable,
                    onColumn = { MangaTable.id },
                    otherColumn = { CategoryMangaTable.manga },
                    additionalConstraint = { CategoryMangaTable.user eq userId },
                ).selectAll()
                .where { MangaUserTable.inLibrary eq true }
                .andWhere { CategoryMangaTable.manga.isNull() }
                .empty()
                .not()
        }

    const val DEFAULT_CATEGORY_NAME = "Default"

    fun getDefaultCategory(userId: Int): CategoryDataClass? =
        transaction {
            CategoryTable
                .selectAll()
                .where { CategoryTable.user eq userId and (CategoryTable.isDefaultCategory eq true) }
                .firstOrNull()
                ?.let { CategoryTable.toDataClass(it) }
        }

    fun getDefaultCategoryId(userId: Int): Int? =
        transaction {
            CategoryTable
                .select(CategoryTable.id)
                .where { CategoryTable.user eq userId and (CategoryTable.isDefaultCategory eq true) }
                .firstOrNull()
                ?.get(CategoryTable.id)
                ?.value
        }

    fun getCategoryList(
        userId: Int,
        includeDefault: Boolean = false,
    ): List<CategoryDataClass> =
        transaction {
            CategoryTable
                .selectAll()
                .where { CategoryTable.user eq userId }
                .orderBy(CategoryTable.order to SortOrder.ASC)
                .let {
                    if (needsDefaultCategory(userId) || includeDefault) {
                        it
                    } else {
                        it.andWhere { CategoryTable.isDefaultCategory eq false }
                    }
                }.map {
                    CategoryTable.toDataClass(it)
                }
        }

    fun getCategoryById(
        userId: Int,
        categoryId: Int,
    ): CategoryDataClass? =
        transaction {
            CategoryTable.selectAll().where { CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }.firstOrNull()?.let {
                CategoryTable.toDataClass(it)
            }
        }

    fun getCategorySize(
        userId: Int,
        categoryId: Int,
    ): Int =
        transaction {
            if (categoryId == getDefaultCategoryId(userId)) {
                MangaTable
                    .getWithUserData(userId)
                    .leftJoin(
                        CategoryMangaTable,
                        onColumn = { MangaTable.id },
                        otherColumn = { CategoryMangaTable.manga },
                        additionalConstraint = { CategoryMangaTable.user eq userId },
                    ).selectAll()
                    .where { MangaUserTable.inLibrary eq true }
                    .andWhere { CategoryMangaTable.manga.isNull() }
            } else {
                CategoryMangaTable
                    .leftJoin(MangaTable.getWithUserData(userId))
                    .selectAll()
                    .where { CategoryMangaTable.category eq categoryId and (CategoryMangaTable.user eq userId) }
                    .andWhere { MangaUserTable.inLibrary eq true }
            }.count().toInt()
        }

    fun getCategoryMetaMap(
        userId: Int,
        categoryId: Int,
    ): Map<String, String> =
        transaction {
            CategoryMetaTable
                .selectAll()
                .where { CategoryMetaTable.ref eq categoryId and (CategoryMetaTable.user eq userId) }
                .associate { it[CategoryMetaTable.key] to it[CategoryMetaTable.value] }
        }

    fun getCategoriesMetaMaps(
        userId: Int,
        ids: List<Int>,
    ): Map<Int, Map<String, String>> =
        transaction {
            CategoryMetaTable
                .selectAll()
                .where { CategoryMetaTable.ref inList ids and (CategoryMetaTable.user eq userId) }
                .groupBy { it[CategoryMetaTable.ref].value }
                .mapValues { it.value.associate { it[CategoryMetaTable.key] to it[CategoryMetaTable.value] } }
                .withDefault { emptyMap() }
        }

    fun modifyMeta(
        userId: Int,
        categoryId: Int,
        key: String,
        value: String,
    ) {
        modifyCategoriesMetas(userId, mapOf(categoryId to mapOf(key to value)))
    }

    fun modifyCategoriesMetas(
        userId: Int,
        metaByCategoryId: Map<Int, Map<String, String>>,
    ) {
        transaction {
            val categoryIds = metaByCategoryId.keys
            val metaKeys = metaByCategoryId.flatMap { it.value.keys }

            val dbMetaByCategoryId =
                CategoryMetaTable
                    .selectAll()
                    .where {
                        (CategoryMetaTable.ref inList categoryIds) and (CategoryMetaTable.key inList metaKeys) and
                            (CategoryMetaTable.user eq userId)
                    }.groupBy { it[CategoryMetaTable.ref].value }

            val existingMetaByMetaId =
                categoryIds.flatMap { categoryId ->
                    val dbMetaByKey = dbMetaByCategoryId[categoryId].orEmpty().associateBy { it[CategoryMetaTable.key] }
                    val existingMetas = metaByCategoryId[categoryId].orEmpty().filter { (key) -> key in dbMetaByKey.keys }

                    existingMetas.map { entry ->
                        val metaId = dbMetaByKey[entry.key]!![CategoryMetaTable.id].value

                        metaId to entry
                    }
                }

            val newMetaByCategoryId =
                categoryIds.flatMap { categoryID ->
                    val dbMetaByKey = dbMetaByCategoryId[categoryID].orEmpty().associateBy { it[CategoryMetaTable.key] }

                    metaByCategoryId[categoryID]
                        .orEmpty()
                        .filter { entry -> entry.key !in dbMetaByKey.keys }
                        .map { entry -> categoryID to entry }
                }

            if (existingMetaByMetaId.isNotEmpty()) {
                BatchUpdateStatement(CategoryMetaTable)
                    .apply {
                        existingMetaByMetaId.forEach { (metaId, entry) ->
                            addBatch(EntityID(metaId, CategoryMetaTable))
                            this[CategoryMetaTable.value] = entry.value
                        }
                    }.toExecutable()
                    .execute(this@transaction)
            }

            if (newMetaByCategoryId.isNotEmpty()) {
                CategoryMetaTable.batchInsert(newMetaByCategoryId) { (categoryId, entry) ->
                    this[CategoryMetaTable.ref] = EntityID(categoryId, CategoryTable)
                    this[CategoryMetaTable.key] = entry.key
                    this[CategoryMetaTable.value] = entry.value
                    this[CategoryMetaTable.user] = userId
                }
            }
        }
    }
}

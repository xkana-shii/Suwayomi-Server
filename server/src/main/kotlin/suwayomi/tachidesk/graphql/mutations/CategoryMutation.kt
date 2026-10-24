@file:Suppress("RedundantNullableReturnType", "unused")

package suwayomi.tachidesk.graphql.mutations

import com.expediagroup.graphql.generator.annotations.GraphQLDescription
import com.expediagroup.graphql.generator.annotations.GraphQLIgnore
import org.jetbrains.exposed.v1.core.LikePattern
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.greaterEq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.innerJoin
import org.jetbrains.exposed.v1.core.like
import org.jetbrains.exposed.v1.core.minus
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.core.plus
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insertAndGetId
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import suwayomi.tachidesk.global.impl.sync.SyncYomiSyncService
import suwayomi.tachidesk.graphql.directives.RequireAuth
import suwayomi.tachidesk.graphql.types.CategoryMetaType
import suwayomi.tachidesk.graphql.types.CategoryType
import suwayomi.tachidesk.graphql.types.MangaType
import suwayomi.tachidesk.graphql.types.MetaInput
import suwayomi.tachidesk.manga.impl.Category
import suwayomi.tachidesk.manga.impl.CategoryManga
import suwayomi.tachidesk.manga.impl.category.SmartCategories
import suwayomi.tachidesk.manga.model.dataclass.IncludeOrExclude
import suwayomi.tachidesk.manga.model.table.CategoryMangaTable
import suwayomi.tachidesk.manga.model.table.CategoryMetaTable
import suwayomi.tachidesk.manga.model.table.CategoryTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.getWithUserData

class CategoryMutation {
    data class SetCategoryMetaInput(
        val clientMutationId: String? = null,
        val meta: CategoryMetaType,
    )

    data class SetCategoryMetaPayload(
        val clientMutationId: String?,
        val meta: CategoryMetaType,
    )

    @RequireAuth
    fun setCategoryMeta(
        @GraphQLIgnore
        userId: Int,
        input: SetCategoryMetaInput,
    ): SetCategoryMetaPayload? {
        val (clientMutationId, meta) = input

        Category.modifyMeta(userId, meta.categoryId, meta.key, meta.value)

        return SetCategoryMetaPayload(clientMutationId, meta)
    }

    data class DeleteCategoryMetaInput(
        val clientMutationId: String? = null,
        val categoryId: Int,
        val key: String,
    )

    data class DeleteCategoryMetaPayload(
        val clientMutationId: String?,
        val meta: CategoryMetaType?,
        val category: CategoryType,
    )

    @RequireAuth
    fun deleteCategoryMeta(
        @GraphQLIgnore
        userId: Int,
        input: DeleteCategoryMetaInput,
    ): DeleteCategoryMetaPayload? {
        val (clientMutationId, categoryId, key) = input

        val (meta, category) =
            transaction {
                val meta =
                    CategoryMetaTable
                        .selectAll()
                        .where {
                            CategoryMetaTable.user eq userId and
                                (CategoryMetaTable.ref eq categoryId) and
                                (CategoryMetaTable.key eq key)
                        }.firstOrNull()

                CategoryMetaTable.deleteWhere {
                    CategoryMetaTable.user eq userId and
                        (CategoryMetaTable.ref eq categoryId) and
                        (CategoryMetaTable.key eq key)
                }

                val category =
                    transaction {
                        CategoryType(CategoryTable.selectAll().where { CategoryTable.id eq categoryId }.first())
                    }

                if (meta != null) {
                    CategoryMetaType(meta)
                } else {
                    null
                } to category
            }

        return DeleteCategoryMetaPayload(clientMutationId, meta, category)
    }

    data class SetCategoryMetasItem(
        val categoryIds: List<Int>,
        val metas: List<MetaInput>,
    )

    data class SetCategoryMetasInput(
        val clientMutationId: String? = null,
        val items: List<SetCategoryMetasItem>,
    )

    data class SetCategoryMetasPayload(
        val clientMutationId: String?,
        val metas: List<CategoryMetaType>,
        val categories: List<CategoryType>,
    )

    @RequireAuth
    fun setCategoryMetas(
        @GraphQLIgnore
        userId: Int,
        input: SetCategoryMetasInput,
    ): SetCategoryMetasPayload? {
        val (clientMutationId, items) = input

        val metaByCategoryId =
            items
                .flatMap { item ->
                    val metaMap = item.metas.associate { it.key to it.value }
                    item.categoryIds.map { categoryId -> categoryId to metaMap }
                }.groupBy({ it.first }, { it.second })
                .mapValues { (_, maps) -> maps.reduce { acc, map -> acc + map } }

        Category.modifyCategoriesMetas(userId, metaByCategoryId)

        val allCategoryIds = metaByCategoryId.keys
        val allMetaKeys = metaByCategoryId.values.flatMap { item -> item.keys }.distinct()

        val (updatedMetas, categories) =
            transaction {
                val updatedMetas =
                    CategoryMetaTable
                        .selectAll()
                        .where {
                            (CategoryMetaTable.user eq userId) and (CategoryMetaTable.ref inList allCategoryIds) and
                                (CategoryMetaTable.key inList allMetaKeys)
                        }.map { CategoryMetaType(it) }

                val categories =
                    CategoryTable
                        .selectAll()
                        .where { CategoryTable.user eq userId and (CategoryTable.id inList allCategoryIds) }
                        .map { CategoryType(it) }
                        .distinctBy { it.id }

                updatedMetas to categories
            }

        return SetCategoryMetasPayload(clientMutationId, updatedMetas, categories)
    }

    data class DeleteCategoryMetasItem(
        val categoryIds: List<Int>,
        val keys: List<String>? = null,
        val prefixes: List<String>? = null,
    )

    data class DeleteCategoryMetasInput(
        val clientMutationId: String? = null,
        val items: List<DeleteCategoryMetasItem>,
    )

    data class DeleteCategoryMetasPayload(
        val clientMutationId: String?,
        val metas: List<CategoryMetaType>,
        val categories: List<CategoryType>,
    )

    @RequireAuth
    fun deleteCategoryMetas(
        @GraphQLIgnore
        userId: Int,
        input: DeleteCategoryMetasInput,
    ): DeleteCategoryMetasPayload? {
        val (clientMutationId, items) = input

        items.forEach { item ->
            require(!item.keys.isNullOrEmpty() || !item.prefixes.isNullOrEmpty()) {
                "Either 'keys' or 'prefixes' must be provided for each item"
            }
        }

        val (allDeletedMetas, allCategoryIds) =
            transaction {
                val deletedMetas = mutableListOf<CategoryMetaType>()
                val categoryIds = mutableSetOf<Int>()

                items.forEach { item ->
                    val keyCondition: Op<Boolean>? =
                        item.keys?.takeIf { it.isNotEmpty() }?.let { CategoryMetaTable.key inList it }

                    val prefixCondition: Op<Boolean>? =
                        item.prefixes
                            ?.filter { it.isNotEmpty() }
                            ?.map { (CategoryMetaTable.key like LikePattern("$it%")) as Op<Boolean> }
                            ?.reduceOrNull { acc, op -> acc or op }

                    val metaKeyCondition =
                        if (keyCondition != null && prefixCondition != null) {
                            keyCondition or prefixCondition
                        } else {
                            keyCondition ?: prefixCondition!!
                        }

                    val condition =
                        (CategoryMetaTable.user eq userId) and (CategoryMetaTable.ref inList item.categoryIds) and metaKeyCondition

                    deletedMetas +=
                        CategoryMetaTable
                            .selectAll()
                            .where { condition }
                            .map { CategoryMetaType(it) }

                    CategoryMetaTable.deleteWhere { condition }
                    categoryIds += item.categoryIds
                }

                deletedMetas to categoryIds
            }

        val categories =
            transaction {
                CategoryTable
                    .selectAll()
                    .where { CategoryTable.id inList allCategoryIds }
                    .map { CategoryType(it) }
                    .distinctBy { it.id }
            }

        return DeleteCategoryMetasPayload(clientMutationId, allDeletedMetas, categories)
    }

    data class UpdateCategoryPatch(
        val name: String? = null,
        val default: Boolean? = null,
        val includeInUpdate: IncludeOrExclude? = null,
        val includeInDownload: IncludeOrExclude? = null,
        @GraphQLDescription("fork: move below this category")
        val parentId: Int? = null,
        @GraphQLDescription("fork: move to the top level")
        val moveToTopLevel: Boolean? = null,
        @GraphQLDescription("fork: Smart Category binding (unified tracker status)")
        val trackStatus: Int? = null,
        @GraphQLDescription("fork: remove the Smart Category binding")
        val clearTrackStatus: Boolean? = null,
        @GraphQLDescription("fork: hide the category from the library tabs (its manga and settings are kept)")
        val hidden: Boolean? = null,
        @GraphQLDescription("fork: automatic library update interval in days (1-365)")
        val updateInterval: Int? = null,
        @GraphQLDescription("fork: follow the parent's interval or the global schedule again")
        val clearUpdateInterval: Boolean? = null,
    )

    data class UpdateCategoryPayload(
        val clientMutationId: String?,
        val category: CategoryType,
    )

    data class UpdateCategoryInput(
        val clientMutationId: String? = null,
        val id: Int,
        val patch: UpdateCategoryPatch,
    )

    data class UpdateCategoriesPayload(
        val clientMutationId: String?,
        val categories: List<CategoryType>,
    )

    data class UpdateCategoriesInput(
        val clientMutationId: String? = null,
        val ids: List<Int>,
        val patch: UpdateCategoryPatch,
    )

    private fun updateCategories(
        @GraphQLIgnore
        userId: Int,
        ids: List<Int>,
        patch: UpdateCategoryPatch,
    ) {
        Category.requireValidTrackStatus(patch.trackStatus)
        if (patch.name != null) {
            // fork: names are unique among siblings
            transaction {
                CategoryTable
                    .select(CategoryTable.id, CategoryTable.parentId)
                    .where { CategoryTable.id inList ids and (CategoryTable.user eq userId) and (CategoryTable.isDefaultCategory eq false) }
                    .forEach { Category.requireValidPlacement(userId, it[CategoryTable.id].value, patch.name, it[CategoryTable.parentId]) }
            }
        }
        if (patch.parentId != null || patch.moveToTopLevel == true) {
            Category.setParent(userId, ids, if (patch.moveToTopLevel == true) null else patch.parentId)
        }
        val trackStatusChanged = patch.trackStatus != null || patch.clearTrackStatus == true
        patch.updateInterval?.let { require(it in 1..365) { "'updateInterval' must be between 1 and 365 days" } }
        transaction {
            if (patch.hidden != null) {
                CategoryTable.update({
                    CategoryTable.id inList ids and (CategoryTable.user eq userId) and (CategoryTable.isDefaultCategory eq false)
                }) { update -> update[hidden] = patch.hidden }
            }
            if (patch.updateInterval != null || patch.clearUpdateInterval == true) {
                CategoryTable.update({
                    CategoryTable.id inList ids and (CategoryTable.user eq userId) and (CategoryTable.isDefaultCategory eq false)
                }) { update -> update[updateInterval] = if (patch.clearUpdateInterval == true) null else patch.updateInterval }
            }
            if (trackStatusChanged) {
                CategoryTable.update({
                    CategoryTable.id inList ids and (CategoryTable.user eq userId) and (CategoryTable.isDefaultCategory eq false)
                }) { update ->
                    update[trackStatus] = if (patch.clearTrackStatus == true) null else patch.trackStatus
                }
            }
            // The default category row is protected from rename and landing-flag changes;
            if (patch.name != null) {
                CategoryTable.update({
                    CategoryTable.id inList ids and (CategoryTable.user eq userId) and (CategoryTable.isDefaultCategory eq false)
                }) { update ->
                    patch.name.also {
                        update[name] = it
                    }
                }
            }
            if (patch.default != null) {
                CategoryTable.update({
                    CategoryTable.id inList ids and (CategoryTable.user eq userId) and (CategoryTable.isDefaultCategory eq false)
                }) { update ->
                    patch.default.also {
                        update[isDefault] = it
                    }
                }
            }
            if (patch.includeInUpdate != null) {
                CategoryTable.update({ CategoryTable.id inList ids and (CategoryTable.user eq userId) }) { update ->
                    patch.includeInUpdate.also {
                        update[includeInUpdate] = it.value
                    }
                }
            }
            if (patch.includeInDownload != null) {
                CategoryTable.update({ CategoryTable.id inList ids and (CategoryTable.user eq userId) }) { update ->
                    patch.includeInDownload.also {
                        update[includeInDownload] = it.value
                    }
                }
            }
        }
        if (trackStatusChanged) SmartCategories.syncUser(userId)
    }

    @RequireAuth
    fun updateCategory(
        @GraphQLIgnore
        userId: Int,
        input: UpdateCategoryInput,
    ): UpdateCategoryPayload? {
        val (clientMutationId, id, patch) = input

        updateCategories(userId, listOf(id), patch)

        val category =
            transaction {
                CategoryType(
                    CategoryTable
                        .selectAll()
                        .where { CategoryTable.id eq id and (CategoryTable.user eq userId) }
                        .first(),
                )
            }

        return UpdateCategoryPayload(
            clientMutationId = clientMutationId,
            category = category,
        )
    }

    @RequireAuth
    fun updateCategories(
        @GraphQLIgnore
        userId: Int,
        input: UpdateCategoriesInput,
    ): UpdateCategoriesPayload? {
        val (clientMutationId, ids, patch) = input

        updateCategories(userId, ids, patch)

        val categories =
            transaction {
                CategoryTable
                    .selectAll()
                    .where { CategoryTable.id inList ids and (CategoryTable.user eq userId) }
                    .map { CategoryType(it) }
            }

        return UpdateCategoriesPayload(
            clientMutationId = clientMutationId,
            categories = categories,
        )
    }

    data class UpdateCategoryOrderPayload(
        val clientMutationId: String?,
        val categories: List<CategoryType>,
    )

    data class UpdateCategoryOrderInput(
        val clientMutationId: String? = null,
        val id: Int,
        val position: Int,
    )

    @RequireAuth
    fun updateCategoryOrder(
        @GraphQLIgnore
        userId: Int,
        input: UpdateCategoryOrderInput,
    ): UpdateCategoryOrderPayload? {
        val (clientMutationId, categoryId, position) = input
        require(position > 0) {
            "'order' must not be <= 0"
        }

        // position-based: stored sort_order values can collide (pre-existing adopted 0-based rows; sync skips newer local copies)
        Category.moveCategoryToPosition(userId, categoryId, position)

        val categories =
            transaction {
                CategoryTable
                    .selectAll()
                    .where { CategoryTable.user eq userId }
                    .orderBy(CategoryTable.order)
                    .map { CategoryType(it) }
            }

        return UpdateCategoryOrderPayload(
            clientMutationId = clientMutationId,
            categories = categories,
        )
    }

    data class CreateCategoryInput(
        val clientMutationId: String? = null,
        val name: String,
        val order: Int? = null,
        val default: Boolean? = null,
        val includeInUpdate: IncludeOrExclude? = null,
        val includeInDownload: IncludeOrExclude? = null,
        @GraphQLDescription("fork: create as a subcategory of this category")
        val parentId: Int? = null,
        @GraphQLDescription("fork: Smart Category binding (unified tracker status)")
        val trackStatus: Int? = null,
    )

    data class CreateCategoryPayload(
        val clientMutationId: String?,
        val category: CategoryType,
    )

    @RequireAuth
    fun createCategory(
        @GraphQLIgnore
        userId: Int,
        input: CreateCategoryInput,
    ): CreateCategoryPayload? {
        val (clientMutationId, name, order, default, includeInUpdate, includeInDownload) = input
        // fork: names only have to be unique among siblings (nested categories)
        Category.requireValidPlacement(userId, null, input.name, input.parentId)
        Category.requireValidTrackStatus(input.trackStatus)
        require(!name.equals(Category.DEFAULT_CATEGORY_NAME, ignoreCase = true)) {
            "'name' must not be ${Category.DEFAULT_CATEGORY_NAME}"
        }
        if (order != null) {
            require(order > 0) {
                "'order' must not be <= 0"
            }
        }

        val category =
            transaction {
                if (order != null) {
                    CategoryTable.update({ CategoryTable.order greaterEq order and (CategoryTable.user eq userId) }) {
                        it[CategoryTable.order] = CategoryTable.order + 1
                    }
                }

                val id =
                    CategoryTable.insertAndGetId {
                        it[CategoryTable.user] = userId
                        it[CategoryTable.name] = input.name
                        it[CategoryTable.order] = order ?: Int.MAX_VALUE
                        if (default != null) {
                            it[CategoryTable.isDefault] = default
                        }
                        if (includeInUpdate != null) {
                            it[CategoryTable.includeInUpdate] = includeInUpdate.value
                        }
                        if (includeInDownload != null) {
                            it[CategoryTable.includeInDownload] = includeInDownload.value
                        }
                        it[CategoryTable.parentId] = input.parentId
                        it[CategoryTable.trackStatus] = input.trackStatus
                    }

                Category.normalizeCategories(userId)

                CategoryType(CategoryTable.selectAll().where { CategoryTable.id eq id and (CategoryTable.user eq userId) }.first())
            }
        if (input.trackStatus != null) SmartCategories.syncUser(userId)

        return CreateCategoryPayload(clientMutationId, category)
    }

    data class DeleteCategoryInput(
        val clientMutationId: String? = null,
        val categoryId: Int,
    )

    data class DeleteCategoryPayload(
        val clientMutationId: String?,
        val category: CategoryType?,
        val mangas: List<MangaType>,
    )

    @RequireAuth
    fun deleteCategory(
        @GraphQLIgnore
        userId: Int,
        input: DeleteCategoryInput,
    ): DeleteCategoryPayload? {
        val (clientMutationId, categoryId) = input

        val defaultCategoryId = Category.getDefaultCategoryId(userId)
        if (categoryId == defaultCategoryId) { // Don't delete default category
            return DeleteCategoryPayload(
                clientMutationId,
                null,
                emptyList(),
            )
        }

        val (category, mangas) =
            transaction {
                val category =
                    CategoryTable
                        .selectAll()
                        .where { CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }
                        .firstOrNull()

                val mangas =
                    transaction {
                        MangaTable
                            .getWithUserData(userId)
                            .innerJoin(
                                CategoryMangaTable,
                                onColumn = { MangaTable.id },
                                otherColumn = { CategoryMangaTable.manga },
                                additionalConstraint = { CategoryMangaTable.user eq userId },
                            ).selectAll()
                            .where { CategoryMangaTable.category eq categoryId }
                            .map { MangaType(it) }
                    }

                Category.reparentChildrenOf(userId, categoryId)
                CategoryTable.deleteWhere { CategoryTable.id eq categoryId and (CategoryTable.user eq userId) }

                Category.normalizeCategories(userId)
                category?.let { SyncYomiSyncService.rememberDeletedCategory(userId, it[CategoryTable.uid]) }

                if (category != null) {
                    CategoryType(category)
                } else {
                    null
                } to mangas
            }

        return DeleteCategoryPayload(clientMutationId, category, mangas)
    }

    data class UpdateMangaCategoriesPatch(
        val clearCategories: Boolean? = null,
        val addToCategories: List<Int>? = null,
        val removeFromCategories: List<Int>? = null,
    )

    data class UpdateMangaCategoriesPayload(
        val clientMutationId: String?,
        val manga: MangaType,
    )

    data class UpdateMangaCategoriesInput(
        val clientMutationId: String? = null,
        val id: Int,
        val patch: UpdateMangaCategoriesPatch,
    )

    data class UpdateMangasCategoriesPayload(
        val clientMutationId: String?,
        val mangas: List<MangaType>,
    )

    data class UpdateMangasCategoriesInput(
        val clientMutationId: String? = null,
        val ids: List<Int>,
        val patch: UpdateMangaCategoriesPatch,
    )

    private fun updateMangas(
        userId: Int,
        ids: List<Int>,
        patch: UpdateMangaCategoriesPatch,
    ) {
        val removeFromCategoryIds = patch.removeFromCategories.orEmpty()
        val addToCategoryIds = patch.addToCategories.orEmpty()
        transaction {
            if (patch.clearCategories == true) {
                CategoryMangaTable.deleteWhere { CategoryMangaTable.manga inList ids and (CategoryMangaTable.user eq userId) }
            } else if (removeFromCategoryIds.isNotEmpty()) {
                CategoryMangaTable.deleteWhere {
                    (CategoryMangaTable.manga inList ids) and
                        (CategoryMangaTable.category inList removeFromCategoryIds) and
                        (CategoryMangaTable.user eq userId)
                }
            }
            if (addToCategoryIds.isNotEmpty()) {
                CategoryManga.addMangasToCategories(userId, ids, addToCategoryIds)
            }
        }
    }

    @RequireAuth
    fun updateMangaCategories(
        @GraphQLIgnore
        userId: Int,
        input: UpdateMangaCategoriesInput,
    ): UpdateMangaCategoriesPayload? {
        val (clientMutationId, id, patch) = input

        updateMangas(userId, listOf(id), patch)

        val manga =
            transaction {
                MangaType(
                    MangaTable
                        .getWithUserData(userId)
                        .selectAll()
                        .where { MangaTable.id eq id }
                        .first(),
                )
            }

        return UpdateMangaCategoriesPayload(
            clientMutationId = clientMutationId,
            manga = manga,
        )
    }

    @RequireAuth
    fun updateMangasCategories(
        @GraphQLIgnore
        userId: Int,
        input: UpdateMangasCategoriesInput,
    ): UpdateMangasCategoriesPayload? {
        val (clientMutationId, ids, patch) = input

        updateMangas(userId, ids, patch)

        val mangas =
            transaction {
                MangaTable
                    .getWithUserData(userId)
                    .selectAll()
                    .where { MangaTable.id inList ids }
                    .map { MangaType(it) }
            }

        return UpdateMangasCategoriesPayload(
            clientMutationId = clientMutationId,
            mangas = mangas,
        )
    }
}

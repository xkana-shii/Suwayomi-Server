package suwayomi.tachidesk.manga.impl.category

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import suwayomi.tachidesk.manga.model.dataclass.CategoryDataClass
import suwayomi.tachidesk.manga.model.dataclass.IncludeOrExclude
import kotlin.time.Duration.Companion.days

/**
 * fork (Komikku port of CategoryUpdateSchedule): library updates with nested categories.
 *
 * - A category refresh covers the category's whole subtree.
 * - Update inclusion and update intervals are inherited: a category without its own value uses the nearest ancestor's.
 * - In automatic updates a manga is due when it has no category, when one of its categories has no interval (global
 *   schedule), or when one of its interval categories is due; each manga is updated once however many categories
 *   include it.
 */
class CategoryUpdateSchedule(
    categories: List<CategoryDataClass>,
) {
    private val byId = categories.associateBy { it.id }
    private val tree = CategoryTree(categories.associate { it.id to it.parentId })

    /** the category and its ancestors, nearest first */
    private fun chain(categoryId: Int): List<CategoryDataClass> = (listOf(categoryId) + tree.ancestors(categoryId)).mapNotNull(byId::get)

    fun effectiveIncludeInUpdate(categoryId: Int): IncludeOrExclude =
        chain(categoryId).firstOrNull { it.includeInUpdate != IncludeOrExclude.UNSET }?.includeInUpdate ?: IncludeOrExclude.UNSET

    /** days, null = global schedule */
    fun effectiveInterval(categoryId: Int): Int? = chain(categoryId).firstNotNullOfOrNull { it.updateInterval }

    /** the given categories and all their descendants */
    fun withSubtrees(categoryIds: Collection<Int>): Set<Int> = categoryIds.flatMapTo(LinkedHashSet()) { listOf(it) + tree.descendants(it) }

    /**
     * Interval categories whose interval has passed since their last scheduled update. The interval is the category's
     * effective one, the last run is the category's own.
     */
    fun dueIntervalCategoryIds(nowEpochSeconds: Long): Set<Int> =
        byId.values
            .filter { category ->
                val interval = effectiveInterval(category.id) ?: return@filter false
                category.lastScheduledUpdateAt == 0L ||
                    nowEpochSeconds - category.lastScheduledUpdateAt >= interval.days.inWholeSeconds
            }.mapTo(HashSet()) { it.id }

    fun isMangaDue(
        mangaCategoryIds: Collection<Int>,
        dueIntervalCategoryIds: Set<Int>,
    ): Boolean {
        val known = mangaCategoryIds.filter { it in byId }
        if (known.isEmpty()) return true
        return known.any { effectiveInterval(it) == null || it in dueIntervalCategoryIds }
    }
}

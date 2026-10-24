package suwayomi.tachidesk.manga.impl.category

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

/**
 * fork: nested categories (Komikku port of domain/.../category/service/CategoryTree.kt).
 *
 * The hierarchy is a forest: a category without a parent (or whose parent no longer exists) is top level. Identity is
 * always the id, names only need to be unique among siblings. Every walk is cycle safe, so a corrupted hierarchy
 * (e.g. from a foreign backup) can never hang the server.
 */
class CategoryTree(
    /** category id to parent id (null = top level) */
    parents: Map<Int, Int?>,
) {
    private val parentOf: Map<Int, Int?> = parents.mapValues { (_, parent) -> parent?.takeIf { it in parents } }

    private val childrenOf: Map<Int?, List<Int>> = parentOf.entries.groupBy({ it.value }, { it.key })

    val ids: Set<Int> get() = parentOf.keys

    fun parentOf(id: Int): Int? = parentOf[id]

    fun children(id: Int?): List<Int> = childrenOf[id].orEmpty()

    /** ancestors of [id], nearest first */
    fun ancestors(id: Int): List<Int> {
        val result = mutableListOf<Int>()
        val seen = mutableSetOf(id)
        var current = parentOf[id]
        while (current != null && seen.add(current)) {
            result += current
            current = parentOf[current]
        }
        return result
    }

    fun descendants(id: Int): Set<Int> {
        val result = linkedSetOf<Int>()
        val queue = ArrayDeque(children(id))
        while (queue.isNotEmpty()) {
            val next = queue.removeFirst()
            if (next != id && result.add(next)) queue += children(next)
        }
        return result
    }

    /** [ids] and all their ancestors: the categories a manga filed in [ids] "reaches" */
    fun withAncestors(ids: Collection<Int>): Set<Int> =
        ids.filter { it in parentOf }.flatMapTo(LinkedHashSet()) {
            listOf(it) +
                ancestors(it)
        }

    fun depth(id: Int): Int = ancestors(id).size

    /** the top level category of [id]'s branch */
    fun root(id: Int): Int = ancestors(id).lastOrNull() ?: id

    /** Moving [id] below [newParent] (null = top level) keeps the hierarchy acyclic. */
    fun canMove(
        id: Int,
        newParent: Int?,
    ): Boolean = newParent == null || (newParent in parentOf && newParent != id && newParent !in descendants(id))

    /** Depth-first order, siblings by [order]; categories of a cycle are appended so nothing is lost. */
    fun flattened(order: (Int) -> Int): List<Int> {
        val result = linkedSetOf<Int>()

        fun visit(id: Int) {
            if (!result.add(id)) return
            children(id).sortedBy(order).forEach(::visit)
        }
        children(null).sortedBy(order).forEach(::visit)
        parentOf.keys.sortedBy(order).forEach(::visit)
        return result.toList()
    }
}

package suwayomi.tachidesk.manga.impl.category

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import suwayomi.tachidesk.manga.model.dataclass.CategoryDataClass
import suwayomi.tachidesk.manga.model.dataclass.IncludeOrExclude
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CategoryUpdateScheduleTest {
    private val day = 24 * 60 * 60L

    private fun category(
        id: Int,
        parentId: Int? = null,
        include: IncludeOrExclude = IncludeOrExclude.UNSET,
        interval: Int? = null,
        lastRun: Long = 0,
    ) = CategoryDataClass(
        id = id,
        order = id,
        name = "c$id",
        default = false,
        includeInUpdate = include,
        includeInDownload = IncludeOrExclude.UNSET,
        isDefaultCategory = false,
        version = 0,
        uid = 0,
        lastModifiedAt = 0,
        parentId = parentId,
        updateInterval = interval,
        lastScheduledUpdateAt = lastRun,
    )

    // Shounen(1, every 7 days, last run 3 days ago) / Weekly(2) / Daily(3, every day); Josei(4, excluded) / Old(5)
    private val now = 100 * day
    private val schedule =
        CategoryUpdateSchedule(
            listOf(
                category(1, interval = 7, lastRun = now - 3 * day),
                category(2, parentId = 1, lastRun = now - 8 * day),
                category(3, parentId = 1, interval = 1, lastRun = now - 2 * day),
                category(4, include = IncludeOrExclude.EXCLUDE),
                category(5, parentId = 4),
                category(6),
            ),
        )

    @Test
    fun `inclusion and intervals are inherited from the nearest ancestor`() {
        assertEquals(IncludeOrExclude.EXCLUDE, schedule.effectiveIncludeInUpdate(5))
        assertEquals(IncludeOrExclude.UNSET, schedule.effectiveIncludeInUpdate(2))
        assertEquals(7, schedule.effectiveInterval(2))
        assertEquals(1, schedule.effectiveInterval(3))
        assertEquals(null, schedule.effectiveInterval(6))
    }

    @Test
    fun `a category refresh covers the subtree`() {
        assertEquals(setOf(1, 2, 3), schedule.withSubtrees(listOf(1)))
        assertEquals(setOf(4, 5, 3), schedule.withSubtrees(listOf(4, 3)))
    }

    @Test
    fun `due categories use the inherited interval and their own last run`() {
        // 2 inherits 7 days and last ran 8 days ago, 1 ran 3 days ago, 3 ran 2 days ago with a 1 day interval
        assertEquals(setOf(2, 3), schedule.dueIntervalCategoryIds(now))
    }

    @Test
    fun `a manga is due through any of its categories, a global category always wins`() {
        val due = schedule.dueIntervalCategoryIds(now)
        assertTrue(schedule.isMangaDue(emptyList(), due), "no category: global schedule")
        assertTrue(schedule.isMangaDue(listOf(6), due), "category without interval: global schedule")
        assertFalse(schedule.isMangaDue(listOf(1), due), "Shounen is not due yet")
        assertTrue(schedule.isMangaDue(listOf(1, 3), due), "due through Daily")
        assertTrue(schedule.isMangaDue(listOf(1, 6), due))
    }
}

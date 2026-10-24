package suwayomi.tachidesk.manga.impl.chapter

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** fork: rules ported from the Komikku fork's ChapterSmartMergeTest / ChapterBlacklistTest */
class ChapterVisibilityTest {
    private var nextId = 1

    private fun chapter(
        number: Double,
        scanlator: String?,
        name: String = "Chapter $number",
        url: String = "/c/$nextId",
        sourceOrder: Int = nextId,
    ) = VisibilityChapter(nextId++, url, name, number, scanlator, sourceOrder)

    private fun merged(
        chapters: List<VisibilityChapter>,
        priority: List<String> = emptyList(),
        rules: List<ScanlatorRangeRule> = emptyList(),
    ) = SmartScanlatorMerge.visibleIds(chapters, priority, rules)

    @Test
    fun `merge keeps the preferred scanlator per number`() {
        val a1 = chapter(1.0, "A")
        val b1 = chapter(1.0, "B")
        val b2 = chapter(2.0, "B")
        assertEquals(setOf(b1.id, b2.id), merged(listOf(a1, b1, b2), priority = listOf("B", "A")))
        assertEquals(setOf(a1.id, b2.id), merged(listOf(a1, b1, b2), priority = listOf("A", "B")))
    }

    @Test
    fun `the last covering range rule wins over the priority`() {
        val a5 = chapter(5.0, "A")
        val b5 = chapter(5.0, "B")
        val c5 = chapter(5.0, "C")
        val rules = listOf(ScanlatorRangeRule(1.0, 10.0, "B"), ScanlatorRangeRule(4.0, 6.0, "C"))
        assertEquals(setOf(c5.id), merged(listOf(a5, b5, c5), priority = listOf("A"), rules = rules))
    }

    @Test
    fun `equally preferred scanlators are decided by source order`() {
        // Suwayomi: a higher source order is earlier in the source list
        val late = chapter(3.0, "X", sourceOrder = 1)
        val early = chapter(3.0, "Y", sourceOrder = 9)
        assertEquals(setOf(early.id), merged(listOf(late, early)))
    }

    @Test
    fun `all uploads of the chosen scanlator stay, unnumbered chapters and notices are never merged`() {
        val a1 = chapter(1.0, "A")
        val a1Again = chapter(1.0, "A", name = "Chapter 1 (HQ)")
        val b1 = chapter(1.0, "B")
        val extra = chapter(-1.0, "B", name = "Extra")
        val notice = chapter(1.0, "B", name = "Hiatus announcement")
        val visible = merged(listOf(a1, a1Again, b1, extra, notice), priority = listOf("A", "B"))
        assertEquals(setOf(a1.id, a1Again.id, extra.id, notice.id), visible)
        assertTrue(SmartScanlatorMerge.isNotice("Notice: delayed"))
        assertTrue(!SmartScanlatorMerge.isNotice("Chapter 12 - The break"))
    }

    @Test
    fun `range rules round trip and reject invalid ranges`() {
        val rule = ScanlatorRangeRule(1.5, 20.0, "Group: with colon")
        assertEquals(rule, ScanlatorRangeRule.parse(rule.encode()))
        assertEquals(null, ScanlatorRangeRule.parse("5.0:1.0:A"))
        assertEquals(null, ScanlatorRangeRule.parse("x:1.0:A"))
    }

    @Test
    fun `url keys match exactly and hybrid keys survive a url change`() {
        val c1 = chapter(1.0, "A", url = "/a,1")
        val c2 = chapter(2.0, "A", url = "/a,2")
        assertEquals(setOf(c1.id), ChapterBlacklist.blacklistedIds(listOf(c1, c2), listOf(ChapterBlacklist.urlKey("/a,1"))))

        val key = ChapterBlacklist.hybridKey(c2)
        val moved = c2.copy(id = 99, url = "/moved/2")
        assertEquals(setOf(99), ChapterBlacklist.blacklistedIds(listOf(c1, moved), listOf(key)))
    }

    @Test
    fun `ambiguous keys never hide anything`() {
        val first = chapter(3.0, "A", name = "Chapter 3")
        val second = chapter(3.0, "A", name = "Chapter 3")
        val legacy = ChapterBlacklist.legacyKey(3.0, "A")
        assertEquals(emptySet(), ChapterBlacklist.blacklistedIds(listOf(first, second), listOf(legacy)))

        // a hybrid key whose url is gone, with two candidates of the same number, scanlator and name
        val key = ChapterBlacklist.hybridKey(first.copy(url = "/gone"))
        assertEquals(emptySet(), ChapterBlacklist.blacklistedIds(listOf(first, second), listOf(key)))
    }

    @Test
    fun `hybrid keys use the name to tell a notice from the chapter with the same number`() {
        val chapter = chapter(7.0, "A", name = "Chapter 7")
        val notice = chapter(7.0, "A", name = "Notice")
        val key = ChapterBlacklist.hybridKey(notice.copy(url = "/old-notice-url"))
        assertEquals(setOf(notice.id), ChapterBlacklist.blacklistedIds(listOf(chapter, notice), listOf(key)))
    }

    @Test
    fun `merge never shows a blacklisted chapter and never reports it as merge hidden`() {
        val a1 = chapter(1.0, "A")
        val b1 = chapter(1.0, "B")
        val result =
            ChapterVisibility.compute(
                listOf(a1, b1),
                ScanlatorSettings(blacklistedChapters = listOf(ChapterBlacklist.hybridKey(a1)), scanlatorPriority = listOf("A", "B")),
                smartMergeEnabled = true,
            )
        assertEquals(setOf(a1.id), result.blacklistedIds)
        // A is blacklisted, so B becomes the visible release of chapter 1
        assertEquals(emptySet(), result.mergeHiddenIds)
    }

    @Test
    fun `new scanlators never disturb the saved priority`() {
        assertEquals(listOf("B", "A", "c", "D"), ChapterVisibility.effectivePriority(listOf("Gone", "B", "A"), listOf("A", "D", "B", "c")))
    }

    @Test
    fun `editing the visible scanlators keeps the position of the hidden ones`() {
        val stored = listOf("A", "Gone", "B", "C")
        assertEquals(listOf("C", "Gone", "A", "B"), ChapterVisibility.mergePriority(stored, listOf("C", "A", "B")))
        assertEquals(listOf("B", "Gone", "New", "A"), ChapterVisibility.mergePriority(listOf("A", "Gone", "B"), listOf("B", "New", "A")))
    }
}

package suwayomi.tachidesk.manga.impl.track

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.assertThrows
import suwayomi.tachidesk.manga.impl.track.tracker.TrackerManager
import suwayomi.tachidesk.test.ApplicationTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TrackerEditsAndMetadataTest : ApplicationTest() {
    @Test
    fun `scores are converted to the closest entry of each scale`() {
        val pointTen = (0..10).map { it.toString() }
        val pointHundred = (0..100).map { it.toString() }
        val pointFive = listOf("0", "1", "2", "3", "4", "5")
        val smileys = listOf("-", "sad", "neutral", "happy")
        assertEquals("7", UnifiedTrackerEdits.nearestScore(pointTen, 7.0))
        assertEquals("75", UnifiedTrackerEdits.nearestScore(pointHundred, 7.5))
        assertEquals("4", UnifiedTrackerEdits.nearestScore(pointFive, 8.0))
        assertEquals("neutral", UnifiedTrackerEdits.nearestScore(smileys, 6.0))
        assertEquals(null, UnifiedTrackerEdits.nearestScore(emptyList(), 6.0))
    }

    @Test
    fun `invalid edits are rejected before contacting any tracker`() =
        runTest {
            assertThrows<IllegalArgumentException> { UnifiedTrackerEdits.apply(1, 1, UnifiedTrackerEdits.Edit()) }
            assertThrows<IllegalArgumentException> { UnifiedTrackerEdits.apply(1, 1, UnifiedTrackerEdits.Edit(score = 11.0)) }
            assertThrows<IllegalArgumentException> {
                UnifiedTrackerEdits.apply(1, 1, UnifiedTrackerEdits.Edit(startDate = 2000, finishDate = 1000))
            }
        }

    @Test
    fun `only metadata trackers autofill and untracked manga are reported as such`() =
        runTest {
            assertTrue(TrackerMetadata.supportsMetadata(TrackerManager.MANGABAKA))
            assertTrue(TrackerMetadata.supportsMetadata(TrackerManager.ANILIST))
            assertFalse(TrackerMetadata.supportsMetadata(TrackerManager.KITSU))
            assertThrows<TrackerMetadata.NotTrackedException> { TrackerMetadata.getMetadata(1, -1, TrackerManager.MYANIMELIST) }
            assertEquals(listOf("Action", "Drama"), TrackerMetadata.autofillTags(listOf(" Action", "action ", "", "Drama")))
            assertEquals(listOf("Drama", "Action"), TrackerMetadata.mergeTags(listOf("Drama"), listOf("drama", "Action")))
        }
}

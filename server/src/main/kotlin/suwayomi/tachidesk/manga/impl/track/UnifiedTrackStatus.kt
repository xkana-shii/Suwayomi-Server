package suwayomi.tachidesk.manga.impl.track

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import suwayomi.tachidesk.manga.impl.track.tracker.TrackerManager
import suwayomi.tachidesk.manga.impl.track.tracker.anilist.Anilist
import suwayomi.tachidesk.manga.impl.track.tracker.bangumi.Bangumi
import suwayomi.tachidesk.manga.impl.track.tracker.kitsu.Kitsu
import suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.MangaBaka
import suwayomi.tachidesk.manga.impl.track.tracker.mangaupdates.MangaUpdates
import suwayomi.tachidesk.manga.impl.track.tracker.myanimelist.MyAnimeList
import suwayomi.tachidesk.manga.impl.track.tracker.shikimori.Shikimori

/**
 * fork: a tracker independent reading status (Komikku's `TrackStatus`, same values so backups of Smart Category
 * bindings are exchangeable). Used by Smart Categories and unified tracker edits.
 */
enum class UnifiedTrackStatus(
    val value: Int,
) {
    READING(1),
    REPEATING(2),
    PLAN_TO_READ(3),
    PAUSED(4),
    COMPLETED(5),
    DROPPED(6),

    /** Komikku's "not tracked" group; never produced by a tracker */
    OTHER(7),
    ;

    companion object {
        fun fromValue(value: Int?): UnifiedTrackStatus? = entries.firstOrNull { it.value == value }

        /** tracker status → unified status, in the tracker's own preference order (first match wins for the reverse) */
        private val mappings: Map<Int, List<Pair<Int, UnifiedTrackStatus>>> =
            mapOf(
                TrackerManager.MYANIMELIST to
                    listOf(
                        MyAnimeList.READING to READING,
                        MyAnimeList.COMPLETED to COMPLETED,
                        MyAnimeList.ON_HOLD to PAUSED,
                        MyAnimeList.PLAN_TO_READ to PLAN_TO_READ,
                        MyAnimeList.DROPPED to DROPPED,
                        MyAnimeList.REREADING to REPEATING,
                    ),
                TrackerManager.ANILIST to
                    listOf(
                        Anilist.READING to READING,
                        Anilist.COMPLETED to COMPLETED,
                        Anilist.ON_HOLD to PAUSED,
                        Anilist.PLAN_TO_READ to PLAN_TO_READ,
                        Anilist.DROPPED to DROPPED,
                        Anilist.REREADING to REPEATING,
                    ),
                TrackerManager.KITSU to
                    listOf(
                        Kitsu.READING to READING,
                        Kitsu.COMPLETED to COMPLETED,
                        Kitsu.ON_HOLD to PAUSED,
                        Kitsu.PLAN_TO_READ to PLAN_TO_READ,
                        Kitsu.DROPPED to DROPPED,
                    ),
                TrackerManager.SHIKIMORI to
                    listOf(
                        Shikimori.READING to READING,
                        Shikimori.COMPLETED to COMPLETED,
                        Shikimori.ON_HOLD to PAUSED,
                        Shikimori.PLAN_TO_READ to PLAN_TO_READ,
                        Shikimori.DROPPED to DROPPED,
                        Shikimori.REREADING to REPEATING,
                    ),
                TrackerManager.BANGUMI to
                    listOf(
                        Bangumi.READING to READING,
                        Bangumi.COMPLETED to COMPLETED,
                        Bangumi.ON_HOLD to PAUSED,
                        Bangumi.PLAN_TO_READ to PLAN_TO_READ,
                        Bangumi.DROPPED to DROPPED,
                    ),
                TrackerManager.MANGA_UPDATES to
                    listOf(
                        MangaUpdates.READING_LIST to READING,
                        MangaUpdates.COMPLETE_LIST to COMPLETED,
                        MangaUpdates.ON_HOLD_LIST to PAUSED,
                        MangaUpdates.WISH_LIST to PLAN_TO_READ,
                        MangaUpdates.UNFINISHED_LIST to DROPPED,
                    ),
                TrackerManager.MANGABAKA to
                    listOf(
                        MangaBaka.READING to READING,
                        MangaBaka.COMPLETED to COMPLETED,
                        MangaBaka.PAUSED to PAUSED,
                        MangaBaka.PLAN_TO_READ to PLAN_TO_READ,
                        MangaBaka.DROPPED to DROPPED,
                        MangaBaka.REREADING to REPEATING,
                        // no exact analog for "considering"; Komikku treats it as plan to read
                        MangaBaka.CONSIDERING to PLAN_TO_READ,
                    ),
            )

        fun fromTracker(
            trackerId: Int,
            status: Int,
        ): UnifiedTrackStatus? {
            val mapping = mappings[trackerId] ?: return null
            // Bangumi has no "not tracked" status of its own; Komikku maps anything unknown to reading
            return mapping.firstOrNull { it.first == status }?.second ?: if (trackerId == TrackerManager.BANGUMI) READING else null
        }

        /** the tracker's status for [status], null when the tracker has no equivalent */
        fun toTracker(
            trackerId: Int,
            status: UnifiedTrackStatus,
        ): Int? = mappings[trackerId]?.firstOrNull { it.second == status }?.first

        /**
         * The unified status of a manga tracked by several trackers: the first tracker (by id) with a mappable status,
         * so the result does not depend on the order the records were written in.
         */
        fun ofRecords(records: Collection<Pair<Int, Int>>): UnifiedTrackStatus? =
            records.sortedBy { it.first }.firstNotNullOfOrNull { (trackerId, status) -> fromTracker(trackerId, status) }
    }
}

package suwayomi.tachidesk.manga.impl.track

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.manga.impl.metadata.AniListMetadataProvider
import suwayomi.tachidesk.manga.impl.metadata.MangaBakaMetadataProvider
import suwayomi.tachidesk.manga.impl.metadata.MangaUpdatesMetadataProvider
import suwayomi.tachidesk.manga.impl.metadata.MetadataDetails
import suwayomi.tachidesk.manga.impl.metadata.MetadataProvider
import suwayomi.tachidesk.manga.impl.metadata.MyAnimeListMetadataProvider
import suwayomi.tachidesk.manga.impl.track.tracker.TrackerManager
import suwayomi.tachidesk.manga.model.table.TrackRecordTable

/**
 * fork (Komikku port of MetadataAutofill): manga metadata of a tracker binding. It reuses this fork's metadata
 * providers, whose ids are the trackers' remote ids, and only reads: the client fills the metadata editor with the
 * result and the user saves it as local overrides, so source data and existing overrides are never replaced silently.
 */
object TrackerMetadata {
    private val providerByTracker: Map<Int, () -> MetadataProvider> =
        mapOf(
            TrackerManager.MYANIMELIST to ::MyAnimeListMetadataProvider,
            TrackerManager.ANILIST to ::AniListMetadataProvider,
            TrackerManager.MANGA_UPDATES to ::MangaUpdatesMetadataProvider,
            TrackerManager.MANGABAKA to ::MangaBakaMetadataProvider,
        )

    fun supportsMetadata(trackerId: Int): Boolean = trackerId in providerByTracker

    class NotTrackedException(
        message: String,
    ) : IllegalArgumentException(message)

    suspend fun getMetadata(
        userId: Int,
        mangaId: Int,
        trackerId: Int,
    ): MetadataDetails {
        val provider =
            providerByTracker[trackerId]?.invoke()
                ?: throw IllegalArgumentException(
                    "${TrackerManager.getTracker(trackerId)?.name ?: trackerId} does not provide manga metadata",
                )
        val remoteId =
            transaction {
                TrackRecordTable
                    .selectAll()
                    .where {
                        (TrackRecordTable.mangaId eq mangaId) and (TrackRecordTable.trackerId eq trackerId) and
                            (TrackRecordTable.user eq userId)
                    }.firstOrNull()
                    ?.get(TrackRecordTable.remoteId)
            } ?: throw NotTrackedException("The manga is not tracked with ${TrackerManager.getTracker(trackerId)?.name ?: trackerId}")

        val details = provider.getDetails(remoteId.toString())
        return details.copy(genre = autofillTags(details.genre.orEmpty()))
    }

    /** trimmed, without blanks and case-insensitive duplicates, in their original order */
    fun autofillTags(tags: List<String>): List<String> =
        tags
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .distinctBy { it.lowercase() }

    /** the current tags first, then the new ones that are not already present (case-insensitive) */
    fun mergeTags(
        current: List<String>,
        new: List<String>,
    ): List<String> = autofillTags(current + new)
}

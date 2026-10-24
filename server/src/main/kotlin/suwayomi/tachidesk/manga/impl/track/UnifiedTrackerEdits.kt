package suwayomi.tachidesk.manga.impl.track

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import com.expediagroup.graphql.generator.annotations.GraphQLName
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.manga.impl.track.tracker.Tracker
import suwayomi.tachidesk.manga.impl.track.tracker.TrackerManager
import suwayomi.tachidesk.manga.model.table.TrackRecordTable
import kotlin.math.abs

/**
 * fork (Komikku port of UpdateTracks/UnifiedTrackerEdits): applies one edit to every tracker a manga is tracked with.
 *
 * Each tracker is refreshed first and then updated remotely; a value the tracker cannot represent (a status without
 * equivalent, reading dates on a tracker without dates) is reported as skipped instead of being claimed as success.
 */
object UnifiedTrackerEdits {
    private val logger = KotlinLogging.logger {}

    // edits of the same user are serialized, so two quick edits cannot interleave their refresh/update round trips
    private val mutexes = mutableMapOf<Int, Mutex>()

    private fun mutexFor(userId: Int) = synchronized(mutexes) { mutexes.getOrPut(userId) { Mutex() } }

    data class Edit(
        val status: UnifiedTrackStatus? = null,
        val lastChapterRead: Double? = null,
        /** 0..10 */
        val score: Double? = null,
        val startDate: Long? = null,
        val finishDate: Long? = null,
    ) {
        fun isEmpty() = listOf(status, lastChapterRead, score, startDate, finishDate).all { it == null }
    }

    @GraphQLName("UnifiedTrackEditOutcome")
    enum class Outcome {
        UPDATED,
        SKIPPED,
        FAILED,
    }

    data class Result(
        val recordId: Int,
        val trackerId: Int,
        val outcome: Outcome,
        /** why fields were skipped or the update failed */
        val messages: List<String>,
    )

    suspend fun apply(
        userId: Int,
        mangaId: Int,
        edit: Edit,
    ): List<Result> {
        require(!edit.isEmpty()) { "Nothing to update" }
        edit.score?.let { require(it in 0.0..10.0) { "The score must be between 0 and 10" } }
        if (edit.startDate != null && edit.finishDate != null && edit.startDate > 0 && edit.finishDate > 0) {
            require(edit.startDate <= edit.finishDate) { "The start date must not be after the finish date" }
        }

        val records =
            transaction {
                TrackRecordTable
                    .selectAll()
                    .where { (TrackRecordTable.mangaId eq mangaId) and (TrackRecordTable.user eq userId) }
                    .map { it[TrackRecordTable.id].value to it[TrackRecordTable.trackerId] }
            }

        return mutexFor(userId).withLock {
            records.map { (recordId, trackerId) -> applyTo(userId, recordId, trackerId, edit) }
        }
    }

    private suspend fun applyTo(
        userId: Int,
        recordId: Int,
        trackerId: Int,
        edit: Edit,
    ): Result {
        val tracker =
            TrackerManager.getTracker(trackerId)
                ?: return Result(recordId, trackerId, Outcome.SKIPPED, listOf("Unknown tracker"))
        if (!tracker.isLoggedIn(userId)) {
            return Result(recordId, trackerId, Outcome.SKIPPED, listOf("Not logged in to ${tracker.name}"))
        }

        val skipped = mutableListOf<String>()
        val status =
            edit.status?.let { status ->
                UnifiedTrackStatus.toTracker(trackerId, status).also {
                    if (it == null) skipped += "${tracker.name} has no status equivalent to ${status.name}"
                }
            }
        val dateSupported = tracker.supportsReadingDates
        if (!dateSupported && (edit.startDate != null || edit.finishDate != null)) {
            skipped += "${tracker.name} does not support reading dates"
        }
        val score = edit.score?.let { nearestScore(userId, tracker, it) }

        val input =
            Track.UpdateInput(
                recordId = recordId,
                status = status,
                lastChapterRead = edit.lastChapterRead,
                scoreString = score,
                startDate = edit.startDate.takeIf { dateSupported },
                finishDate = edit.finishDate.takeIf { dateSupported },
            )
        if (listOf(input.status, input.lastChapterRead, input.scoreString, input.startDate, input.finishDate).all { it == null }) {
            return Result(recordId, trackerId, Outcome.SKIPPED, skipped)
        }

        return try {
            Track.refresh(userId, recordId)
            Track.update(userId, input)
            Result(recordId, trackerId, Outcome.UPDATED, skipped)
        } catch (e: Exception) {
            logger.warn(e) { "unified tracker edit of record $recordId (${tracker.name}) failed" }
            Result(recordId, trackerId, Outcome.FAILED, skipped + (e.message ?: e::class.simpleName.orEmpty()))
        }
    }

    /**
     * The entry of the tracker's score list closest to [score] on a 0-10 scale. Numeric lists are scaled by their
     * maximum (e.g. 0-100 or 0-5), other lists (e.g. smileys) by their position.
     */
    fun nearestScore(
        userId: Int,
        tracker: Tracker,
        score: Double,
    ): String? = nearestScore(tracker.getScoreList(userId), score)

    fun nearestScore(
        scores: List<String>,
        score: Double,
    ): String? {
        if (scores.isEmpty()) return null
        val numeric = scores.map { it.toDoubleOrNull() }
        val max = numeric.filterNotNull().maxOrNull()
        val scaled =
            if (numeric.count { it == null } <= 1 && max != null && max > 0) {
                numeric.map { (it ?: 0.0) / max * 10 }
            } else {
                scores.indices.map { if (scores.size == 1) 0.0 else it.toDouble() / (scores.size - 1) * 10 }
            }
        return scores[scaled.indices.minBy { abs(scaled[it] - score) }]
    }
}

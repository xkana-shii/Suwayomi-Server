package suwayomi.tachidesk.manga.impl.chapter

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import suwayomi.tachidesk.global.model.table.UserAccountTable
import suwayomi.tachidesk.graphql.server.primitives.inIds
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ChapterUserTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.server.settings.userConfig
import suwayomi.tachidesk.server.settings.value
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * fork: keeps CHAPTERUSER.BLACKLISTED / MERGE_HIDDEN in line with the user's scanlator rules (Komikku port).
 *
 * The flags are derived data: the rules in MANGAUSER (and the global Smart Scanlator Merge setting) are the source
 * of truth, and every change of the rules or of a manga's chapter list recomputes them. Recomputing is idempotent and
 * only writes rows whose flags change; a chapter with no rules applying has both flags false.
 */
object ChapterVisibilityService {
    private val logger = KotlinLogging.logger {}

    // recomputations of the same manga must not interleave (chapter refresh vs. a settings change)
    private val lock = ReentrantLock()

    private const val CHUNK_SIZE = 200

    /** Recomputes [mangaIds] for [userId]. */
    fun recompute(
        userId: Int,
        mangaIds: Collection<Int>,
    ) {
        if (mangaIds.isEmpty()) return
        lock.withLock {
            mangaIds.distinct().chunked(CHUNK_SIZE).forEach { chunk -> recomputeChunk(userId, chunk) }
        }
    }

    /** Recomputes a manga for every user that has rules for it or still has hidden chapters of it. */
    fun recomputeForManga(mangaId: Int) {
        val userIds =
            transaction {
                val withSettings =
                    MangaUserTable
                        .select(MangaUserTable.user)
                        .where { MangaUserTable.manga eq mangaId }
                        .map { it[MangaUserTable.user].value }
                val withFlags =
                    ChapterUserTable
                        .innerJoin(ChapterTable)
                        .select(ChapterUserTable.user)
                        .where { (ChapterTable.manga eq mangaId) and hiddenOp() }
                        .withDistinct()
                        .map { it[ChapterUserTable.user].value }
                (withSettings + withFlags).toSet()
            }
        userIds.forEach { recompute(it, listOf(mangaId)) }
    }

    /** Recomputes every manga of [userId], e.g. after the global Smart Scanlator Merge setting changed. */
    fun recomputeForUser(userId: Int) {
        val mangaIds =
            transaction {
                val withSettings =
                    MangaUserTable
                        .select(MangaUserTable.manga)
                        .where { MangaUserTable.user eq userId }
                        .map { it[MangaUserTable.manga].value }
                val withFlags =
                    ChapterUserTable
                        .innerJoin(ChapterTable)
                        .select(ChapterTable.manga)
                        .where { (ChapterUserTable.user eq userId) and hiddenOp() }
                        .withDistinct()
                        .map { it[ChapterTable.manga].value }
                (withSettings + withFlags).toSet()
            }
        logger.debug { "recomputing chapter visibility of ${mangaIds.size} manga for user $userId" }
        recompute(userId, mangaIds)
    }

    private fun hiddenOp(): Op<Boolean> = (ChapterUserTable.isBlacklisted eq true) or (ChapterUserTable.isMergeHidden eq true)

    private fun recomputeChunk(
        userId: Int,
        mangaIds: List<Int>,
    ) {
        transaction {
            val global = userConfig.smartScanlatorMerge.value(userId)
            val settingsByManga = ScanlatorSettings.getAll(userId, mangaIds)
            val mangaIdsWithRules = mangaIds.filter { settingsByManga[it]?.hasRules(global) == true }

            val chaptersByManga =
                if (mangaIdsWithRules.isEmpty()) {
                    emptyMap()
                } else {
                    loadChapters(mangaIdsWithRules)
                }

            val currentFlags =
                ChapterUserTable
                    .innerJoin(ChapterTable)
                    .select(ChapterUserTable.chapter, ChapterUserTable.isBlacklisted, ChapterUserTable.isMergeHidden)
                    .where { (ChapterTable.manga inIds mangaIds) and (ChapterUserTable.user eq userId) and hiddenOp() }
                    .associate {
                        it[ChapterUserTable.chapter].value to (it[ChapterUserTable.isBlacklisted] to it[ChapterUserTable.isMergeHidden])
                    }

            val desiredFlags = mutableMapOf<Int, Pair<Boolean, Boolean>>()
            mangaIdsWithRules.forEach { mangaId ->
                val settings = settingsByManga.getValue(mangaId)
                val result =
                    ChapterVisibility.compute(
                        chaptersByManga[mangaId].orEmpty(),
                        settings,
                        settings.smartMergeEnabled(global),
                    )
                result.blacklistedIds.forEach { desiredFlags[it] = true to false }
                result.mergeHiddenIds.forEach { desiredFlags[it] = false to true }
            }

            val changes =
                (currentFlags.keys + desiredFlags.keys)
                    .mapNotNull { chapterId ->
                        val desired = desiredFlags[chapterId] ?: (false to false)
                        val current = currentFlags[chapterId] ?: (false to false)
                        (chapterId to desired).takeIf { desired != current }
                    }.toMap()
            if (changes.isEmpty()) return@transaction

            val existingRows =
                ChapterUserTable
                    .select(ChapterUserTable.chapter)
                    .where { (ChapterUserTable.chapter inIds changes.keys) and (ChapterUserTable.user eq userId) }
                    .mapTo(HashSet()) { it[ChapterUserTable.chapter].value }

            changes
                .filterKeys { it in existingRows }
                .entries
                .groupBy({ it.value }, { it.key })
                .forEach { (flags, chapterIds) ->
                    chapterIds.chunked(CHUNK_SIZE).forEach { ids ->
                        ChapterUserTable.update({ (ChapterUserTable.chapter inIds ids) and (ChapterUserTable.user eq userId) }) {
                            it[isBlacklisted] = flags.first
                            it[isMergeHidden] = flags.second
                        }
                    }
                }

            val rowsToInsert = changes.filterKeys { it !in existingRows }.toList()
            if (rowsToInsert.isNotEmpty()) {
                ChapterUserTable.batchInsert(rowsToInsert) { (chapterId, flags) ->
                    this[ChapterUserTable.chapter] = EntityID(chapterId, ChapterTable)
                    this[ChapterUserTable.user] = EntityID(userId, UserAccountTable)
                    this[ChapterUserTable.isBlacklisted] = flags.first
                    this[ChapterUserTable.isMergeHidden] = flags.second
                }
            }
        }
    }

    fun loadChapters(mangaIds: Collection<Int>): Map<Int, List<VisibilityChapter>> =
        transaction {
            ChapterTable
                .select(
                    ChapterTable.id,
                    ChapterTable.manga,
                    ChapterTable.url,
                    ChapterTable.name,
                    ChapterTable.chapter_number,
                    ChapterTable.scanlator,
                    ChapterTable.sourceOrder,
                ).where { ChapterTable.manga inIds mangaIds }
                .groupBy({ it[ChapterTable.manga].value }) {
                    VisibilityChapter(
                        id = it[ChapterTable.id].value,
                        url = it[ChapterTable.url],
                        name = it[ChapterTable.name],
                        // through the string representation, so 10.1f compares equal to a 10.1 range bound
                        chapterNumber = it[ChapterTable.chapter_number].toString().toDouble(),
                        scanlator = it[ChapterTable.scanlator],
                        sourceOrder = it[ChapterTable.sourceOrder],
                    )
                }
        }

    /**
     * Adds [chapterIds] to (or removes them from) the chapter blacklist of their manga. New entries are hybrid keys
     * (URL plus number, scanlator and name); removing deletes every stored key that resolves to one of the chapters.
     */
    fun setBlacklisted(
        userId: Int,
        chapterIds: Collection<Int>,
        blacklisted: Boolean,
    ) {
        if (chapterIds.isEmpty()) return
        val mangaIds =
            transaction {
                ChapterTable
                    .select(ChapterTable.manga)
                    .where { ChapterTable.id inIds chapterIds }
                    .withDistinct()
                    .map { it[ChapterTable.manga].value }
            }
        val targetIds = chapterIds.toSet()
        lock.withLock {
            val chaptersByManga = loadChapters(mangaIds)
            val settingsByManga = ScanlatorSettings.getAll(userId, mangaIds)
            mangaIds.forEach { mangaId ->
                val chapters = chaptersByManga[mangaId].orEmpty()
                val targets = chapters.filter { it.id in targetIds }
                val stored = settingsByManga[mangaId]?.blacklistedChapters.orEmpty()
                val updated =
                    if (blacklisted) {
                        val alreadyBlacklisted = ChapterBlacklist.blacklistedIds(chapters, stored)
                        stored + targets.filter { it.id !in alreadyBlacklisted }.map(ChapterBlacklist::hybridKey)
                    } else {
                        val matching = ChapterBlacklist.keysMatching(chapters, targets, stored)
                        stored.filterNot { it in matching }
                    }
                if (updated != stored) {
                    ScanlatorSettings.update(userId, mangaId, blacklistedChapters = updated)
                }
            }
        }
        recompute(userId, mangaIds)
    }

    /** The ids of [chapterIds] that are hidden for [userId] (blacklisted or a merged duplicate). */
    fun hiddenChapterIds(
        userId: Int,
        chapterIds: Collection<Int>,
    ): Set<Int> {
        if (chapterIds.isEmpty()) return emptySet()
        return transaction {
            chapterIds.distinct().chunked(CHUNK_SIZE).flatMapTo(HashSet()) { ids ->
                ChapterUserTable
                    .select(ChapterUserTable.chapter)
                    .where { (ChapterUserTable.chapter inIds ids) and (ChapterUserTable.user eq userId) and hiddenOp() }
                    .map { it[ChapterUserTable.chapter].value }
            }
        }
    }
}

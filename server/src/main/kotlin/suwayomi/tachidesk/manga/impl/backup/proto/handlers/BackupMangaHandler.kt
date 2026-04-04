package suwayomi.tachidesk.manga.impl.backup.proto.handlers

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import eu.kanade.tachiyomi.source.model.UpdateStrategy
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.statements.BatchUpdateStatement
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import suwayomi.tachidesk.manga.impl.CategoryManga
import suwayomi.tachidesk.manga.impl.Chapter
import suwayomi.tachidesk.manga.impl.Chapter.modifyChaptersMetas
import suwayomi.tachidesk.manga.impl.Manga
import suwayomi.tachidesk.manga.impl.Manga.clearThumbnail
import suwayomi.tachidesk.manga.impl.Manga.modifyMangasMetas
import suwayomi.tachidesk.manga.impl.backup.BackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupChapter
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupHistory
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupManga
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupTracking
import suwayomi.tachidesk.manga.impl.track.tracker.TrackerManager
import suwayomi.tachidesk.manga.impl.track.tracker.model.toTrack
import suwayomi.tachidesk.manga.impl.track.tracker.model.toTrackRecordDataClass
import suwayomi.tachidesk.manga.model.dataclass.TrackRecordDataClass
import suwayomi.tachidesk.manga.model.table.*
import suwayomi.tachidesk.manga.model.table.toDataClass
import suwayomi.tachidesk.server.database.dbTransaction
import java.util.Date
import kotlin.math.max
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import suwayomi.tachidesk.manga.impl.track.Track as Tracker
import io.github.oshai.kotlinlogging.KotlinLogging

object BackupMangaHandler {
    private val logger = KotlinLogging.logger {}
    private enum class RestoreMode {
        NEW,
        EXISTING,
    }

    // small helper for timings
    private fun nowMs(): Long = System.nanoTime() / 1_000_000L

    /**
     * Optimized backup that bulk-prefetches related tables inside a short db transaction,
     * then assembles the BackupManga list outside the transaction (no DB connection held
     * while building/serializing).
     *
     * The optional progress callback is invoked per-manga during assembly:
     *   progress(currentIndex, totalMangaCount, title)
     */
    fun backup(flags: BackupFlags, progress: ((Int, Int, String) -> Unit)? = null): List<BackupManga> {
        if (!flags.includeManga) {
            return emptyList()
        }

        val tStart = nowMs()

        // Holder for prefetched data
        data class Prefetched(
            val mangaRows: List<ResultRow>,
            val mangaMetaByMangaId: Map<Int, Map<String, String>>,
            val chaptersByMangaId: Map<Int, List<suwayomi.tachidesk.manga.model.dataclass.ChapterDataClass>>,
            val chapterMetaByChapterId: Map<Int, Map<String, String>>,
            val categoriesByMangaId: Map<Int, List<Int>>,
            val tracksByMangaId: Map<Int, List<TrackRecordDataClass>>,
        )

        // Stage 1: Bulk fetch everything we need inside a short transaction and convert to in-memory maps
        val fetchStart = nowMs()
        val prefetched = dbTransaction {
            // 1) fetch manga rows
            val mangaRows = MangaTable.selectAll().where { MangaTable.inLibrary eq true }.toList()
            val mangaIds = mangaRows.map { it[MangaTable.id].value }

            // 2) manga metas
            val mangaMetaByMangaId =
                if (flags.includeClientData && mangaIds.isNotEmpty()) {
                    MangaMetaTable
                        .selectAll()
                        .where { MangaMetaTable.ref inList mangaIds }
                        .groupBy { it[MangaMetaTable.ref].value }
                        .mapValues { (_, rows) -> rows.associate { it[MangaMetaTable.key] to it[MangaMetaTable.value] } }
                        .withDefault { emptyMap() }
                } else {
                    emptyMap()
                }

            // 3) chapters for all manga
            val chaptersList =
                if ((flags.includeChapters || flags.includeHistory) && mangaIds.isNotEmpty()) {
                    ChapterTable
                        .selectAll()
                        .where { ChapterTable.manga inList mangaIds }
                        .orderBy(ChapterTable.sourceOrder to SortOrder.DESC)
                        .map { ChapterTable.toDataClass(it) }
                } else {
                    emptyList()
                }
            val chaptersByMangaId = if (chaptersList.isNotEmpty()) chaptersList.groupBy { it.mangaId } else emptyMap()

            // 4) chapter metas
            val chapterMetaByChapterId =
                if (flags.includeClientData && chaptersList.isNotEmpty()) {
                    val chapterIds = chaptersList.map { it.id }
                    ChapterMetaTable
                        .selectAll()
                        .where { ChapterMetaTable.ref inList chapterIds }
                        .groupBy { it[ChapterMetaTable.ref].value }
                        .mapValues { (_, rows) -> rows.associate { it[ChapterMetaTable.key] to it[ChapterMetaTable.value] } }
                        .withDefault { emptyMap() }
                } else {
                    emptyMap()
                }

            // 5) categories for all manga (collect category.order)
            val categoriesByMangaId =
                if (flags.includeCategories && mangaIds.isNotEmpty()) {
                    CategoryMangaTable
                        .innerJoin(CategoryTable)
                        .selectAll()
                        .where { CategoryMangaTable.manga inList mangaIds }
                        .orderBy(CategoryTable.order to SortOrder.ASC)
                        .groupBy({ it[CategoryMangaTable.manga].value }, { CategoryTable.toDataClass(it).order })
                } else {
                    emptyMap()
                }

            // 6) tracks for all manga
            val tracksByMangaId =
                if (flags.includeTracking && mangaIds.isNotEmpty()) {
                    TrackRecordTable
                        .selectAll()
                        .where { TrackRecordTable.mangaId inList mangaIds }
                        .map { it.toTrackRecordDataClass() }
                        .groupBy { it.mangaId }
                } else {
                    emptyMap()
                }

            Prefetched(
                mangaRows = mangaRows,
                mangaMetaByMangaId = mangaMetaByMangaId,
                chaptersByMangaId = chaptersByMangaId,
                chapterMetaByChapterId = chapterMetaByChapterId,
                categoriesByMangaId = categoriesByMangaId,
                tracksByMangaId = tracksByMangaId,
            )
        } // transaction ends here; connection returned to pool
        val fetchEnd = nowMs()
        val fetchMs = fetchEnd - fetchStart

        // For compatibility with old log format, map_total is the time spent mapping INSIDE the transaction.
        // In this optimized workflow mapping happens outside the transaction, so it's 0 here.
        val mapTotalMsInsideTransaction = 0L
        val mappedCount = prefetched.mangaRows.size

        // Log the inside-transaction line using the exact same format as the original implementation
        logger.info {
            "BackupMangaHandler.backup (inside transaction): fetch=${fetchMs}ms, map_total=${mapTotalMsInsideTransaction}ms, mapped_count=${mappedCount}"
        }

        // Stage 2: assemble BackupManga objects in-memory, reporting per-manga progress via callback
        val assembleStart = nowMs()
        val total = prefetched.mangaRows.size
        val result = prefetched.mangaRows.mapIndexed { index, mangaRow ->
            val backupManga =
                BackupManga(
                    source = mangaRow[MangaTable.sourceReference],
                    url = mangaRow[MangaTable.url],
                    title = mangaRow[MangaTable.title],
                    artist = mangaRow[MangaTable.artist],
                    author = mangaRow[MangaTable.author],
                    description = mangaRow[MangaTable.description],
                    genre = mangaRow[MangaTable.genre]?.split(", ") ?: emptyList(),
                    status = MangaStatus.valueOf(mangaRow[MangaTable.status]).value,
                    thumbnailUrl = mangaRow[MangaTable.thumbnail_url],
                    dateAdded = mangaRow[MangaTable.inLibraryAt].seconds.inWholeMilliseconds,
                    viewer = 0, // not supported in Tachidesk
                    updateStrategy = UpdateStrategy.valueOf(mangaRow[MangaTable.updateStrategy]),
                )

            val mangaId = mangaRow[MangaTable.id].value

            if (flags.includeClientData) {
                backupManga.meta = prefetched.mangaMetaByMangaId[mangaId] ?: emptyMap()
            }

            if (flags.includeChapters || flags.includeHistory) {
                val chapters = prefetched.chaptersByMangaId[mangaId].orEmpty()
                if (flags.includeChapters) {
                    val chapterToMeta = prefetched.chapterMetaByChapterId
                    backupManga.chapters =
                        chapters.map {
                            BackupChapter(
                                it.url,
                                it.name,
                                it.scanlator,
                                it.read,
                                it.bookmarked,
                                it.fillermarked,
                                it.lastPageRead,
                                it.fetchedAt.seconds.inWholeMilliseconds,
                                it.uploadDate,
                                it.chapterNumber,
                                chapters.size - it.index,
                            ).apply {
                                if (flags.includeClientData) {
                                    this.meta = chapterToMeta[it.id] ?: emptyMap()
                                }
                            }
                        }
                }
                if (flags.includeHistory) {
                    backupManga.history =
                        chapters.mapNotNull {
                            if (it.lastReadAt > 0) {
                                BackupHistory(
                                    url = it.url,
                                    lastRead = it.lastReadAt.seconds.inWholeMilliseconds,
                                )
                            } else {
                                null
                            }
                        }
                }
            }

            if (flags.includeCategories) {
                backupManga.categories = prefetched.categoriesByMangaId[mangaId] ?: emptyList()
            }

            if (flags.includeTracking) {
                val records = prefetched.tracksByMangaId[mangaId].orEmpty()
                val tracks =
                    records.map { rec ->
                        BackupTracking(
                            syncId = rec.trackerId,
                            libraryId = rec.libraryId ?: 0,
                            mediaId = rec.remoteId,
                            title = rec.title,
                            lastChapterRead = rec.lastChapterRead.toFloat(),
                            totalChapters = rec.totalChapters,
                            score = rec.score.toFloat(),
                            status = rec.status,
                            startedReadingDate = rec.startDate,
                            finishedReadingDate = rec.finishDate,
                            trackingUrl = rec.remoteUrl,
                            private = rec.private,
                        )
                    }
                if (tracks.isNotEmpty()) {
                    backupManga.tracking = tracks
                }
            }

            // report progress after assembling each manga (no DB connection held)
            progress?.invoke(index + 1, total, backupManga.title)

            backupManga
        }
        val assembleEnd = nowMs()

        val tEnd = nowMs()

        // Log total timing in the exact same format as the original implementation
        logger.info { "BackupMangaHandler.backup timing: total=${tEnd - tStart}ms" }

        return result
    }

    // The rest of the restore functions remain unchanged...
    fun restore(
        backupManga: BackupManga,
        categoryMapping: Map<Int, Int>,
        sourceMapping: Map<Long, String>,
        errors: MutableList<Pair<Date, String>>,
        flags: BackupFlags,
    ) {
        val chapters = backupManga.chapters
        val categories = backupManga.categories
        val history = backupManga.history
        val tracking = backupManga.tracking

        val dbCategoryIds = categories.mapNotNull { categoryMapping[it] }

        try {
            restoreMangaData(backupManga, chapters, dbCategoryIds, history, tracking, flags)
        } catch (e: Exception) {
            val sourceName = sourceMapping[backupManga.source] ?: backupManga.source.toString()
            errors.add(Date() to "${backupManga.title} [$sourceName]: ${e.message}")
        }
    }

    private fun restoreMangaData(
        manga: BackupManga,
        chapters: List<BackupChapter>,
        categoryIds: List<Int>,
        history: List<BackupHistory>,
        tracks: List<BackupTracking>,
        flags: BackupFlags,
    ) {
        val dbManga =
            transaction {
                MangaTable
                    .selectAll()
                    .where { (MangaTable.url eq manga.url) and (MangaTable.sourceReference eq manga.source) }
                    .firstOrNull()
            }
        val restoreMode = if (dbManga != null) RestoreMode.EXISTING else RestoreMode.NEW

        val mangaId =
            transaction {
                val mangaId =
                    if (dbManga == null) {
                        // insert manga to database
                        MangaTable
                            .insertAndGetId {
                                it[url] = manga.url
                                it[title] = manga.title

                                it[artist] = manga.artist
                                it[author] = manga.author
                                it[description] = manga.description
                                it[genre] = manga.genre.joinToString()
                                it[status] = manga.status
                                it[thumbnail_url] = manga.thumbnailUrl
                                it[updateStrategy] = manga.updateStrategy.name

                                it[sourceReference] = manga.source

                                it[initialized] = manga.description != null

                                it[inLibrary] = manga.favorite

                                it[inLibraryAt] = manga.dateAdded.milliseconds.inWholeSeconds
                            }.value
                    } else {
                        val dbMangaId = dbManga[MangaTable.id].value

                        // Merge manga data
                        MangaTable.update({ MangaTable.id eq dbMangaId }) {
                            it[artist] = manga.artist ?: dbManga[artist]
                            it[author] = manga.author ?: dbManga[author]
                            it[description] = manga.description ?: dbManga[description]
                            it[genre] = manga.genre.ifEmpty { null }?.joinToString() ?: dbManga[genre]
                            it[status] = manga.status
                            it[thumbnail_url] = manga.thumbnailUrl ?: dbManga[thumbnail_url]
                            it[updateStrategy] = manga.updateStrategy.name

                            it[initialized] = dbManga[initialized] || manga.description != null

                            it[inLibrary] = manga.favorite || dbManga[inLibrary]

                            it[inLibraryAt] = manga.dateAdded.milliseconds.inWholeSeconds
                        }

                        dbMangaId
                    }

                // delete thumbnail in case cached data still exists
                clearThumbnail(mangaId)

                if (flags.includeClientData && manga.meta.isNotEmpty()) {
                    modifyMangasMetas(mapOf(mangaId to manga.meta))
                }

                // merge chapter data
                if (flags.includeChapters || flags.includeHistory) {
                    restoreMangaChapterData(mangaId, restoreMode, chapters, history, flags)
                }

                // merge categories
                if (flags.includeCategories) {
                    restoreMangaCategoryData(mangaId, categoryIds)
                }

                mangaId
            }

        if (flags.includeTracking) {
            restoreMangaTrackerData(mangaId, tracks)
        }

        // TODO: insert/merge history
    }

    private fun getMangaChapterToRestoreInfo(
        mangaId: Int,
        restoreMode: RestoreMode,
        chapters: List<BackupChapter>,
    ): Pair<List<BackupChapter>, List<Pair<BackupChapter, ResultRow>>> {
        val uniqueChapters = chapters.distinctBy { it.url }

        if (restoreMode == RestoreMode.NEW) {
            return Pair(uniqueChapters, emptyList())
        }

        val dbChaptersByUrl = ChapterTable.selectAll().where { ChapterTable.manga eq mangaId }.associateBy { it[ChapterTable.url] }

        val (chaptersToUpdate, chaptersToInsert) = uniqueChapters.partition { dbChaptersByUrl.contains(it.url) }
        val chaptersToUpdateToDbChapter = chaptersToUpdate.map { it to dbChaptersByUrl[it.url]!! }

        return chaptersToInsert to chaptersToUpdateToDbChapter
    }

    private fun restoreMangaChapterData(
        mangaId: Int,
        restoreMode: RestoreMode,
        chapters: List<BackupChapter>,
        history: List<BackupHistory>,
        flags: BackupFlags,
    ) = dbTransaction {
        val (chaptersToInsert, chaptersToUpdateToDbChapter) = getMangaChapterToRestoreInfo(mangaId, restoreMode, chapters)
        val historyByChapter = history.groupBy({ it.url }, { it.lastRead })

        val insertedChapterIds =
            if (flags.includeChapters) {
                ChapterTable
                    .batchInsert(chaptersToInsert) { chapter ->
                        this[ChapterTable.url] = chapter.url
                        this[ChapterTable.name] = chapter.name
                        if (chapter.dateUpload == 0L) {
                            this[ChapterTable.date_upload] = chapter.dateFetch
                        } else {
                            this[ChapterTable.date_upload] = chapter.dateUpload
                        }
                        this[ChapterTable.chapter_number] = chapter.chapterNumber
                        this[ChapterTable.scanlator] = chapter.scanlator

                        this[ChapterTable.sourceOrder] = chaptersToInsert.size - chapter.sourceOrder
                        this[ChapterTable.manga] = mangaId

                        this[ChapterTable.isRead] = chapter.read
                        this[ChapterTable.lastPageRead] = chapter.lastPageRead.coerceAtLeast(0)
                        this[ChapterTable.isBookmarked] = chapter.bookmark
                        this[ChapterTable.isFillermarked] = chapter.fillermark

                        this[ChapterTable.fetchedAt] = chapter.dateFetch.milliseconds.inWholeSeconds

                        if (flags.includeHistory) {
                            this[ChapterTable.lastReadAt] =
                                historyByChapter[chapter.url]?.maxOrNull()?.milliseconds?.inWholeSeconds ?: 0
                        }
                    }.map { it[ChapterTable.id].value }
            } else {
                emptyList()
            }

        if (chaptersToUpdateToDbChapter.isNotEmpty()) {
            BatchUpdateStatement(ChapterTable).apply {
                chaptersToUpdateToDbChapter.forEach { (backupChapter, dbChapter) ->
                    addBatch(EntityID(dbChapter[ChapterTable.id].value, ChapterTable))
                    if (flags.includeChapters) {
                        this[ChapterTable.isRead] = backupChapter.read || dbChapter[ChapterTable.isRead]
                        this[ChapterTable.lastPageRead] =
                            max(backupChapter.lastPageRead, dbChapter[ChapterTable.lastPageRead]).coerceAtLeast(0)
                        this[ChapterTable.isBookmarked] = backupChapter.bookmark || dbChapter[ChapterTable.isBookmarked]
                        this[ChapterTable.isFillermarked] = backupChapter.fillermark || dbChapter[ChapterTable.isFillermarked]
                    }

                    if (flags.includeHistory) {
                        this[ChapterTable.lastReadAt] =
                            (historyByChapter[backupChapter.url]?.maxOrNull()?.milliseconds?.inWholeSeconds ?: 0)
                                .coerceAtLeast(dbChapter[ChapterTable.lastReadAt])
                    }
                }
                execute(this@dbTransaction)
            }
        }

        if (flags.includeClientData) {
            val chaptersToInsertByChapterId = insertedChapterIds.zip(chaptersToInsert)
            val chapterToUpdateByChapterId =
                chaptersToUpdateToDbChapter.map { (backupChapter, dbChapter) ->
                    dbChapter[ChapterTable.id].value to
                        backupChapter
                }
            val metaEntryByChapterId =
                (chaptersToInsertByChapterId + chapterToUpdateByChapterId)
                    .associate { (chapterId, backupChapter) ->
                        chapterId to backupChapter.meta
                    }

            modifyChaptersMetas(metaEntryByChapterId)
        }
    }

    private fun restoreMangaCategoryData(
        mangaId: Int,
        categoryIds: List<Int>,
    ) {
        CategoryManga.addMangaToCategories(mangaId, categoryIds)
    }

    private fun restoreMangaTrackerData(
        mangaId: Int,
        tracks: List<BackupTracking>,
    ) {
        val dbTrackRecordsByTrackerId =
            Tracker
                .getTrackRecordsByMangaId(mangaId)
                .mapNotNull { it.record?.toTrack() }
                .associateBy { it.tracker_id }

        val (existingTracks, newTracks) =
            tracks
                .mapNotNull { backupTrack ->
                    val track = backupTrack.toTrack(mangaId)

                    val isUnsupportedTracker = TrackerManager.getTracker(track.tracker_id) == null
                    if (isUnsupportedTracker) {
                        return@mapNotNull null
                    }

                    val dbTrack =
                        dbTrackRecordsByTrackerId[backupTrack.syncId]
                            ?: // new track
                            return@mapNotNull track

                    if (track.toTrackRecordDataClass().forComparison() == dbTrack.toTrackRecordDataClass().forComparison()) {
                        return@mapNotNull null
                    }

                    dbTrack.also {
                        it.remote_id = track.remote_id
                        it.library_id = track.library_id
                        it.last_chapter_read = max(dbTrack.last_chapter_read, track.last_chapter_read)
                    }
                }.partition { (it.id ?: -1) > 0 }

        Tracker.updateTrackRecords(existingTracks)
        Tracker.insertTrackRecords(newTracks)
    }

    private fun TrackRecordDataClass.forComparison() = this.copy(id = 0, mangaId = 0)
}

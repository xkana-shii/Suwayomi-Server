package suwayomi.tachidesk.manga.impl.download.fileProvider

import eu.kanade.tachiyomi.source.local.metadata.COMIC_INFO_FILE
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.sample
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import libcore.net.MimeUtils
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import suwayomi.tachidesk.graphql.types.DownloadConversion
import suwayomi.tachidesk.manga.impl.Page
import suwayomi.tachidesk.manga.impl.chapter.getChapterDownloadReady
import suwayomi.tachidesk.manga.impl.download.model.DownloadQueueItem
import suwayomi.tachidesk.manga.impl.util.KoreaderHelper
import suwayomi.tachidesk.manga.impl.util.createComicInfoFile
import suwayomi.tachidesk.manga.impl.util.getChapterCachePath
import suwayomi.tachidesk.manga.impl.util.getChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.getChapterDownloadPath
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterDownloadFolder
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.storage.ImageResponse
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.server.serverConfig
import suwayomi.tachidesk.util.ConversionUtil
import java.io.File
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import javax.imageio.IIOImage
import javax.imageio.ImageIO
import javax.imageio.ImageWriteParam
import javax.imageio.ImageWriter

sealed class FileType {
    data class RegularFile(
        val file: File,
    ) : FileType()

    data class ZipFile(
        val entry: ZipArchiveEntry,
    ) : FileType()

    fun getName(): String =
        when (this) {
            is RegularFile -> {
                this.file.name
            }

            is ZipFile -> {
                this.entry.name
            }
        }

    fun getExtension(): String =
        when (this) {
            is RegularFile -> {
                this.file.extension
            }

            is ZipFile -> {
                this.entry.name.substringAfterLast(".")
            }
        }
}

/*
* Base class for downloaded chapter files provider, example: Folder, Archive
*/
abstract class ChaptersFilesProvider<Type : FileType>(
    val mangaId: Int,
    val chapterId: Int,
) : DownloadedFilesProvider {
    protected val logger = KotlinLogging.logger {}

    protected abstract fun getImageFiles(): List<Type>

    protected abstract fun getImageInputStream(image: Type): InputStream

    fun getImageImpl(index: Int): Pair<InputStream, String> {
        val images = getImageFiles().filter { it.getName() != COMIC_INFO_FILE }.sortedBy { it.getName() }

        if (images.isEmpty()) {
            throw Exception("no downloaded images found")
        }

        val image = images[index]
        val imageFileType = image.getExtension()

        return Pair(getImageInputStream(image).buffered(), MimeUtils.guessMimeTypeFromExtension(imageFileType) ?: "image/$imageFileType")
    }

    fun getImageCount(): Int = getImageFiles().filter { it.getName() != COMIC_INFO_FILE }.size

    override fun getImage(): RetrieveFile1Args<Int> = RetrieveFile1Args(::getImageImpl)

    /**
     * Extract the existing download to the base download folder (see [getChapterDownloadPath])
     */
    protected abstract fun extractExistingDownload()

    protected abstract suspend fun handleSuccessfulDownload()

    @OptIn(FlowPreview::class)
    private suspend fun downloadImpl(
        download: DownloadQueueItem,
        scope: CoroutineScope,
        step: suspend (DownloadQueueItem?, Boolean) -> Unit,
    ): Boolean {
        val existingDownloadPageCount =
            try {
                getImageCount()
            } catch (_: Exception) {
                0
            }
        val pageCount = download.pageCount

        check(pageCount > 0) { "pageCount must be greater than 0 - ChapterForDownload#getChapterDownloadReady not called" }
        check(existingDownloadPageCount == 0 || existingDownloadPageCount == pageCount) {
            "existingDownloadPageCount must be 0 or equal to pageCount - ChapterForDownload#getChapterDownloadReady not called"
        }

        val doesUnrecognizedDownloadExist = existingDownloadPageCount == pageCount
        if (doesUnrecognizedDownloadExist) {
            download.progress = 1f
            step(download, false)

            return true
        }

        extractExistingDownload()

        // Prefer any previously existing candidate folder; otherwise use canonical generated path
        val finalDownloadFolder = resolveExistingChapterDownloadFolder(mangaId, chapterId) ?: getChapterDownloadPath(mangaId, chapterId)

        val cacheChapterDir = getChapterCachePath(mangaId, chapterId)
        val downloadCacheFolder = File(cacheChapterDir)
        downloadCacheFolder.mkdirs()

        // Concurrent page-download implementation using a Semaphore to limit concurrency.
        val pageConcurrency = serverConfig.maxPagesInParallel.value.coerceAtLeast(1)
        val semaphore = Semaphore(pageConcurrency)

        // Map to hold per-page percent (0..100). Uses ConcurrentHashMap for thread-safe updates.
        val pageProgressMap = ConcurrentHashMap<Int, Int>().apply {
            // initialize all pages to 0%
            for (i in 0 until pageCount) {
                this[i] = 0
            }
        }
        val completedPages = AtomicInteger(0)

        // Use coroutineScope so failures/cancellation propagate and we can await all page jobs.
        coroutineScope {
            val jobs = (0 until pageCount).map { pageNum ->
                async {
                    val fileName = Page.getPageName(pageNum, pageCount) // might have to change this to index stored in database

                    val pageExistsInFinalDownloadFolder = ImageResponse.findFileNameStartingWith(finalDownloadFolder, fileName) != null
                    val pageExistsInCacheDownloadFolder = ImageResponse.findFileNameStartingWith(cacheChapterDir, fileName) != null

                    val doesPageAlreadyExist = pageExistsInFinalDownloadFolder || pageExistsInCacheDownloadFolder
                    if (doesPageAlreadyExist) {
                        // Mark page as complete for progress aggregation
                        pageProgressMap[pageNum] = 100
                        completedPages.incrementAndGet()
                        // update aggregated progress and notify
                        download.progress = completedPages.get().toFloat() / pageCount
                        step(download, false)
                        return@async
                    }

                    // Limit concurrent page downloads via semaphore
                    semaphore.withPermit {
                        var pageProgressJob: Job? = null
                        try {
                            Page.getPageImageDownload(
                                mangaId = download.mangaId,
                                chapterId = download.chapterId,
                                index = pageNum,
                                downloadCacheFolder,
                                fileName,
                            ) { flow ->
                                pageProgressJob =
                                    flow
                                        .sample(100)
                                        .distinctUntilChanged()
                                        .onEach { progressValue ->
                                            // Update this page's percent and compute aggregated progress.
                                            pageProgressMap[pageNum] = progressValue
                                            val totalPercent = pageProgressMap.values.sum()
                                            val overallProgress = totalPercent.toFloat() / (pageCount * 100)
                                            download.progress = overallProgress
                                            // Notify progress (non-throwing)
                                            step(null, false)
                                        }.launchIn(scope)
                            }

                            // Mark page as finished (100%) after successful download
                            pageProgressMap[pageNum] = 100
                            completedPages.incrementAndGet()
                            download.progress = completedPages.get().toFloat() / pageCount
                            step(download, false)
                        } finally {
                            // always cancel the page progress job even if it throws an exception to avoid memory leaks
                            pageProgressJob?.cancel()
                        }
                    }
                }
            }
            // Wait for all pages to finish. Exceptions will propagate and cancel siblings.
            jobs.awaitAll()
        }

        // Create ComicInfo.xml in cache folder (the util will encode using XML serializer)
        createComicInfoFile(
            downloadCacheFolder.toPath(),
            transaction {
                MangaTable.selectAll().where { MangaTable.id eq mangaId }.first()
            },
            transaction {
                ChapterTable.selectAll().where { ChapterTable.id eq chapterId }.first()
            },
        )

        handleSuccessfulDownload()

        // Calculate and save Koreader hash for CBZ files (try existing cbz candidate first)
        val chapterFile = File(resolveExistingChapterCbzPath(mangaId, chapterId) ?: getChapterCbzPath(mangaId, chapterId))
        if (chapterFile.exists()) {
            val koreaderHash = KoreaderHelper.hashContents(chapterFile)
            if (koreaderHash != null) {
                transaction {
                    ChapterTable.update({ ChapterTable.id eq chapterId }) {
                        it[ChapterTable.koreaderHash] = koreaderHash
                    }
                }
            }
        }

        File(cacheChapterDir).deleteRecursively()

        return true
    }

    /**
     * This function should never be called without calling [getChapterDownloadReady] beforehand.
     */
    override fun download(): FileDownload3Args<DownloadQueueItem, CoroutineScope, suspend (DownloadQueueItem?, Boolean) -> Unit> =
        FileDownload3Args(::downloadImpl)

    abstract override fun delete(): Boolean

    abstract fun getAsArchiveStream(): Pair<InputStream, Long>

    abstract fun getArchiveSize(): Long
}

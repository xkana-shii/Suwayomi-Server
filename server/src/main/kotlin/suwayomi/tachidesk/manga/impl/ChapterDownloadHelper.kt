package suwayomi.tachidesk.manga.impl

import kotlinx.coroutines.CoroutineScope
import org.jetbrains.exposed.sql.transactions.transaction
import suwayomi.tachidesk.manga.impl.chapter.getChapterDownloadReady
import suwayomi.tachidesk.manga.impl.download.fileProvider.ChaptersFilesProvider
import suwayomi.tachidesk.manga.impl.download.fileProvider.impl.ArchiveProvider
import suwayomi.tachidesk.manga.impl.download.fileProvider.impl.FolderProvider
import suwayomi.tachidesk.manga.impl.download.model.DownloadQueueItem
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterDownloadFolder
import suwayomi.tachidesk.manga.impl.util.getChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.getChapterDownloadPath
import suwayomi.tachidesk.manga.model.dataclass.ChapterDataClass
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.toDataClass
import suwayomi.tachidesk.server.serverConfig
import java.io.File
import java.io.InputStream

object ChapterDownloadHelper {
    fun getImage(
        mangaId: Int,
        chapterId: Int,
        index: Int,
    ): Pair<InputStream, String> = provider(mangaId, chapterId).getImage().execute(index)

    fun getImageCount(
        mangaId: Int,
        chapterId: Int,
    ): Int = provider(mangaId, chapterId).getImageCount()

    fun delete(
        mangaId: Int,
        chapterId: Int,
    ): Boolean = provider(mangaId, chapterId).delete()

    /**
     * This function should never be called without calling [getChapterDownloadReady] beforehand.
     */
    suspend fun download(
        mangaId: Int,
        chapterId: Int,
        download: DownloadQueueItem,
        scope: CoroutineScope,
        step: suspend (DownloadQueueItem?, Boolean) -> Unit,
    ): Boolean = provider(mangaId, chapterId).download().execute(download, scope, step)

    // return the appropriate provider based on existing saved download type.
    private fun provider(
        mangaId: Int,
        chapterId: Int,
    ): ChaptersFilesProvider<*> {
        // If a CBZ already exists among candidates, use ArchiveProvider
        if (resolveExistingChapterCbzPath(mangaId, chapterId) != null) return ArchiveProvider(mangaId, chapterId)

        // If a folder exists among candidates, use FolderProvider
        if (resolveExistingChapterDownloadFolder(mangaId, chapterId) != null) return FolderProvider(mangaId, chapterId)

        // If config prefers cbz for new downloads, write CBZ provider
        if (serverConfig.downloadAsCbz.value) return ArchiveProvider(mangaId, chapterId)

        // Default fallback: folder provider
        return FolderProvider(mangaId, chapterId)
    }

    fun getArchiveStreamWithSize(
        mangaId: Int,
        chapterId: Int,
    ): Pair<InputStream, Long> = provider(mangaId, chapterId).getAsArchiveStream()

    private fun getChapterWithCbzFileName(chapterId: Int): Pair<ChapterDataClass, String> =
        transaction {
            val row =
                (ChapterTable innerJoin MangaTable)
                    .select(ChapterTable.columns + MangaTable.columns)
                    .where { ChapterTable.id eq chapterId }
                    .firstOrNull() ?: throw IllegalArgumentException("ChapterId $chapterId not found")
            val chapter = ChapterTable.toDataClass(row)
            val mangaTitle = row[MangaTable.title]

            val scanlatorPart = chapter.scanlator?.let { "[$it] " } ?: ""
            val fileName = "$scanlatorPart${chapter.name}"
            chapter to fileName
        }

    // Added functions requested: return CBZ stream / metadata (filename + size).
    fun getCbzForDownload(
        chapterId: Int,
        markAsRead: Boolean?,
    ): Triple<InputStream, String, Long> {
        // Build chapter info and filename including manga title and .cbz extension
        val (chapterData, baseName) = transaction {
            val row =
                (ChapterTable innerJoin MangaTable)
                    .select(ChapterTable.columns + MangaTable.columns)
                    .where { ChapterTable.id eq chapterId }
                    .firstOrNull() ?: throw IllegalArgumentException("ChapterId $chapterId not found")
            val chapter = ChapterTable.toDataClass(row)
            val mangaTitle = row[MangaTable.title]
            val scanlatorPart = chapter.scanlator?.let { "[$it] " } ?: ""
            val fileName = "$mangaTitle - $scanlatorPart${chapter.name}.cbz"
            chapter to fileName
        }

        val cbzStreamWithSize = provider(chapterData.mangaId, chapterData.id).getAsArchiveStream()

        if (markAsRead == true) {
            Chapter.modifyChapter(
                chapterData.mangaId,
                chapterData.index,
                isRead = true,
                isBookmarked = null,
                isFillermarked = null,
                markPrevRead = null,
                lastPageRead = null,
            )
        }

        return Triple(cbzStreamWithSize.first, baseName, cbzStreamWithSize.second)
    }

    fun getCbzMetadataForDownload(chapterId: Int): Pair<String, Long> { // fileName, fileSize
        val (chapterData, fileName) = transaction {
            val row =
                (ChapterTable innerJoin MangaTable)
                    .select(ChapterTable.columns + MangaTable.columns)
                    .where { ChapterTable.id eq chapterId }
                    .firstOrNull() ?: throw IllegalArgumentException("ChapterId $chapterId not found")
            val chapter = ChapterTable.toDataClass(row)
            val mangaTitle = row[MangaTable.title]
            val scanlatorPart = chapter.scanlator?.let { "[$it] " } ?: ""
            val fullName = "$mangaTitle - $scanlatorPart${chapter.name}.cbz"
            chapter to fullName
        }

        val fileSize = provider(chapterData.mangaId, chapterData.id).getArchiveSize()

        return Pair(fileName, fileSize)
    }
}

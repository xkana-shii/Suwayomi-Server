package suwayomi.tachidesk.manga.impl

import kotlinx.coroutines.CoroutineScope
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.manga.impl.chapter.getChapterDownloadReady
import suwayomi.tachidesk.manga.impl.download.fileProvider.ChaptersFilesProvider
import suwayomi.tachidesk.manga.impl.download.fileProvider.impl.ArchiveProvider
import suwayomi.tachidesk.manga.impl.download.fileProvider.impl.FolderProvider
import suwayomi.tachidesk.manga.impl.download.model.DownloadQueueItem
import suwayomi.tachidesk.manga.impl.util.getChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.getChapterDownloadPath
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterDownloadFolder
import suwayomi.tachidesk.manga.model.dataclass.ChapterDataClass
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.toDataClass
import suwayomi.tachidesk.server.serverConfig
import xyz.nulldev.androidcompat.util.SafePath
import java.io.File
import java.io.InputStream

object ChapterDownloadHelper {
    suspend fun getImage(
        mangaId: Int,
        chapterId: Int,
        index: Int,
    ): Pair<InputStream, String> = provider(mangaId, chapterId).getImage().execute(index)

    suspend fun getImageCount(
        mangaId: Int,
        chapterId: Int,
    ): Int = provider(mangaId, chapterId).getImageCount()

    suspend fun delete(
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

    // return the appropriate provider based on how the download was saved. For the logic is simple but will evolve when new types of downloads are available
    private suspend fun provider(
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

    suspend fun getArchiveStreamWithSize(
        mangaId: Int,
        chapterId: Int,
    ): Pair<InputStream, Long> = provider(mangaId, chapterId).getAsArchiveStream()

    suspend fun getChapterArchiveSize(
        mangaId: Int,
        chapterId: Int,
    ): Long = provider(mangaId, chapterId).getArchiveSize()

    private fun getChapterWithCbzFileName(chapterId: Int): Pair<ChapterDataClass, String> =
        transaction {
            val row =
                (ChapterTable innerJoin MangaTable)
                    .select(ChapterTable.columns + MangaTable.title)
                    .where { ChapterTable.id eq chapterId }
                    .firstOrNull() ?: throw IllegalArgumentException("ChapterId $chapterId not found")

            val chapter = ChapterTable.toDataClass(row)
            Pair(chapter, getCbzFileName(row[MangaTable.title], chapter))
        }

    fun getCbzFileName(
        mangaTitle: String,
        chapter: ChapterDataClass,
    ): String {
        val scanlatorName = chapter.scanlator?.trim()?.takeIf { it.isNotEmpty() }
        val chapterName =
            chapter.name
                .trim()
                .run { if (endsWith(".cbz", true)) substring(0, length - 4) else this }
                .takeIf { it.isNotEmpty() }

        val fileName =
            buildString {
                append(mangaTitle.trim())
                append(" - ")

                if (chapterName != null) {
                    append(chapterName)
                } else if (chapter.chapterNumber >= 0f) {
                    // chapterNumber is a Float, drop .0 for whole numbers
                    val formatNumber =
                        if (chapter.chapterNumber % 1 == 0f) {
                            chapter.chapterNumber.toInt().toString()
                        } else {
                            chapter.chapterNumber.toString()
                        }
                    append("#$formatNumber")
                } else {
                    // Fallback without name nor valid chapter number
                    append("#${chapter.index}")
                }

                if (scanlatorName != null) {
                    append(" [")
                    append(scanlatorName)
                    append("]")
                }
                append(".cbz")
            }

        // Sanitize filename for OS compatibility
        return SafePath.buildValidFilename(fileName)
    }

    suspend fun getCbzForDownload(
        userId: Int,
        chapterId: Int,
        markAsRead: Boolean?,
    ): Triple<InputStream, String, Long> {
        val (chapterData, fileName) = getChapterWithCbzFileName(chapterId)

        val cbzFile = provider(chapterData.mangaId, chapterData.id).getAsArchiveStream()

        if (markAsRead == true) {
            Chapter.modifyChapter(
                userId = userId,
                chapterData.mangaId,
                chapterData.index,
                isRead = true,
                isBookmarked = null,
                markPrevRead = null,
                lastPageRead = null,
            )
        }

        return Triple(cbzFile.first, fileName, cbzFile.second)
    }

    suspend fun getCbzMetadataForDownload(chapterId: Int): Pair<String, Long> { // fileName, fileSize
        val (chapterData, fileName) = getChapterWithCbzFileName(chapterId)

        val fileSize = provider(chapterData.mangaId, chapterData.id).getArchiveSize()

        return Pair(fileName, fileSize)
    }

    /**
     * Try to migrate/rename an existing downloaded chapter (either a folder or a cbz candidate)
     * to the canonical current naming scheme.
     *
     * Returns true if a migration/rename happened (or the desired file already exists).
     *
     * Note: This helper performs a filesystem rename. It does not recompute or inject ComicInfo
     * inside an existing CBZ. If you require updating the CBZ contents (e.g. to add/update ComicInfo.xml),
     * extract -> createComicInfoFile(...) -> re-create the CBZ will be necessary.
     */
    suspend fun migrateDownloadedChapterFilename(chapterId: Int): Boolean {
        // Fetch chapter + manga id
        val chapterRow =
            transaction {
                ChapterTable.select(ChapterTable.columns).where { ChapterTable.id eq chapterId }.firstOrNull()
            } ?: return false

        val mangaId = chapterRow[ChapterTable.manga].value

        val existingCbzPath = resolveExistingChapterCbzPath(mangaId, chapterId)
        val existingFolderPath = resolveExistingChapterDownloadFolder(mangaId, chapterId)

        val desiredCbzPath = getChapterCbzPath(mangaId, chapterId)
        val desiredFolderPath = getChapterDownloadPath(mangaId, chapterId)

        // If a CBZ candidate exists, prefer migrating that.
        if (!existingCbzPath.isNullOrBlank()) {
            val existingFile = File(existingCbzPath)
            val desiredFile = File(desiredCbzPath)
            if (!existingFile.exists()) return false
            if (existingFile.absolutePath == desiredFile.absolutePath) return true
            desiredFile.parentFile?.mkdirs()
            return existingFile.renameTo(desiredFile)
        }

        // Otherwise if a folder candidate exists, migrate it.
        if (!existingFolderPath.isNullOrBlank()) {
            val existingDir = File(existingFolderPath)
            val desiredDir = File(desiredFolderPath)
            if (!existingDir.exists()) return false
            if (existingDir.absolutePath == desiredDir.absolutePath) return true
            desiredDir.parentFile?.mkdirs()
            return existingDir.renameTo(desiredDir)
        }

        // Nothing found to migrate
        return false
    }
}

package suwayomi.tachidesk.manga.impl.download.fileProvider.impl

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.archivers.zip.ZipFile
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import suwayomi.tachidesk.manga.impl.download.fileProvider.ChaptersFilesProvider
import suwayomi.tachidesk.manga.impl.download.fileProvider.FileType
import suwayomi.tachidesk.manga.impl.util.getCandidateChapterDownloadPathsRelative
import suwayomi.tachidesk.manga.impl.util.getChapterCachePath
import suwayomi.tachidesk.manga.impl.util.getChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.getChapterDownloadPath
import suwayomi.tachidesk.manga.impl.util.getMangaDownloadDir
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterCbzPath
import suwayomi.tachidesk.manga.impl.util.resolveExistingChapterDownloadFolder
import suwayomi.tachidesk.manga.impl.util.storage.FileDeletionHelper
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.server.ApplicationDirs
import uy.kohesive.injekt.injectLazy
import java.io.File
import java.io.InputStream
import java.nio.file.NoSuchFileException
import java.util.zip.Deflater

private val applicationDirs: ApplicationDirs by injectLazy()

class ArchiveProvider(
    mangaId: Int,
    chapterId: Int,
) : ChaptersFilesProvider<FileType.ZipFile>(mangaId, chapterId) {
    // fork: downloads may still use the legacy (unhashed) chapter name
    private suspend fun existingOrCanonicalCbzPath(): String =
        resolveExistingChapterCbzPath(mangaId, chapterId) ?: getChapterCbzPath(mangaId, chapterId)

    override suspend fun getImageFiles(): List<FileType.ZipFile> {
        val cbzPath = existingOrCanonicalCbzPath()
        return try {
            ZipFile.builder().setFile(cbzPath).get().use { zipFile ->
                zipFile.entries
                    .toList()
                    .filter { !it.isDirectory }
                    .map { FileType.ZipFile(it) }
            }
        } catch (e: NoSuchFileException) {
            // "no download present" should throw NoSuchElementException
            throw NoSuchElementException("CBZ archive does not exist", e)
        }
    }

    override suspend fun getImageInputStream(image: FileType.ZipFile): InputStream =
        ZipFile
            .builder()
            .setFile(existingOrCanonicalCbzPath())
            .get()
            .getInputStream(image.entry)

    override suspend fun extractExistingDownload() {
        val outputFile = File(existingOrCanonicalCbzPath())
        val chapterDownloadFolder =
            File(resolveExistingChapterDownloadFolder(mangaId, chapterId) ?: getChapterDownloadPath(mangaId, chapterId))

        if (!outputFile.exists()) {
            return
        }

        extractCbzFile(outputFile, chapterDownloadFolder)
    }

    override suspend fun handleSuccessfulDownload() {
        val mangaDownloadFolder = File(getMangaDownloadDir(mangaId))
        val outputFile = File(getChapterCbzPath(mangaId, chapterId))
        val chapterCacheFolder = File(getChapterCachePath(mangaId, chapterId))

        withContext(Dispatchers.IO) {
            mangaDownloadFolder.mkdirs()
            outputFile.createNewFile()
        }

        ZipArchiveOutputStream(outputFile.outputStream()).use { zipOut ->
            zipOut.setMethod(ZipArchiveOutputStream.DEFLATED)
            zipOut.setLevel(Deflater.DEFAULT_COMPRESSION)
            if (chapterCacheFolder.isDirectory) {
                chapterCacheFolder.listFiles()?.sortedBy { it.name }?.forEach {
                    val entry = ZipArchiveEntry(it.name)
                    entry.time = 0L
                    try {
                        zipOut.putArchiveEntry(entry)
                        it.inputStream().use { inputStream ->
                            inputStream.copyTo(zipOut)
                        }
                    } finally {
                        zipOut.closeArchiveEntry()
                    }
                }
            }
        }

        if (chapterCacheFolder.exists() && chapterCacheFolder.isDirectory) {
            chapterCacheFolder.deleteRecursively()
        }
    }

    override suspend fun delete(): Boolean {
        // fork: delete every candidate name, otherwise a leftover legacy archive would still be picked up
        val cbzFiles =
            getCandidateChapterDownloadPathsRelative(mangaId, chapterId)
                .map { File(applicationDirs.mangaDownloadsRoot, "$it.cbz") }
                .filter { it.exists() }
        if (cbzFiles.isEmpty()) {
            return true
        }

        var cbzDeleted = true
        cbzFiles.forEach { cbzFile ->
            cbzDeleted = cbzFile.delete() && cbzDeleted
            FileDeletionHelper.cleanupParentFoldersFor(cbzFile, applicationDirs.mangaDownloadsRoot)
        }
        if (cbzDeleted) {
            transaction {
                ChapterTable.update({ ChapterTable.id eq chapterId }) {
                    it[koreaderHash] = null
                }
            }
        }
        return cbzDeleted
    }

    override suspend fun getAsArchiveStream(): Pair<InputStream, Long> {
        val cbzFile =
            File(existingOrCanonicalCbzPath())
                .takeIf { it.exists() }
                ?: throw IllegalArgumentException("CBZ file not found for chapter ID: $chapterId (Manga ID: $mangaId)")

        return cbzFile.inputStream() to cbzFile.length()
    }

    override suspend fun getArchiveSize(): Long {
        val cbzFile = File(existingOrCanonicalCbzPath())
        return if (cbzFile.exists()) cbzFile.length() else 0L
    }

    private fun extractCbzFile(
        cbzFile: File,
        chapterFolder: File,
    ) {
        if (!chapterFolder.exists()) chapterFolder.mkdirs()
        ZipArchiveInputStream(cbzFile.inputStream()).use { zipInputStream ->
            var zipEntry = zipInputStream.nextEntry
            while (zipEntry != null) {
                val file = File(chapterFolder, zipEntry.name)
                if (!file.exists()) {
                    file.parentFile.mkdirs()
                    file.createNewFile()
                }
                file.outputStream().use { outputStream ->
                    zipInputStream.copyTo(outputStream)
                }
                zipEntry = zipInputStream.nextEntry
            }
        }
        // fork: the archive is kept; it is overwritten by handleSuccessfulDownload, so a failed re-download does not lose it
    }
}

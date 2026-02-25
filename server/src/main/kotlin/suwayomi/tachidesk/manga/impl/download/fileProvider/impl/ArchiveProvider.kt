package suwayomi.tachidesk.manga.impl.download.fileProvider.impl

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.archivers.zip.ZipFile
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import suwayomi.tachidesk.manga.impl.download.fileProvider.ChaptersFilesProvider
import suwayomi.tachidesk.manga.impl.download.fileProvider.FileType
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
import java.io.FileInputStream
import java.io.InputStream
import java.util.zip.Deflater

private val applicationDirs: ApplicationDirs by injectLazy()

class ArchiveProvider(
    mangaId: Int,
    chapterId: Int,
) : ChaptersFilesProvider<FileType.ZipFile>(mangaId, chapterId) {
    override fun getImageFiles(): List<FileType.ZipFile> {
        // Try existing candidate cbz first
        val existingCbz = resolveExistingChapterCbzPath(mangaId, chapterId)
        val cbzPath = existingCbz ?: getChapterCbzPath(mangaId, chapterId)
        val cbzFile = File(cbzPath)
        val zipFile = ZipFile.builder().setFile(cbzFile).get()

        // Build a list of entries from the zip enumeration to avoid type inference issues
        val entries = mutableListOf<org.apache.commons.compress.archivers.zip.ZipArchiveEntry>()
        val enumer = zipFile.entries
        while (enumer.hasMoreElements()) {
            val e = enumer.nextElement() as org.apache.commons.compress.archivers.zip.ZipArchiveEntry
            entries.add(e)
        }
        return entries.map { FileType.ZipFile(it) }
    }

    override fun getImageInputStream(image: FileType.ZipFile): InputStream =
        ZipFile
            .builder()
            .setFile(File(resolveExistingChapterCbzPath(mangaId, chapterId) ?: getChapterCbzPath(mangaId, chapterId)))
            .get()
            .getInputStream(image.entry)

    override fun extractExistingDownload() {
        val existingCbz = resolveExistingChapterCbzPath(mangaId, chapterId)
        val outputFile = if (existingCbz != null) File(existingCbz) else File(getChapterCbzPath(mangaId, chapterId))
        val chapterDownloadFolder = File(resolveExistingChapterDownloadFolder(mangaId, chapterId) ?: getChapterDownloadPath(mangaId, chapterId))

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

        // Note: createComicInfoFile already executed in the download flow before this method,
        // so the cache folder should contain ComicInfo.xml if available.

        ZipArchiveOutputStream(outputFile.outputStream()).use { zipOut ->
            zipOut.setMethod(ZipArchiveOutputStream.DEFLATED)
            zipOut.setLevel(Deflater.DEFAULT_COMPRESSION)
            if (chapterCacheFolder.isDirectory) {
                chapterCacheFolder.listFiles()?.sortedBy { it.name }?.forEach { file ->
                    val entry = ZipArchiveEntry(file.name)
                    entry.time = 0L
                    try {
                        zipOut.putArchiveEntry(entry)
                        file.inputStream().use { inputStream ->
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

    override fun delete(): Boolean {
        val cbzFile = File(resolveExistingChapterCbzPath(mangaId, chapterId) ?: getChapterCbzPath(mangaId, chapterId))
        val deleted = if (cbzFile.exists()) {
            cbzFile.delete()
        } else {
            false
        }

        if (deleted) {
            transaction {
                ChapterTable.update({ ChapterTable.id eq chapterId }) {
                    it[koreaderHash] = null
                }
            }
            FileDeletionHelper.cleanupParentFoldersFor(cbzFile, applicationDirs.mangaDownloadsRoot)
        }

        return deleted
    }

    override fun getAsArchiveStream(): Pair<InputStream, Long> {
        val cbzPath = resolveExistingChapterCbzPath(mangaId, chapterId) ?: getChapterCbzPath(mangaId, chapterId)
        val file = File(cbzPath)
        if (!file.exists() || !file.isFile) {
            throw IllegalArgumentException("CBZ for chapter ID $chapterId not found")
        }
        return FileInputStream(file) to file.length()
    }

    override fun getArchiveSize(): Long {
        val cbzPath = resolveExistingChapterCbzPath(mangaId, chapterId) ?: getChapterCbzPath(mangaId, chapterId)
        val file = File(cbzPath)
        return if (file.exists() && file.isFile) file.length() else 0L
    }

    // Helper to extract cbz to folder
    private fun extractCbzFile(cbz: File, destDir: File) {
        destDir.mkdirs()
        ZipFile.builder().setFile(cbz).get().use { zip ->
            val enumer = zip.entries
            while (enumer.hasMoreElements()) {
                val entry = enumer.nextElement() as ZipArchiveEntry
                val file = File(destDir, entry.name)
                if (!entry.isDirectory) {
                    file.parentFile?.mkdirs()
                    zip.getInputStream(entry).use { input ->
                        file.outputStream().use { out ->
                            input.copyTo(out)
                        }
                    }
                }
            }
        }
    }
}

package suwayomi.tachidesk.manga.impl.util

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import eu.kanade.tachiyomi.util.lang.withIOContext
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.suspendTransaction
import suwayomi.tachidesk.manga.impl.util.source.GetSource
import suwayomi.tachidesk.manga.model.dataclass.ChapterDataClass
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.server.ApplicationDirs
import uy.kohesive.injekt.injectLazy
import xyz.nulldev.androidcompat.util.SafePath
import java.io.File
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.Path
import kotlin.io.path.isDirectory
import kotlin.io.path.isRegularFile
import kotlin.io.path.moveTo
import kotlin.io.path.notExists

private val applicationDirs: ApplicationDirs by injectLazy()

private val logger = KotlinLogging.logger { }

private fun getMangaDir(
    title: String,
    sourceName: String,
): String {
    val sourceDir = SafePath.buildValidFilename(sourceName)
    val mangaDir = SafePath.buildValidFilename(title)

    return "$sourceDir/$mangaDir"
}

private suspend fun getMangaDir(mangaId: Int): String =
    suspendTransaction {
        val mangaEntry = MangaTable.selectAll().where { MangaTable.id eq mangaId }.first()
        val source = GetSource.getSourceOrStub(mangaEntry[MangaTable.sourceReference])

        getMangaDir(mangaEntry[MangaTable.title], source.toString())
    }

/**
 * Truncate a string safely to a maximum number of bytes in UTF-8,
 * preserving valid codepoint boundaries.
 */
private fun truncateToBytes(
    s: String,
    maxBytes: Int,
    charset: Charset = StandardCharsets.UTF_8,
): String {
    val bytes = s.toByteArray(charset)
    if (bytes.size <= maxBytes) return s

    // Use decoder to avoid cutting in the middle of a multi-byte character
    val decoder = charset.newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    decoder.onUnmappableCharacter(CodingErrorAction.IGNORE)

    val bb = ByteBuffer.wrap(bytes, 0, maxBytes)
    val cb = CharBuffer.allocate(maxBytes)
    decoder.decode(bb, cb, true)
    decoder.flush(cb)
    return String(cb.array(), 0, cb.position())
}

/** Return hex digest (lowercase) for MD5 of input. */
private fun md5Hex(input: String): String {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(input.toByteArray(StandardCharsets.UTF_8))
    return digest.joinToString("") { "%02x".format(it) }
}

// Reserve bytes for "_" + 6-hex-hash + ".cbz" => underscore (1) + 6 hex chars (6) + ".cbz" (4) = 11 bytes
private const val CHAPTER_DIR_RESERVED_BYTES = 11
private const val CHAPTER_DIR_MAX_FILE_NAME_BYTES = 240 // conservative safe truncation target

/** Name used before the fork switched to hashed names; still resolved so old downloads keep working. */
private fun legacyChapterDirName(
    title: String,
    scanlator: String?,
): String =
    SafePath.buildValidFilename(
        when {
            scanlator != null -> "${scanlator}_$title"
            else -> title
        },
    )

/**
 * Build the chapter directory name with Mihon-style disambiguator:
 *  - Base: maybe "<scanlator>_<chapter name>" (sanitized)
 *  - Truncate base so final filename bytes <= MAX_FILE_NAME_BYTES minus reserved (hash + underscore + ".cbz")
 *  - Append "_" + md5(chapterUrl).take(6)
 *
 * Without a chapter url the legacy (unhashed) name is used.
 */
private fun chapterDirName(
    title: String,
    scanlator: String?,
    url: String?,
): String {
    if (url.isNullOrBlank()) {
        return legacyChapterDirName(title, scanlator)
    }

    val sanitizedBase = legacyChapterDirName(title, scanlator)
    val allowedBaseBytes = (CHAPTER_DIR_MAX_FILE_NAME_BYTES - CHAPTER_DIR_RESERVED_BYTES).coerceAtLeast(1)
    val truncatedBase = truncateToBytes(sanitizedBase, allowedBaseBytes)

    // Final sanitize just in case
    return SafePath.buildValidFilename("${truncatedBase}_${md5Hex(url).take(6)}")
}

private suspend fun getChapterDir(
    mangaId: Int,
    title: String,
    scanlator: String?,
    url: String? = null,
): String = getMangaDir(mangaId) + "/" + chapterDirName(title, scanlator, url)

private suspend fun getChapterDir(
    mangaId: Int,
    chapterId: Int,
): String =
    suspendTransaction {
        val chapterEntry = ChapterTable.selectAll().where { ChapterTable.id eq chapterId }.first()

        // Get manga directory and combine with chapter directory
        // Note: This creates a nested transaction, but Exposed handles this with useNestedTransactions=true
        getChapterDir(
            mangaId,
            chapterEntry[ChapterTable.name],
            chapterEntry[ChapterTable.scanlator],
            chapterEntry[ChapterTable.url],
        )
    }

/**
 * Candidate (possibly-existing) relative download paths for a chapter within the downloads root.
 * First element is the canonical hashed name (current format). Next element is the legacy name without hash.
 */
private suspend fun getCandidateChapterDirs(
    mangaId: Int,
    title: String,
    scanlator: String?,
    url: String?,
): List<String> {
    val mangaDir = getMangaDir(mangaId)
    return listOf(
        mangaDir + "/" + chapterDirName(title, scanlator, url),
        mangaDir + "/" + legacyChapterDirName(title, scanlator),
    ).distinct()
}

suspend fun getCandidateChapterDownloadPathsRelative(
    mangaId: Int,
    chapterId: Int,
): List<String> =
    suspendTransaction {
        val chapterEntry = ChapterTable.selectAll().where { ChapterTable.id eq chapterId }.first()
        getCandidateChapterDirs(
            mangaId,
            chapterEntry[ChapterTable.name],
            chapterEntry[ChapterTable.scanlator],
            chapterEntry[ChapterTable.url],
        )
    }

/**
 * Resolve first existing folder among candidates and return absolute path, else null.
 */
suspend fun resolveExistingChapterDownloadFolder(
    mangaId: Int,
    chapterId: Int,
): String? =
    getCandidateChapterDownloadPathsRelative(mangaId, chapterId)
        .map { File(applicationDirs.mangaDownloadsRoot, it) }
        .firstOrNull { it.exists() && it.isDirectory }
        ?.absolutePath

/**
 * Resolve first existing CBZ among candidates and return absolute path, else null.
 */
suspend fun resolveExistingChapterCbzPath(
    mangaId: Int,
    chapterId: Int,
): String? =
    getCandidateChapterDownloadPathsRelative(mangaId, chapterId)
        .map { File(applicationDirs.mangaDownloadsRoot, "$it.cbz") }
        .firstOrNull { it.exists() && it.isFile }
        ?.absolutePath

fun getThumbnailDownloadPath(mangaId: Int): String = applicationDirs.thumbnailDownloadsRoot + "/$mangaId"

fun getMangaDownloadDir(
    title: String,
    sourceName: String,
): String = applicationDirs.mangaDownloadsRoot + "/" + getMangaDir(title, sourceName)

suspend fun getMangaDownloadDir(mangaId: Int): String = applicationDirs.mangaDownloadsRoot + "/" + getMangaDir(mangaId)

fun getMangaCacheDir(
    title: String,
    sourceName: String,
): String = applicationDirs.tempMangaCacheRoot + "/" + getMangaDir(title, sourceName)

suspend fun getChapterDownloadPath(
    mangaId: Int,
    title: String,
    scanlator: String?,
    url: String? = null,
): String = applicationDirs.mangaDownloadsRoot + "/" + getChapterDir(mangaId, title, scanlator, url)

suspend fun getChapterDownloadPath(
    mangaId: Int,
    chapterId: Int,
): String = applicationDirs.mangaDownloadsRoot + "/" + getChapterDir(mangaId, chapterId)

suspend fun getChapterCbzPath(
    mangaId: Int,
    title: String,
    scanlator: String?,
    url: String? = null,
): String = getChapterDownloadPath(mangaId, title, scanlator, url) + ".cbz"

suspend fun getChapterCbzPath(
    mangaId: Int,
    chapterId: Int,
): String = getChapterDownloadPath(mangaId, chapterId) + ".cbz"

suspend fun getChapterCachePath(
    mangaId: Int,
    title: String,
    scanlator: String?,
    url: String? = null,
): String = applicationDirs.tempMangaCacheRoot + "/" + getChapterDir(mangaId, title, scanlator, url)

suspend fun getChapterCachePath(
    mangaId: Int,
    chapterId: Int,
): String = applicationDirs.tempMangaCacheRoot + "/" + getChapterDir(mangaId, chapterId)

private fun updateDir(
    currentDir: String,
    newDir: String,
): Boolean {
    val currentDirPath = Path(currentDir)
    val newDirPath = Path(newDir)

    if (currentDirPath.notExists()) {
        return true
    }

    return try {
        currentDirPath.moveTo(newDirPath)

        if (newDirPath.notExists()) {
            return false
        }

        return true
    } catch (e: Exception) {
        logger.error(e) { "updateDownloadDir: failed to rename download folder from \"$currentDir\" to \"$newDir\"" }
        false
    }
}

private fun updateDownloadDir(
    currentDownloadDir: String,
    newDownloadDir: String,
    currentCacheDir: String,
    newCacheDir: String,
): Boolean {
    val renamed = updateDir(currentDownloadDir, newDownloadDir)

    val tryToKeepCachedFilesUsable = renamed
    if (tryToKeepCachedFilesUsable) {
        updateDir(currentCacheDir, newCacheDir)
    }

    return renamed
}

private val mutexByManga = ConcurrentHashMap<Int, Mutex>()

suspend fun updateChapterDownloadDir(
    oldChapter: ChapterDataClass,
    newChapter: ChapterDataClass,
): Boolean {
    if (oldChapter.mangaId != newChapter.mangaId) {
        return false
    }

    return mutexByManga.getOrPut(oldChapter.mangaId) { Mutex() }.withLock {
        // fork: the existing download may still use the legacy (unhashed) name, move it to the current hashed name
        val oldCandidates =
            getCandidateChapterDirs(oldChapter.mangaId, oldChapter.name, oldChapter.scanlator, oldChapter.url)
                .map { applicationDirs.mangaDownloadsRoot + "/" + it }
        val currentDownloadDir = oldCandidates.firstOrNull { Path(it).isDirectory() } ?: oldCandidates.first()
        val newDownloadDir = getChapterDownloadPath(oldChapter.mangaId, newChapter.name, newChapter.scanlator, newChapter.url)
        val currentDownloadCbz = oldCandidates.map { "$it.cbz" }.firstOrNull { Path(it).isRegularFile() } ?: "${oldCandidates.first()}.cbz"
        val newDownloadCbz = getChapterCbzPath(oldChapter.mangaId, newChapter.name, newChapter.scanlator, newChapter.url)

        val currentCacheDir = getChapterCachePath(oldChapter.mangaId, oldChapter.name, oldChapter.scanlator, oldChapter.url)
        val newCacheDir = getChapterCachePath(oldChapter.mangaId, newChapter.name, newChapter.scanlator, newChapter.url)

        withIOContext {
            val dirRename = updateDownloadDir(currentDownloadDir, newDownloadDir, currentCacheDir, newCacheDir)
            val cbzRename = updateDownloadDir(currentDownloadCbz, newDownloadCbz, currentCacheDir, newCacheDir)

            dirRename && cbzRename
        }
    }
}

/** return value says if rename/move was successful */
suspend fun updateMangaDownloadDir(
    mangaId: Int,
    title: String,
    sourceName: String,
    newTitle: String,
): Boolean =
    mutexByManga.getOrPut(mangaId) { Mutex() }.withLock {
        val currentDownloadDir = getMangaDownloadDir(title, sourceName)
        val newDownloadDir = getMangaDownloadDir(newTitle, sourceName)

        val currentCacheDir = getMangaCacheDir(title, sourceName)
        val newCacheDir = getMangaCacheDir(newTitle, sourceName)

        withIOContext {
            updateDownloadDir(currentDownloadDir, newDownloadDir, currentCacheDir, newCacheDir)
        }
    }

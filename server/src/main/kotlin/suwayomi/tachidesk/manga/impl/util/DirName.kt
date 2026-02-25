package suwayomi.tachidesk.manga.impl.util

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import suwayomi.tachidesk.manga.impl.util.source.GetCatalogueSource
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
import kotlin.math.max

private val applicationDirs: ApplicationDirs by injectLazy()

private val logger = KotlinLogging.logger { }

/**
 * Truncate a string safely to a maximum number of bytes in UTF-8,
 * preserving valid codepoint boundaries.
 */
private fun truncateToBytes(s: String, maxBytes: Int, charset: Charset = StandardCharsets.UTF_8): String {
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

private fun getMangaDir(mangaId: Int): String =
    transaction {
        val mangaEntry = MangaTable.selectAll().where { MangaTable.id eq mangaId }.first()
        val source = GetCatalogueSource.getCatalogueSourceOrStub(mangaEntry[MangaTable.sourceReference])

        val sourceDir = SafePath.buildValidFilename(source.toString())
        val mangaDir = SafePath.buildValidFilename(mangaEntry[MangaTable.title])
        "$sourceDir/$mangaDir"
    }

/**
 * Build the chapter directory name with Mihon-style disambiguator:
 *  - Base: maybe "<scanlator>_<chapter name>" (sanitized)
 *  - Truncate base so final filename bytes <= MAX_FILE_NAME_BYTES minus reserved (hash + underscore + ".cbz")
 *  - Append "_" + md5(chapterUrl).take(6)
 *
 * This mirrors Mihon's approach of using an MD5-based short hash of the chapter URL as the disambiguator.
 */
private fun getChapterDir(
    mangaId: Int,
    chapterId: Int,
): String =
    transaction {
        val chapterEntry = ChapterTable.selectAll().where { ChapterTable.id eq chapterId }.first()

        // Build base name (scanlator prefix if present)
        val base = when {
            chapterEntry[ChapterTable.scanlator] != null -> "${chapterEntry[ChapterTable.scanlator]}_${chapterEntry[ChapterTable.name]}"
            else -> chapterEntry[ChapterTable.name]
        }

        // Sanitize base name first
        val sanitizedBase = SafePath.buildValidFilename(base)

        // Reserve bytes for "_" + 6-hex-hash + ".cbz" => underscore (1) + 6 hex chars (6) + ".cbz" (4) = 11 bytes
        val RESERVED_BYTES = 11
        val MAX_FILE_NAME_BYTES = 240 // conservative safe truncation target
        val allowedBaseBytes = (MAX_FILE_NAME_BYTES - RESERVED_BYTES).coerceAtLeast(1)

        // Truncate the sanitized base by bytes to allowed length
        val truncatedBase = truncateToBytes(sanitizedBase, allowedBaseBytes)

        // Compute disambiguator from chapter URL if present, else fallback to chapterId
        val url = chapterEntry[ChapterTable.url]
        val disambiguator = if (!url.isNullOrBlank()) {
            md5Hex(url).take(6)
        } else {
            chapterId.toString()
        }

        val chapterDirName = "${truncatedBase}_$disambiguator"
        // Final sanitize just in case
        val finalName = SafePath.buildValidFilename(chapterDirName)

        getMangaDir(mangaId) + "/$finalName"
    }

/**
 * Return a list of candidate (possibly-existing) relative download paths for a chapter within the manga folder.
 * First element is the canonical hashed name (current format). Next element is the legacy name without hash.
 */
fun getCandidateChapterDownloadPathsRelative(mangaId: Int, chapterId: Int): List<String> =
    transaction {
        val chapterEntry = ChapterTable.selectAll().where { ChapterTable.id eq chapterId }.first()
        val name = chapterEntry[ChapterTable.name]
        val scanlator = chapterEntry[ChapterTable.scanlator]

        // Current (hashed) relative path (source/manga/<hashed>)
        val canonical = getChapterDir(mangaId, chapterId)

        // Legacy: sanitized base without hash
        val baseLegacy =
            SafePath.buildValidFilename(
                when {
                    scanlator != null -> "${scanlator}_$name"
                    else -> name
                },
            )
        val legacy = getMangaDir(mangaId) + "/$baseLegacy"

        listOf(canonical, legacy)
    }

/**
 * Resolve first existing folder among candidates and return absolute path, else null.
 */
fun resolveExistingChapterDownloadFolder(mangaId: Int, chapterId: Int): String? {
    val candidates = getCandidateChapterDownloadPathsRelative(mangaId, chapterId)
    for (rel in candidates) {
        val f = File(applicationDirs.mangaDownloadsRoot, rel)
        if (f.exists() && f.isDirectory) return f.absolutePath
    }
    return null
}

/**
 * Resolve first existing CBZ among candidates and return absolute path, else null.
 */
fun resolveExistingChapterCbzPath(mangaId: Int, chapterId: Int): String? {
    val candidates = getCandidateChapterDownloadPathsRelative(mangaId, chapterId)
    for (rel in candidates) {
        val f = File(applicationDirs.mangaDownloadsRoot, "$rel.cbz")
        if (f.exists() && f.isFile) return f.absolutePath
    }
    return null
}

fun getThumbnailDownloadPath(mangaId: Int): String = applicationDirs.thumbnailDownloadsRoot + "/$mangaId"

fun getMangaDownloadDir(mangaId: Int): String = applicationDirs.mangaDownloadsRoot + "/" + getMangaDir(mangaId)

fun getChapterDownloadPath(
    mangaId: Int,
    chapterId: Int,
): String = applicationDirs.mangaDownloadsRoot + "/" + getChapterDir(mangaId, chapterId)

fun getChapterCbzPath(
    mangaId: Int,
    chapterId: Int,
): String = getChapterDownloadPath(mangaId, chapterId) + ".cbz"

fun getChapterCachePath(
    mangaId: Int,
    chapterId: Int,
): String = applicationDirs.tempMangaCacheRoot + "/" + getChapterDir(mangaId, chapterId)

/** return value says if rename/move was successful */
fun updateMangaDownloadDir(
    mangaId: Int,
    newTitle: String,
): Boolean {
    // Get current manga directory (uses its own transaction)
    val currentMangaDir = getMangaDir(mangaId)

    // Build new directory path
    val newMangaDir =
        transaction {
            val mangaEntry = MangaTable.selectAll().where { MangaTable.id eq mangaId }.first()
            val source = GetCatalogueSource.getCatalogueSourceOrStub(mangaEntry[MangaTable.sourceReference])
            val sourceDir = SafePath.buildValidFilename(source.toString())
            val newMangaDirName = SafePath.buildValidFilename(newTitle)
            "$sourceDir/$newMangaDirName"
        }

    val oldPath = File(applicationDirs.mangaDownloadsRoot, currentMangaDir)
    val newPath = File(applicationDirs.mangaDownloadsRoot, newMangaDir)
    return try {
        if (oldPath.exists()) {
            java.nio.file.Files.move(oldPath.toPath(), newPath.toPath())
            true
        } else {
            true
        }
    } catch (e: Exception) {
        logger.error(e) { "updateMangaDownloadDir: failed to rename manga download folder from \"$oldPath\" to \"$newPath\"" }
        false
    }
}

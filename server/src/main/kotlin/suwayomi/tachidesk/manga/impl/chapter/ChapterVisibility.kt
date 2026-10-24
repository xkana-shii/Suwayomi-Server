package suwayomi.tachidesk.manga.impl.chapter

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import kotlin.math.roundToInt

/*
 * fork: chapter blacklist and Smart Scanlator Merge, ported from the Komikku fork
 * (domain/.../chapter/service/ChapterBlacklist.kt and domain/.../chapter/model/ChapterSmartMerge.kt).
 *
 * The rules are pure functions over a chapter snapshot. The server stores their result per user as
 * CHAPTERUSER.BLACKLISTED / MERGE_HIDDEN (see ChapterVisibilityService), so SQL counts, the next-chapter queries,
 * Updates and downloads all see the same visibility. Hidden chapters are never deleted.
 */

/** The chapter fields the visibility rules read. */
data class VisibilityChapter(
    val id: Int,
    val url: String,
    val name: String,
    val chapterNumber: Double,
    val scanlator: String?,
    /** Suwayomi's source order: higher = earlier in the source's list */
    val sourceOrder: Int,
)

data class ChapterVisibilityResult(
    val blacklistedIds: Set<Int>,
    /** duplicates Smart Scanlator Merge does not show; never contains blacklisted chapters */
    val mergeHiddenIds: Set<Int>,
)

object ChapterBlacklist {
    private const val MAX_KEY_SCANLATOR_LENGTH = 64
    private const val MAX_KEY_NAME_LENGTH = 256
    private const val URL_KEY_PREFIX = "url:"
    private const val HYBRID_KEY_PREFIX = "hybrid:"
    private const val HYBRID_KEY_V2_PREFIX = "${HYBRID_KEY_PREFIX}v2:"

    private val whitespace = Regex("\\s+")

    /** Komikku's legacy per-number-and-scanlator key, only resolved when it matches exactly one chapter. */
    fun legacyKey(
        chapterNumber: Double,
        scanlator: String?,
    ): String {
        val rounded = (chapterNumber * 100).roundToInt() / 100.0
        return "$rounded@${scanlator.orEmpty().trim().take(MAX_KEY_SCANLATOR_LENGTH)}"
    }

    /** An exact key: the chapter's source URL (commas and percent signs escaped). */
    fun urlKey(url: String): String = URL_KEY_PREFIX + url.replace("%", "%25").replace(",", "%2C")

    /**
     * The key new blacklist entries get: the URL plus number, scanlator and name, so the entry can be recovered when
     * the source changes the URL, without ever guessing between several candidates.
     */
    fun hybridKey(chapter: VisibilityChapter): String {
        val number = normalizedNumber(chapter.chapterNumber).orEmpty()
        return HYBRID_KEY_V2_PREFIX +
            listOf(
                number,
                normalizedScanlator(chapter.scanlator),
                chapter.url,
                normalizedName(chapter.name),
            ).joinToString("|", transform = ::encodeComponent)
    }

    private fun encodeComponent(value: String): String = value.replace("%", "%25").replace(",", "%2C").replace("|", "%7C")

    private fun decodeComponent(value: String): String = value.replace("%7C", "|").replace("%2C", ",").replace("%25", "%")

    private fun normalizedNumber(chapterNumber: Double): String? {
        if (!chapterNumber.isFinite() || chapterNumber <= 0.0) return null
        return ((chapterNumber * 100).roundToInt() / 100.0).toString()
    }

    private fun normalizedScanlator(scanlator: String?): String =
        scanlator
            .orEmpty()
            .trim()
            .lowercase()
            .take(MAX_KEY_SCANLATOR_LENGTH)

    private fun normalizedName(name: String): String =
        name
            .trim()
            .lowercase()
            .replace(whitespace, " ")
            .take(MAX_KEY_NAME_LENGTH)

    private data class HybridEntry(
        val number: String?,
        val scanlator: String,
        val url: String,
        val name: String?,
    )

    private fun parseHybrid(key: String): HybridEntry? =
        when {
            key.startsWith(HYBRID_KEY_V2_PREFIX) -> {
                val values = key.removePrefix(HYBRID_KEY_V2_PREFIX).split('|', limit = 4)
                if (values.size != 4) {
                    null
                } else {
                    HybridEntry(
                        number = values[0].takeIf(String::isNotEmpty),
                        scanlator = decodeComponent(values[1]),
                        url = decodeComponent(values[2]),
                        name = decodeComponent(values[3]).takeIf(String::isNotEmpty),
                    )
                }
            }

            key.startsWith(HYBRID_KEY_PREFIX) -> {
                val values = key.removePrefix(HYBRID_KEY_PREFIX).split('|', limit = 3)
                if (values.size != 3) {
                    null
                } else {
                    HybridEntry(
                        number = values[0].takeIf(String::isNotEmpty),
                        scanlator = decodeComponent(values[1]),
                        url = decodeComponent(values[2]),
                        name = null,
                    )
                }
            }

            else -> {
                null
            }
        }

    /**
     * The chapters a stored key refers to in [chapters]. URL keys match their URL only; hybrid keys prefer the exact
     * URL, then a unique name match, then a unique number-and-scanlator match; legacy keys only a unique match.
     */
    fun resolve(
        key: String,
        chapters: List<VisibilityChapter>,
    ): List<VisibilityChapter> {
        if (key.startsWith(URL_KEY_PREFIX)) return chapters.filter { urlKey(it.url) == key }

        val hybrid = parseHybrid(key)
        if (hybrid != null) {
            val exact = chapters.filter { it.url == hybrid.url }
            if (exact.isNotEmpty()) return exact
            val metadataMatches =
                chapters.filter {
                    (hybrid.number == null || normalizedNumber(it.chapterNumber) == hybrid.number) &&
                        normalizedScanlator(it.scanlator) == hybrid.scanlator
                }
            val nameMatches = hybrid.name?.let { name -> metadataMatches.filter { normalizedName(it.name) == name } }
            if (!nameMatches.isNullOrEmpty()) return nameMatches.takeIf { it.size == 1 }.orEmpty()
            if (hybrid.number == null) return emptyList()
            return metadataMatches.takeIf { it.size == 1 }.orEmpty()
        }

        return chapters.filter { legacyKey(it.chapterNumber, it.scanlator) == key }.takeIf { it.size == 1 }.orEmpty()
    }

    fun blacklistedIds(
        chapters: List<VisibilityChapter>,
        blacklist: Collection<String>,
    ): Set<Int> {
        if (chapters.isEmpty() || blacklist.isEmpty()) return emptySet()
        return blacklist.flatMapTo(HashSet()) { key -> resolve(key, chapters).map { it.id } }
    }

    /** The keys of [blacklist] that refer to any of [chapters]. */
    fun keysMatching(
        chapters: List<VisibilityChapter>,
        targets: Collection<VisibilityChapter>,
        blacklist: Collection<String>,
    ): Set<String> {
        val targetIds = targets.mapTo(HashSet()) { it.id }
        return blacklist.filterTo(LinkedHashSet()) { key -> resolve(key, chapters).any { it.id in targetIds } }
    }

    /** (chapter number, scanlator) of a key, to describe entries that no longer match a chapter */
    fun keyMetadata(key: String): Pair<Double?, String?> {
        if (key.startsWith(URL_KEY_PREFIX)) return null to null
        parseHybrid(key)?.let { hybrid ->
            return hybrid.number?.toDoubleOrNull() to hybrid.scanlator.takeIf(String::isNotEmpty)
        }
        val number = key.substringBefore('@').toDoubleOrNull()?.takeIf { it.isFinite() && it > 0.0 }
        val scanlator = key.substringAfter('@', "").trim().takeIf(String::isNotEmpty)
        return number to scanlator
    }
}

data class ScanlatorRangeRule(
    val from: Double,
    val to: Double,
    val scanlator: String,
) {
    fun covers(chapterNumber: Double): Boolean = chapterNumber in from..to

    fun encode(): String = "$from:$to:$scanlator"

    companion object {
        fun parse(raw: String): ScanlatorRangeRule? {
            val parts = raw.split(':', limit = 3)
            if (parts.size != 3) return null
            val from = parts[0].toDoubleOrNull() ?: return null
            val to = parts[1].toDoubleOrNull() ?: return null
            if (!from.isFinite() || !to.isFinite() || from > to) return null
            return ScanlatorRangeRule(from, to, parts[2])
        }
    }
}

object SmartScanlatorMerge {
    private const val MONTHS =
        "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|" +
            "sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"

    private val noticeName =
        Regex(
            """\b(?:notice|announcement|hiatus|anniversary|comeback|delay(?:ed)?|break)\b""" +
                """|\b(?:$MONTHS)\.? *[0-9]{1,2}(?:st|nd|rd|th)?\b|\b[0-9]{1,2}/[0-9]{1,2}\b""",
            RegexOption.IGNORE_CASE,
        )
    private val chapterPrefix = Regex("""\b(?:ch|chap|chapter|ep|episode)\.? *[0-9]|#[0-9]""", RegexOption.IGNORE_CASE)

    /** An announcement/hiatus/dated post: its number says nothing about which chapter it is, so it is never merged. */
    fun isNotice(name: String): Boolean = noticeName.containsMatchIn(name) && !chapterPrefix.containsMatchIn(name)

    private fun isMergeable(chapter: VisibilityChapter) =
        chapter.chapterNumber.isFinite() && chapter.chapterNumber > 0.0 && !isNotice(chapter.name)

    /**
     * The ids of [chapters] Smart Scanlator Merge shows: per chapter number the preferred scanlator (the last range
     * rule covering the number, else the first in [priority], else the first in source order) keeps all its
     * uploads; unnumbered chapters and notices always stay.
     */
    fun visibleIds(
        chapters: List<VisibilityChapter>,
        priority: List<String>,
        rangeRules: List<ScanlatorRangeRule>,
    ): Set<Int> {
        val priorityIndex = priority.withIndex().associate { (index, scanlator) -> scanlator to index }
        val selectedIds =
            chapters
                .sortedByDescending { it.sourceOrder }
                .filter(::isMergeable)
                .groupBy { it.chapterNumber }
                .values
                .flatMap { candidates ->
                    val number = candidates.first().chapterNumber
                    val preferredScanlator = rangeRules.lastOrNull { it.covers(number) }?.scanlator
                    val chosen =
                        candidates.minWithOrNull(
                            compareBy<VisibilityChapter> { if (it.scanlator == preferredScanlator) 0 else 1 }
                                .thenBy { priorityIndex[it.scanlator] ?: Int.MAX_VALUE },
                        ) ?: return@flatMap emptyList()
                    candidates.filter { it.scanlator == chosen.scanlator }.map { it.id }
                }.toSet()

        return chapters.filter { !isMergeable(it) || it.id in selectedIds }.mapTo(HashSet()) { it.id }
    }
}

object ChapterVisibility {
    fun compute(
        chapters: List<VisibilityChapter>,
        settings: ScanlatorSettings,
        smartMergeEnabled: Boolean,
    ): ChapterVisibilityResult {
        val blacklisted = ChapterBlacklist.blacklistedIds(chapters, settings.blacklistedChapters)
        if (!smartMergeEnabled) return ChapterVisibilityResult(blacklisted, emptySet())

        val unblacklisted = chapters.filter { it.id !in blacklisted }
        val visible =
            SmartScanlatorMerge.visibleIds(
                unblacklisted,
                settings.scanlatorPriority,
                settings.scanlatorRangeRules.mapNotNull(ScanlatorRangeRule::parse),
            )
        return ChapterVisibilityResult(
            blacklistedIds = blacklisted,
            mergeHiddenIds = unblacklisted.mapNotNullTo(HashSet()) { it.id.takeIf { id -> id !in visible } },
        )
    }

    /**
     * The saved priority narrowed to [available] scanlators first, then new scanlators alphabetically: a scanlator
     * that appears later never moves the ones the user ordered.
     */
    fun effectivePriority(
        stored: List<String>,
        available: Collection<String>,
    ): List<String> {
        val availableSet = available.toSet()
        val known = stored.filter { it in availableSet }.distinct()
        val new = availableSet.filterNot { it in known }.sortedWith(String.CASE_INSENSITIVE_ORDER)
        return known + new
    }

    /**
     * Applies an [edited] order of the scanlators the user saw to the [stored] priority: stored scanlators that were
     * not shown (e.g. a scanlator currently without chapters) keep their positions.
     */
    fun mergePriority(
        stored: List<String>,
        edited: List<String>,
    ): List<String> {
        val editedSet = edited.toSet()
        val queue = ArrayDeque(edited.distinct())
        val result = mutableListOf<String>()
        stored.distinct().forEach { scanlator ->
            if (scanlator in editedSet) {
                queue.removeFirstOrNull()?.let(result::add)
            } else {
                result += scanlator
            }
        }
        result += queue
        return result
    }
}

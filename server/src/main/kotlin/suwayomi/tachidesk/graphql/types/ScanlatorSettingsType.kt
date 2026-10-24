package suwayomi.tachidesk.graphql.types

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import com.expediagroup.graphql.generator.annotations.GraphQLDescription
import suwayomi.tachidesk.manga.impl.chapter.ChapterBlacklist
import suwayomi.tachidesk.manga.impl.chapter.ChapterVisibility
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorRangeRule
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorSettings
import suwayomi.tachidesk.manga.impl.chapter.VisibilityChapter

/** fork (Komikku port): Smart Scanlator Merge mode of a manga */
enum class SmartMergeMode {
    /** follow the user's global "smartScanlatorMerge" setting */
    GLOBAL,
    ENABLED,
    DISABLED,
    ;

    val override: Boolean?
        get() =
            when (this) {
                GLOBAL -> null
                ENABLED -> true
                DISABLED -> false
            }

    companion object {
        fun of(override: Boolean?): SmartMergeMode =
            when (override) {
                null -> GLOBAL
                true -> ENABLED
                false -> DISABLED
            }
    }
}

data class ScanlatorRangeRuleType(
    val from: Double,
    val to: Double,
    val scanlator: String,
)

data class BlacklistEntryType(
    @GraphQLDescription("The stored key; pass it to updateMangaScanlatorSettings.removeBlacklistEntries")
    val key: String,
    @GraphQLDescription("Current chapters the entry hides (empty when the chapter is gone from the source)")
    val chapterIds: List<Int>,
    val chapterNames: List<String>,
    @GraphQLDescription("Number and scanlator recorded in the key, to describe entries without a current chapter")
    val chapterNumber: Double?,
    val scanlator: String?,
)

@GraphQLDescription("fork (Komikku port): the user's scanlator rules of a manga")
data class MangaScanlatorSettingsType(
    val mangaId: Int,
    @GraphQLDescription("The saved priority narrowed to the manga's scanlators, followed by new scanlators alphabetically")
    val scanlatorPriority: List<String>,
    val scanlatorRangeRules: List<ScanlatorRangeRuleType>,
    val smartMergeMode: SmartMergeMode,
    @GraphQLDescription("Whether Smart Scanlator Merge applies to this manga (its mode or the global setting)")
    val smartMergeEnabled: Boolean,
    val blacklistedChapters: List<BlacklistEntryType>,
) {
    companion object {
        fun of(
            mangaId: Int,
            settings: ScanlatorSettings,
            chapters: List<VisibilityChapter>,
            globalSmartMerge: Boolean,
        ): MangaScanlatorSettingsType =
            MangaScanlatorSettingsType(
                mangaId = mangaId,
                scanlatorPriority =
                    ChapterVisibility.effectivePriority(
                        settings.scanlatorPriority,
                        chapters.mapNotNull { it.scanlator }.distinct(),
                    ),
                scanlatorRangeRules =
                    settings.scanlatorRangeRules
                        .mapNotNull(ScanlatorRangeRule::parse)
                        .map { ScanlatorRangeRuleType(it.from, it.to, it.scanlator) },
                smartMergeMode = SmartMergeMode.of(settings.smartMergeOverride),
                smartMergeEnabled = settings.smartMergeEnabled(globalSmartMerge),
                blacklistedChapters =
                    settings.blacklistedChapters.map { key ->
                        val matches = ChapterBlacklist.resolve(key, chapters)
                        val (number, scanlator) = ChapterBlacklist.keyMetadata(key)
                        BlacklistEntryType(
                            key = key,
                            chapterIds = matches.map { it.id },
                            chapterNames = matches.map { it.name },
                            chapterNumber = matches.firstOrNull()?.chapterNumber ?: number,
                            scanlator = matches.firstOrNull()?.scanlator ?: scanlator,
                        )
                    },
            )
    }
}

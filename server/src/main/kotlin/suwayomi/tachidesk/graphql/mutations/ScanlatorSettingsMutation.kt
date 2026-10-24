package suwayomi.tachidesk.graphql.mutations

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import com.expediagroup.graphql.generator.annotations.GraphQLDescription
import com.expediagroup.graphql.generator.annotations.GraphQLIgnore
import graphql.schema.DataFetchingEnvironment
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.graphql.directives.RequireAuth
import suwayomi.tachidesk.graphql.types.MangaScanlatorSettingsType
import suwayomi.tachidesk.graphql.types.MangaType
import suwayomi.tachidesk.graphql.types.SmartMergeMode
import suwayomi.tachidesk.manga.impl.chapter.ChapterVisibility
import suwayomi.tachidesk.manga.impl.chapter.ChapterVisibilityService
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorRangeRule
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorSettings
import suwayomi.tachidesk.manga.impl.chapter.SmartMergeOverrideChange
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.getWithUserData
import suwayomi.tachidesk.server.settings.userConfig
import suwayomi.tachidesk.server.settings.value

/**
 * fork (Komikku port): Smart Scanlator Merge and chapter blacklist settings of a manga. Every field is optional and
 * written on its own, so a client can persist each change immediately (no "Save" step).
 */
class ScanlatorSettingsMutation {
    data class ScanlatorRangeRuleInput(
        val from: Double,
        val to: Double,
        val scanlator: String,
    )

    data class UpdateMangaScanlatorSettingsInput(
        val clientMutationId: String? = null,
        val mangaId: Int,
        @GraphQLDescription(
            "The order of the scanlators shown to the user, most preferred first. Saved scanlators that are not part of " +
                "the list (e.g. currently without chapters) keep their position.",
        )
        val scanlatorPriority: List<String>? = null,
        @GraphQLDescription("Replaces the range rules; on overlap the later rule wins")
        val scanlatorRangeRules: List<ScanlatorRangeRuleInput>? = null,
        val smartMergeMode: SmartMergeMode? = null,
        @GraphQLDescription("Keys of BlacklistEntryType to remove")
        val removeBlacklistEntries: List<String>? = null,
    )

    data class UpdateMangaScanlatorSettingsPayload(
        val clientMutationId: String?,
        val manga: MangaType,
        val scanlatorSettings: MangaScanlatorSettingsType,
    )

    @RequireAuth
    fun updateMangaScanlatorSettings(
        @GraphQLIgnore
        userId: Int,
        dataFetchingEnvironment: DataFetchingEnvironment,
        input: UpdateMangaScanlatorSettingsInput,
    ): UpdateMangaScanlatorSettingsPayload {
        val mangaId = input.mangaId
        requireManga(mangaId)

        input.scanlatorRangeRules?.forEach { rule ->
            require(rule.from.isFinite() && rule.to.isFinite() && rule.from <= rule.to) { "Invalid range ${rule.from}-${rule.to}" }
            require(rule.scanlator.isNotBlank()) { "A range rule needs a scanlator" }
        }

        val stored = ScanlatorSettings.get(userId, mangaId)
        ScanlatorSettings.update(
            userId,
            mangaId,
            blacklistedChapters = input.removeBlacklistEntries?.let { keys -> stored.blacklistedChapters.filterNot { it in keys } },
            scanlatorPriority = input.scanlatorPriority?.let { ChapterVisibility.mergePriority(stored.scanlatorPriority, it) },
            scanlatorRangeRules = input.scanlatorRangeRules?.map { ScanlatorRangeRule(it.from, it.to, it.scanlator).encode() },
            smartMergeOverride = input.smartMergeMode?.let { SmartMergeOverrideChange(it.override) },
        )
        ChapterVisibilityService.recompute(userId, listOf(mangaId))

        return payload(userId, mangaId, input.clientMutationId, dataFetchingEnvironment)
    }

    private fun requireManga(mangaId: Int) {
        val exists = transaction { MangaTable.select(MangaTable.id).where { MangaTable.id eq mangaId }.any() }
        require(exists) { "Manga $mangaId not found" }
    }

    private fun payload(
        userId: Int,
        mangaId: Int,
        clientMutationId: String?,
        dataFetchingEnvironment: DataFetchingEnvironment,
    ): UpdateMangaScanlatorSettingsPayload {
        MangaType.clearCacheFor(mangaId, dataFetchingEnvironment)
        val manga =
            transaction {
                MangaType(
                    MangaTable
                        .getWithUserData(userId)
                        .selectAll()
                        .where { MangaTable.id eq mangaId }
                        .first(),
                )
            }
        val settings =
            MangaScanlatorSettingsType.of(
                mangaId,
                ScanlatorSettings.get(userId, mangaId),
                ChapterVisibilityService.loadChapters(listOf(mangaId))[mangaId].orEmpty(),
                userConfig.smartScanlatorMerge.value(userId),
            )
        return UpdateMangaScanlatorSettingsPayload(clientMutationId, manga, settings)
    }
}

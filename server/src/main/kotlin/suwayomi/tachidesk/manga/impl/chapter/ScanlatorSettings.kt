package suwayomi.tachidesk.manga.impl.chapter

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import suwayomi.tachidesk.graphql.server.primitives.inIds
import suwayomi.tachidesk.manga.model.table.MangaUserTable

/**
 * fork: a user's scanlator rules for one manga (Komikku port). Stored in MANGAUSER as JSON string lists; a null
 * column means "never configured", which matters for backups (an empty list in a backup must not wipe local rules).
 */
data class ScanlatorSettings(
    val blacklistedChapters: List<String> = emptyList(),
    val scanlatorPriority: List<String> = emptyList(),
    val scanlatorRangeRules: List<String> = emptyList(),
    /** null = follow the user's global Smart Scanlator Merge setting */
    val smartMergeOverride: Boolean? = null,
    /** whether any of the three lists was ever stored */
    val present: Boolean = false,
) {
    fun smartMergeEnabled(global: Boolean): Boolean = smartMergeOverride ?: global

    fun hasRules(global: Boolean): Boolean = blacklistedChapters.isNotEmpty() || smartMergeEnabled(global)

    companion object {
        private val listSerializer = ListSerializer(String.serializer())

        fun encodeList(list: List<String>): String = Json.encodeToString(listSerializer, list)

        fun decodeList(value: String?): List<String> =
            value?.let { runCatching { Json.decodeFromString(listSerializer, it) }.getOrNull() }.orEmpty()

        fun fromRow(row: ResultRow?): ScanlatorSettings {
            if (row == null) return ScanlatorSettings()
            val blacklist = row.getOrNull(MangaUserTable.blacklistedChapters)
            val priority = row.getOrNull(MangaUserTable.scanlatorPriority)
            val rangeRules = row.getOrNull(MangaUserTable.scanlatorRangeRules)
            return ScanlatorSettings(
                blacklistedChapters = decodeList(blacklist),
                scanlatorPriority = decodeList(priority),
                scanlatorRangeRules = decodeList(rangeRules),
                smartMergeOverride = row.getOrNull(MangaUserTable.smartMergeOverride),
                present = blacklist != null || priority != null || rangeRules != null,
            )
        }

        fun get(
            userId: Int,
            mangaId: Int,
        ): ScanlatorSettings =
            transaction {
                fromRow(
                    MangaUserTable
                        .selectAll()
                        .where { (MangaUserTable.manga eq mangaId) and (MangaUserTable.user eq userId) }
                        .firstOrNull(),
                )
            }

        fun getAll(
            userId: Int,
            mangaIds: Collection<Int>,
        ): Map<Int, ScanlatorSettings> =
            transaction {
                MangaUserTable
                    .selectAll()
                    .where { (MangaUserTable.manga inIds mangaIds) and (MangaUserTable.user eq userId) }
                    .associate { it[MangaUserTable.manga].value to fromRow(it) }
            }

        /** Writes the given fields (null = unchanged) and creates the MANGAUSER row when missing. */
        fun update(
            userId: Int,
            mangaId: Int,
            blacklistedChapters: List<String>? = null,
            scanlatorPriority: List<String>? = null,
            scanlatorRangeRules: List<String>? = null,
            smartMergeOverride: SmartMergeOverrideChange? = null,
        ) {
            if (listOf(blacklistedChapters, scanlatorPriority, scanlatorRangeRules, smartMergeOverride).all { it == null }) return
            transaction {
                val exists =
                    MangaUserTable
                        .select(MangaUserTable.id)
                        .where { (MangaUserTable.manga eq mangaId) and (MangaUserTable.user eq userId) }
                        .any()
                if (!exists) {
                    MangaUserTable.insert {
                        it[manga] = mangaId
                        it[user] = userId
                    }
                }
                MangaUserTable.update({ (MangaUserTable.manga eq mangaId) and (MangaUserTable.user eq userId) }) {
                    blacklistedChapters?.let { list -> it[MangaUserTable.blacklistedChapters] = encodeList(list.distinct()) }
                    scanlatorPriority?.let { list -> it[MangaUserTable.scanlatorPriority] = encodeList(list.distinct()) }
                    scanlatorRangeRules?.let { list ->
                        it[MangaUserTable.scanlatorRangeRules] = encodeList(list.filter { rule -> ScanlatorRangeRule.parse(rule) != null })
                    }
                    smartMergeOverride?.let { change -> it[MangaUserTable.smartMergeOverride] = change.value }
                }
            }
        }
    }
}

/** A change of the per-manga Smart Scanlator Merge override; [value] null = follow the global setting. */
data class SmartMergeOverrideChange(
    val value: Boolean?,
)

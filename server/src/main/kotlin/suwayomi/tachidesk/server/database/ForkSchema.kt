package suwayomi.tachidesk.server.database

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.manga.impl.chapter.ChapterBlacklist
import suwayomi.tachidesk.manga.impl.chapter.ChapterVisibilityService
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorSettings
import suwayomi.tachidesk.server.database.migration.helpers.UNLIMITED_TEXT
import suwayomi.tachidesk.server.database.migration.helpers.toSqlName

/**
 * Schema changes of this fork.
 *
 * They are deliberately not part of the numbered migrations: the migration runner only executes versions
 * above the highest applied one and requires consecutive numbers, so a fork migration would collide with
 * (and silently skip) future upstream migrations. Every step here is idempotent and runs after the upstream
 * migrations on each startup.
 */
object ForkSchema {
    private val logger = KotlinLogging.logger {}

    /** user the single-user data was assigned to by [suwayomi.tachidesk.server.database.migration.M0066_AddUsers] */
    private const val LEGACY_DATA_USER_ID = 1

    fun ensure() {
        transaction {
            ensureKomikkuPortColumns()
            ensureChapterUserFillermark()
        }
        // separate transactions: the blacklist entries are committed before the fillermark data is removed, so an
        // interrupted start resumes where it stopped
        transaction { migrateFillermarksToBlacklist() }
        transaction { removeMigratedFillermarks() }
    }

    /**
     * Columns of the features ported from the Komikku fork (see docs/fork-integration/KOMIKKU_PORT_MIGRATIONS.md).
     * All of them are additive with defaults that keep the previous behaviour, so existing rows need no backfill:
     * - CHAPTERUSER: bookmark color (0 = default color) and the derived visibility flags of the chapter blacklist and
     *   Smart Scanlator Merge (recomputed from the MANGAUSER rules, never a source of truth)
     * - MANGAUSER: the per-user scanlator rules (null = never configured)
     * - CATEGORY: the parent category (null = top level) and the Smart Category tracker status binding
     */
    private fun ensureKomikkuPortColumns() {
        val text = UNLIMITED_TEXT
        listOf(
            Triple("CHAPTERUSER", "BOOKMARK_COLOR", "INT DEFAULT 0 NOT NULL"),
            Triple("CHAPTERUSER", "BLACKLISTED", "BOOLEAN DEFAULT FALSE NOT NULL"),
            Triple("CHAPTERUSER", "MERGE_HIDDEN", "BOOLEAN DEFAULT FALSE NOT NULL"),
            Triple("MANGAUSER", "BLACKLISTED_CHAPTERS", "$text NULL"),
            Triple("MANGAUSER", "SCANLATOR_PRIORITY", "$text NULL"),
            Triple("MANGAUSER", "SCANLATOR_RANGE_RULES", "$text NULL"),
            Triple("MANGAUSER", "SMART_MERGE_OVERRIDE", "BOOLEAN NULL"),
            Triple("CATEGORY", "PARENT_ID", "INT NULL"),
            Triple("CATEGORY", "TRACK_STATUS", "INT NULL"),
        ).forEach { (table, column, definition) ->
            if (!columnExists(table, column)) {
                logger.info { "ForkSchema: adding ${table.toSqlName()}.$column" }
                exec("ALTER TABLE ${table.toSqlName()} ADD COLUMN IF NOT EXISTS $column $definition")
            }
        }
        exec("CREATE INDEX IF NOT EXISTS FORK_CATEGORY_PARENT_ID ON ${"CATEGORY".toSqlName()} (PARENT_ID)")
    }

    /**
     * Fillermarks used to be a column of CHAPTER. Upstream moved all per-user chapter state to CHAPTERUSER,
     * so the fillermark lives there as well now. Older fork databases still have CHAPTER.FILLERMARK, its values
     * are moved to the user that owns the pre-multi-user data.
     */
    private fun ensureChapterUserFillermark() {
        val chapterUserTable = "CHAPTERUSER".toSqlName()
        val chapterTable = "CHAPTER".toSqlName()

        if (!columnExists("CHAPTER", "FILLERMARK")) {
            return
        }

        // staging column of the fillermark → blacklist migration below
        if (!columnExists("CHAPTERUSER", "FILLERMARK")) {
            logger.info { "ForkSchema: adding $chapterUserTable.FILLERMARK" }
            exec("ALTER TABLE $chapterUserTable ADD COLUMN IF NOT EXISTS FILLERMARK BOOLEAN DEFAULT FALSE NOT NULL")
        }

        logger.info { "ForkSchema: moving legacy $chapterTable.FILLERMARK values to $chapterUserTable" }

        // chapters that only have a fillermark did not get a CHAPTERUSER row during the user accounts backfill
        exec(
            """
            INSERT INTO $chapterUserTable (CHAPTER, USER_ID, FILLERMARK)
            SELECT c.ID, $LEGACY_DATA_USER_ID, TRUE
            FROM $chapterTable c
            WHERE c.FILLERMARK = TRUE
                AND NOT EXISTS (
                    SELECT 1 FROM $chapterUserTable cu WHERE cu.CHAPTER = c.ID AND cu.USER_ID = $LEGACY_DATA_USER_ID
                )
            """.trimIndent(),
        )
        exec(
            """
            UPDATE $chapterUserTable
            SET FILLERMARK = TRUE
            WHERE USER_ID = $LEGACY_DATA_USER_ID
                AND FILLERMARK = FALSE
                AND CHAPTER IN (SELECT ID FROM $chapterTable WHERE FILLERMARK = TRUE)
            """.trimIndent(),
        )
        exec("ALTER TABLE $chapterTable DROP COLUMN IF EXISTS FILLERMARK")
    }

    private data class Fillermark(
        val userId: Int,
        val mangaId: Int,
        val chapterId: Int,
    )

    private fun fillermarks(): List<Fillermark> {
        val chapterUserTable = "CHAPTERUSER".toSqlName()
        val chapterTable = "CHAPTER".toSqlName()
        return TransactionManager
            .current()
            .exec(
                """
                SELECT cu.USER_ID, c.MANGA, c.ID
                FROM $chapterUserTable cu JOIN $chapterTable c ON c.ID = cu.CHAPTER
                WHERE cu.FILLERMARK = TRUE
                """.trimIndent(),
            ) { rs ->
                buildList { while (rs.next()) add(Fillermark(rs.getInt(1), rs.getInt(2), rs.getInt(3))) }
            }.orEmpty()
    }

    /**
     * The fillermark feature was replaced by the chapter blacklist (Komikku port). Every fillermarked chapter becomes a
     * blacklist entry of the user that fillermarked it: a hybrid key (URL first, then number, scanlator and name), so
     * the entry follows the chapter through URL changes without ever matching another chapter. Entries are added to
     * the user's existing blacklist, chapters that already are blacklisted get no second entry. Idempotent.
     */
    private fun migrateFillermarksToBlacklist() {
        if (!columnExists("CHAPTERUSER", "FILLERMARK")) return
        val fillermarks = fillermarks()
        if (fillermarks.isEmpty()) return

        logger.info { "ForkSchema: moving ${fillermarks.size} fillermarks to the chapter blacklist" }
        val chaptersByManga = ChapterVisibilityService.loadChapters(fillermarks.map { it.mangaId }.toSet())
        fillermarks.groupBy { it.userId to it.mangaId }.forEach { (key, marks) ->
            val (userId, mangaId) = key
            val chapters = chaptersByManga[mangaId].orEmpty()
            val stored = ScanlatorSettings.get(userId, mangaId).blacklistedChapters
            val alreadyBlacklisted = ChapterBlacklist.blacklistedIds(chapters, stored)
            val markedIds = marks.mapTo(HashSet()) { it.chapterId }
            val newKeys =
                chapters
                    .filter { it.id in markedIds && it.id !in alreadyBlacklisted }
                    .map(ChapterBlacklist::hybridKey)
            if (newKeys.isNotEmpty()) {
                ScanlatorSettings.update(userId, mangaId, blacklistedChapters = stored + newKeys)
            }
        }
        // the derived flag, so counts are right before the first recomputation
        exec("UPDATE ${"CHAPTERUSER".toSqlName()} SET BLACKLISTED = TRUE, MERGE_HIDDEN = FALSE WHERE FILLERMARK = TRUE")
    }

    /**
     * Removes the fillermark column and the settings of the removed feature, but only after every fillermarked
     * chapter is verified to resolve to a blacklist entry of its user; otherwise everything is kept and the
     * migration is retried on the next start.
     */
    private fun removeMigratedFillermarks() {
        if (!columnExists("CHAPTERUSER", "FILLERMARK")) return
        val fillermarks = fillermarks()
        val chaptersByManga = ChapterVisibilityService.loadChapters(fillermarks.map { it.mangaId }.toSet())
        val unresolved =
            fillermarks.groupBy { it.userId to it.mangaId }.flatMap { (key, marks) ->
                val (userId, mangaId) = key
                val blacklisted =
                    ChapterBlacklist.blacklistedIds(
                        chaptersByManga[mangaId].orEmpty(),
                        ScanlatorSettings.get(userId, mangaId).blacklistedChapters,
                    )
                marks.filter { it.chapterId !in blacklisted }
            }
        if (unresolved.isNotEmpty()) {
            logger.warn { "ForkSchema: ${unresolved.size} fillermarks are not in the blacklist yet, keeping CHAPTERUSER.FILLERMARK" }
            return
        }

        logger.info { "ForkSchema: fillermarks migrated, removing CHAPTERUSER.FILLERMARK and the fillermark settings" }
        exec("DELETE FROM ${"MANGAMETA".toSqlName()} WHERE META_KEY = 'webUI_fillermarked'")
        exec(
            "DELETE FROM ${"GLOBALMETA".toSqlName()} WHERE META_KEY IN " +
                "('webUI_fillermarked', 'webUI_hasFillermarkedChapters', 'webUI_deleteChaptersWithFillermark')",
        )
        exec("DELETE FROM ${"CATEGORYMETA".toSqlName()} WHERE META_KEY = 'webUI_hasFillermarkedChapters'")
        exec("ALTER TABLE ${"CHAPTERUSER".toSqlName()} DROP COLUMN IF EXISTS FILLERMARK")
    }

    private fun exec(sql: String) {
        TransactionManager.current().exec(sql)
    }

    private fun columnExists(
        table: String,
        column: String,
    ): Boolean {
        val (properTable, properColumn) =
            TransactionManager.current().db.identifierManager.let {
                it.inProperCase(table) to it.inProperCase(column)
            }
        return (
            TransactionManager.current().exec(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$properTable' AND COLUMN_NAME = '$properColumn'",
            ) { rs ->
                rs.next()
                rs.getInt(1)
            } ?: 0
        ) > 0
    }
}

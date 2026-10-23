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
            ensureChapterUserFillermark()
        }
    }

    /**
     * Fillermarks used to be a column of CHAPTER. Upstream moved all per-user chapter state to CHAPTERUSER,
     * so the fillermark lives there as well now. Older fork databases still have CHAPTER.FILLERMARK, its values
     * are moved to the user that owns the pre-multi-user data.
     */
    private fun ensureChapterUserFillermark() {
        val chapterUserTable = "CHAPTERUSER".toSqlName()
        val chapterTable = "CHAPTER".toSqlName()

        if (!columnExists("CHAPTERUSER", "FILLERMARK")) {
            logger.info { "ForkSchema: adding $chapterUserTable.FILLERMARK" }
            exec("ALTER TABLE $chapterUserTable ADD COLUMN IF NOT EXISTS FILLERMARK BOOLEAN DEFAULT FALSE NOT NULL")
        }

        if (!columnExists("CHAPTER", "FILLERMARK")) {
            return
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

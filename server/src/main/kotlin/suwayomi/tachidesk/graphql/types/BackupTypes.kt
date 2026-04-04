package suwayomi.tachidesk.graphql.types

import kotlinx.coroutines.flow.first
import suwayomi.tachidesk.manga.impl.backup.IBackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupImport
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupExport

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

data class PartialBackupFlags(
    override val includeManga: Boolean?,
    override val includeCategories: Boolean?,
    override val includeChapters: Boolean?,
    override val includeTracking: Boolean?,
    override val includeHistory: Boolean?,
    override val includeClientData: Boolean?,
    override val includeServerSettings: Boolean?,
) : IBackupFlags

enum class BackupRestoreState {
    IDLE,
    SUCCESS,
    FAILURE,
    RESTORING_CATEGORIES,
    RESTORING_MANGA,
    RESTORING_META,
    RESTORING_SETTINGS,
}

data class BackupRestoreStatus(
    val state: BackupRestoreState,
    val totalManga: Int,
    val mangaProgress: Int,
)

fun ProtoBackupImport.BackupRestoreState.toStatus(): BackupRestoreStatus =
    when (this) {
        ProtoBackupImport.BackupRestoreState.Idle -> {
            BackupRestoreStatus(
                state = BackupRestoreState.IDLE,
                totalManga = 0,
                mangaProgress = 0,
            )
        }

        is ProtoBackupImport.BackupRestoreState.Success -> {
            BackupRestoreStatus(
                state = BackupRestoreState.SUCCESS,
                totalManga = 0,
                mangaProgress = 0,
            )
        }

        is ProtoBackupImport.BackupRestoreState.Failure -> {
            BackupRestoreStatus(
                state = BackupRestoreState.FAILURE,
                totalManga = 0,
                mangaProgress = 0,
            )
        }

        is ProtoBackupImport.BackupRestoreState.RestoringCategories -> {
            BackupRestoreStatus(
                state = BackupRestoreState.RESTORING_CATEGORIES,
                totalManga = totalManga,
                mangaProgress = current,
            )
        }

        is ProtoBackupImport.BackupRestoreState.RestoringMeta -> {
            BackupRestoreStatus(
                state = BackupRestoreState.RESTORING_META,
                totalManga = totalManga,
                mangaProgress = current,
            )
        }

        is ProtoBackupImport.BackupRestoreState.RestoringSettings -> {
            BackupRestoreStatus(
                state = BackupRestoreState.RESTORING_SETTINGS,
                totalManga = totalManga,
                mangaProgress = current,
            )
        }

        is ProtoBackupImport.BackupRestoreState.RestoringManga -> {
            BackupRestoreStatus(
                state = BackupRestoreState.RESTORING_MANGA,
                totalManga = totalManga,
                mangaProgress = current,
            )
        }
    }

// --- Create (export) state mapping ---

enum class BackupCreateState {
    IDLE,
    SUCCESS,
    FAILURE,
    CREATING_CATEGORIES,
    CREATING_MANGA,
    CREATING_META,
    CREATING_SETTINGS,
}

data class BackupCreateStatus(
    val state: BackupCreateState,
    val totalManga: Int,
    val mangaProgress: Int,
    val title: String? = null,
)

fun ProtoBackupExport.BackupCreateState.toCreateStatus(): BackupCreateStatus =
    when (this) {
        ProtoBackupExport.BackupCreateState.Idle -> {
            BackupCreateStatus(state = BackupCreateState.IDLE, totalManga = 0, mangaProgress = 0, title = null)
        }

        is ProtoBackupExport.BackupCreateState.Success -> {
            BackupCreateStatus(state = BackupCreateState.SUCCESS, totalManga = 0, mangaProgress = 0, title = null)
        }

        is ProtoBackupExport.BackupCreateState.Failure -> {
            BackupCreateStatus(state = BackupCreateState.FAILURE, totalManga = 0, mangaProgress = 0, title = null)
        }

        is ProtoBackupExport.BackupCreateState.CreatingCategories -> {
            BackupCreateStatus(state = BackupCreateState.CREATING_CATEGORIES, totalManga = totalManga, mangaProgress = current, title = null)
        }

        is ProtoBackupExport.BackupCreateState.CreatingMeta -> {
            BackupCreateStatus(state = BackupCreateState.CREATING_META, totalManga = totalManga, mangaProgress = current, title = null)
        }

        is ProtoBackupExport.BackupCreateState.CreatingSettings -> {
            BackupCreateStatus(state = BackupCreateState.CREATING_SETTINGS, totalManga = totalManga, mangaProgress = current, title = null)
        }

        is ProtoBackupExport.BackupCreateState.CreatingManga -> {
            BackupCreateStatus(state = BackupCreateState.CREATING_MANGA, totalManga = totalManga, mangaProgress = current, title = title)
        }
    }

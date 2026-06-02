package suwayomi.tachidesk.graphql.queries

import io.javalin.http.UploadedFile
import suwayomi.tachidesk.graphql.directives.RequireAuth
import suwayomi.tachidesk.graphql.types.BackupCreateStatus
import suwayomi.tachidesk.graphql.types.BackupRestoreStatus
import suwayomi.tachidesk.graphql.types.toCreateStatus
import suwayomi.tachidesk.graphql.types.toStatus
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupExport
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupImport
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupValidator

class BackupQuery {
    data class ValidateBackupInput(
        val backup: UploadedFile,
    )

    data class ValidateBackupSource(
        val id: Long,
        val name: String,
    )

    data class ValidateBackupTracker(
        val name: String,
    )

    data class ValidateBackupResult(
        val missingSources: List<ValidateBackupSource>,
        val missingTrackers: List<ValidateBackupTracker>,
    )

    @RequireAuth
    fun validateBackup(input: ValidateBackupInput): ValidateBackupResult {
        val result = ProtoBackupValidator.validate(input.backup.content())
        return ValidateBackupResult(
            result.missingSourceIds.map { ValidateBackupSource(it.id, it.name) },
            result.missingTrackers.map { ValidateBackupTracker(it) },
        )
    }

    @RequireAuth
    fun restoreStatus(id: String): BackupRestoreStatus? = ProtoBackupImport.getRestoreState(id)?.toStatus()

    @RequireAuth
    fun createStatus(id: String): BackupCreateStatus? = ProtoBackupExport.getCreateState(id)?.toCreateStatus()
}

@file:Suppress("RedundantNullableReturnType", "unused")

package suwayomi.tachidesk.graphql.mutations

import com.expediagroup.graphql.generator.annotations.GraphQLDeprecated
import com.expediagroup.graphql.generator.annotations.GraphQLIgnore
import io.github.oshai.kotlinlogging.KotlinLogging
import io.javalin.http.UploadedFile
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.withTimeout
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.graphql.directives.RequireAuth
import suwayomi.tachidesk.graphql.server.TemporaryFileStorage
import suwayomi.tachidesk.graphql.types.BackupCreateStatus
import suwayomi.tachidesk.graphql.types.BackupRestoreStatus
import suwayomi.tachidesk.graphql.types.ExtensionType
import suwayomi.tachidesk.graphql.types.PartialBackupFlags
import suwayomi.tachidesk.graphql.types.toCreateStatus
import suwayomi.tachidesk.graphql.types.toStatus
import suwayomi.tachidesk.manga.impl.backup.BackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupExport
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupImport
import suwayomi.tachidesk.manga.impl.backup.proto.ProtoBackupValidator
import suwayomi.tachidesk.manga.impl.backup.proto.models.Backup
import suwayomi.tachidesk.manga.impl.extension.Extension
import suwayomi.tachidesk.manga.impl.extension.ExtensionStoreService
import suwayomi.tachidesk.manga.impl.extension.ExtensionsList
import suwayomi.tachidesk.manga.model.dataclass.ExtensionInfo
import suwayomi.tachidesk.manga.model.table.ExtensionTable
import suwayomi.tachidesk.server.JavalinSetup.future
import suwayomi.tachidesk.server.user.ForbiddenException
import suwayomi.tachidesk.server.user.UserPermission
import suwayomi.tachidesk.server.user.hasPermission
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration.Companion.seconds

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

private val logger = KotlinLogging.logger {}

class BackupMutation {
    data class RestoreBackupInput(
        val clientMutationId: String? = null,
        val backup: UploadedFile,
        val flags: PartialBackupFlags? = null,
    )

    data class RestoreBackupPayload(
        val clientMutationId: String?,
        val id: String,
        val status: BackupRestoreStatus?,
    )

    @RequireAuth
    fun restoreBackup(
        @GraphQLIgnore
        userId: Int,
        input: RestoreBackupInput,
    ): CompletableFuture<RestoreBackupPayload> {
        val (clientMutationId, backup, flags) = input

        return future {
            val restoreId =
                ProtoBackupImport.restore(
                    userId,
                    backup.content(),
                    BackupFlags.fromPartial(flags),
                )

            withTimeout(10.seconds) {
                ProtoBackupImport.notifyFlow
                    .onSubscription {
                        val state = ProtoBackupImport.getRestoreState(restoreId)
                        if (state != null && state != ProtoBackupImport.BackupRestoreState.Idle) {
                            emit(Unit)
                        }
                    }.first {
                        ProtoBackupImport.getRestoreState(restoreId) != null
                    }
            }

            RestoreBackupPayload(
                clientMutationId,
                restoreId,
                ProtoBackupImport.getRestoreState(restoreId)?.toStatus(),
            )
        }
    }

    data class InstallMissingExtensionsFromBackupInput(
        val clientMutationId: String? = null,
        val backup: UploadedFile,
    )

    data class InstallMissingExtensionsFromBackupSource(
        val id: Long,
        val name: String,
    )

    data class InstallMissingExtensionsFromBackupPayload(
        val clientMutationId: String?,
        val requestedSources: List<InstallMissingExtensionsFromBackupSource>,
        val unmatchedSources: List<InstallMissingExtensionsFromBackupSource>,
        val installedExtensions: List<ExtensionType>,
        val matchedExtensionPkgNames: List<String>,
    )

    @RequireAuth
    fun installMissingExtensionsFromBackup(
        @GraphQLIgnore
        userId: Int,
        @GraphQLIgnore
        permissions: List<UserPermission>,
        input: InstallMissingExtensionsFromBackupInput,
    ): CompletableFuture<InstallMissingExtensionsFromBackupPayload> {
        val (clientMutationId, backupFile) = input

        if (!permissions.hasPermission(UserPermission.INSTALL_EXTENSIONS)) {
            throw ForbiddenException()
        }

        return future {
            val backupBytes =
                backupFile
                    .content()
                    .use { inputStream -> inputStream.readBytes() }

            val validationResult = ProtoBackupValidator.validate(userId, backupBytes.inputStream())
            val missingSources = validationResult.missingSourceIds

            if (missingSources.isEmpty()) {
                return@future InstallMissingExtensionsFromBackupPayload(
                    clientMutationId = clientMutationId,
                    requestedSources = emptyList(),
                    unmatchedSources = emptyList(),
                    installedExtensions = emptyList(),
                    matchedExtensionPkgNames = emptyList(),
                )
            }

            ExtensionsList.fetchExtensions()

            val missingSourceIds = missingSources.map { it.id }.toSet()

            val onlineExtensions =
                ExtensionStoreService.getAndRefresh().flatMap { store ->
                    runCatching { ExtensionStoreService.getExtensions(store) }
                        .onFailure { logger.warn(it) { "Failed to fetch extensions for store: ${store.indexUrl}" } }
                        .getOrDefault(emptyList())
                }

            val matchedExtensionsByPkgName =
                onlineExtensions
                    .asSequence()
                    .filter { onlineExtension ->
                        onlineExtension.sources.any { source -> source.id in missingSourceIds }
                    }.groupBy { it.pkgName }
                    .mapValues { (_, extensionsForPkg) ->
                        extensionsForPkg.maxBy { it.versionCode }
                    }

            val matchedExtensionPkgNames = matchedExtensionsByPkgName.keys.toList()

            matchedExtensionPkgNames.forEach { pkgName ->
                Extension.installExtension(pkgName)
            }

            val installedExtensions =
                if (matchedExtensionPkgNames.isEmpty()) {
                    emptyList()
                } else {
                    transaction {
                        ExtensionTable
                            .selectAll()
                            .where { ExtensionTable.pkgName inList matchedExtensionPkgNames }
                            .map { ExtensionType(it) }
                    }
                }

            val matchedSourceIds =
                matchedExtensionsByPkgName
                    .values
                    .flatMap(ExtensionInfo::sources)
                    .map { it.id }
                    .toSet()

            val unmatchedSources =
                missingSources
                    .filterNot { it.id in matchedSourceIds }
                    .map { InstallMissingExtensionsFromBackupSource(it.id, it.name) }

            InstallMissingExtensionsFromBackupPayload(
                clientMutationId = clientMutationId,
                requestedSources =
                    missingSources.map {
                        InstallMissingExtensionsFromBackupSource(
                            it.id,
                            it.name,
                        )
                    },
                unmatchedSources = unmatchedSources,
                installedExtensions = installedExtensions,
                matchedExtensionPkgNames = matchedExtensionPkgNames,
            )
        }
    }

    data class CreateBackupInput(
        val clientMutationId: String? = null,
        val flags: PartialBackupFlags? = null,
        @GraphQLDeprecated("Will get removed", replaceWith = ReplaceWith("flags"))
        val includeChapters: Boolean? = null,
        @GraphQLDeprecated("Will get removed", replaceWith = ReplaceWith("flags"))
        val includeCategories: Boolean? = null,
        @GraphQLDeprecated("Will get removed", replaceWith = ReplaceWith("flags"))
        val includeTracking: Boolean? = null,
        @GraphQLDeprecated("Will get removed", replaceWith = ReplaceWith("flags"))
        val includeHistory: Boolean? = null,
        @GraphQLDeprecated("Will get removed", replaceWith = ReplaceWith("flags"))
        val includeClientData: Boolean? = null,
        @GraphQLDeprecated("Will get removed", replaceWith = ReplaceWith("flags"))
        val includeServerSettings: Boolean? = null,
    )

    data class CreateBackupPayload(
        val clientMutationId: String?,
        val url: String,
    )

    @RequireAuth
    fun createBackup(
        @GraphQLIgnore
        userId: Int,
        @GraphQLIgnore
        permissions: List<UserPermission>,
        input: CreateBackupInput? = null,
    ): CreateBackupPayload {
        val filename = Backup.getFilename()

        val backup = ProtoBackupExport.createBackup(userId, getEffectiveBackupFlags(permissions, input))

        val token = TemporaryFileStorage.saveFile(filename, backup)

        return CreateBackupPayload(
            clientMutationId = input?.clientMutationId,
            url = "/api/graphql/files/backup/$token",
        )
    }

    // New: async create that returns a create job id and a status entry (mirrors restore)
    data class CreateBackupAsyncPayload(
        val clientMutationId: String?,
        val id: String,
        val status: BackupCreateStatus?,
    )

    @RequireAuth
    fun createBackupAsync(
        @GraphQLIgnore
        userId: Int,
        @GraphQLIgnore
        permissions: List<UserPermission>,
        input: CreateBackupInput? = null,
    ): CompletableFuture<CreateBackupAsyncPayload> {
        val flags = getEffectiveBackupFlags(permissions, input)

        return future {
            val createId = ProtoBackupExport.createBackupAsync(userId, flags)

            withTimeout(10.seconds) {
                ProtoBackupExport.createNotifyFlow.first {
                    ProtoBackupExport.getCreateState(createId) != null
                }
            }

            CreateBackupAsyncPayload(
                input?.clientMutationId,
                createId,
                ProtoBackupExport.getCreateState(createId)?.toCreateStatus(),
            )
        }
    }
}

private fun getEffectiveBackupFlags(
    permissions: List<UserPermission>,
    input: BackupMutation.CreateBackupInput?,
): BackupFlags {
    val backupFlags =
        if (input?.flags != null) {
            BackupFlags.fromPartial(input.flags)
        } else {
            BackupFlags(
                includeManga = BackupFlags.DEFAULT.includeManga,
                includeCategories = input?.includeCategories ?: BackupFlags.DEFAULT.includeCategories,
                includeChapters = input?.includeChapters ?: BackupFlags.DEFAULT.includeChapters,
                includeTracking = input?.includeTracking ?: BackupFlags.DEFAULT.includeTracking,
                includeHistory = input?.includeHistory ?: BackupFlags.DEFAULT.includeHistory,
                includeClientData = input?.includeClientData ?: BackupFlags.DEFAULT.includeClientData,
                includeServerSettings = input?.includeServerSettings ?: BackupFlags.DEFAULT.includeServerSettings,
                includeUserSettings = BackupFlags.DEFAULT.includeUserSettings,
            )
        }

    // users without the MANAGE_SETTINGS permission may not export the global server settings
    return if (permissions.hasPermission(UserPermission.MANAGE_SETTINGS)) {
        backupFlags
    } else {
        backupFlags.copy(includeServerSettings = false)
    }
}

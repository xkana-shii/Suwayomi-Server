@file:Suppress("RedundantNullableReturnType", "unused")

package suwayomi.tachidesk.graphql.mutations

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.neq
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.graphql.directives.RequireAuth
import suwayomi.tachidesk.graphql.types.UpdateState.DOWNLOADING
import suwayomi.tachidesk.graphql.types.UpdateState.ERROR
import suwayomi.tachidesk.graphql.types.UpdateState.IDLE
import suwayomi.tachidesk.graphql.types.WebUIFlavor
import suwayomi.tachidesk.graphql.types.WebUIUpdateStatus
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.server.JavalinSetup.future
import suwayomi.tachidesk.server.util.ExitCode
import suwayomi.tachidesk.server.util.WebInterfaceManager
import suwayomi.tachidesk.server.util.shutdownApp
import java.util.concurrent.CompletableFuture
import kotlin.concurrent.thread
import kotlin.time.Duration.Companion.seconds

class InfoMutation {
    data class WebUIUpdateInput(
        val clientMutationId: String? = null,
    )

    data class WebUIUpdatePayload(
        val clientMutationId: String?,
        val updateStatus: WebUIUpdateStatus,
    )

    data class ShutdownServerInput(
        val clientMutationId: String? = null,
    )

    data class ShutdownServerPayload(
        val clientMutationId: String?,
        val success: Boolean,
    )

    // Minimal clear-database input / payload (Komikku-equivalent)
    data class ClearDatabaseInput(
        val clientMutationId: String? = null,
        val keepReadManga: Boolean? = true,
        val sourceIds: List<Int>? = null,
    )

    data class ClearDatabasePayload(
        val clientMutationId: String?,
        val success: Boolean,
    )

    @RequireAuth
    fun updateWebUI(input: WebUIUpdateInput): CompletableFuture<WebUIUpdatePayload?> {
        return future {
            withTimeout(30.seconds) {
                if (WebInterfaceManager.status.value.state === DOWNLOADING) {
                    return@withTimeout WebUIUpdatePayload(input.clientMutationId, WebInterfaceManager.status.value)
                }

                val flavor = WebUIFlavor.current

                val (version, updateAvailable) = WebInterfaceManager.isUpdateAvailable(flavor)

                if (!updateAvailable) {
                    val didUpdateCheckFail = version.isEmpty()

                    return@withTimeout WebUIUpdatePayload(
                        input.clientMutationId,
                        WebInterfaceManager.getStatus(version, if (didUpdateCheckFail) ERROR else IDLE),
                    )
                }
                try {
                    WebInterfaceManager.startDownloadInScope(flavor, version)
                } catch (e: Exception) {
                    // ignore since we use the status anyway
                }

                WebUIUpdatePayload(
                    input.clientMutationId,
                    updateStatus = WebInterfaceManager.status.first { it.state == DOWNLOADING },
                )
            }
        }
    }

    @RequireAuth
    fun resetWebUIUpdateStatus(): CompletableFuture<WebUIUpdateStatus?> =
        future {
            withTimeout(30.seconds) {
                val isUpdateFinished = WebInterfaceManager.status.value.state != DOWNLOADING
                if (!isUpdateFinished) {
                    throw Exception("Status reset is not allowed during status \"$DOWNLOADING\"")
                }

                WebInterfaceManager.resetStatus()

                WebInterfaceManager.status.first { it.state == IDLE }
            }
        }

    @RequireAuth
    fun shutdownServer(input: ShutdownServerInput): CompletableFuture<ShutdownServerPayload?> =
        future {
            thread(start = true, isDaemon = false) {
                Thread.sleep(250)
                shutdownApp(ExitCode.Success)
            }

            ShutdownServerPayload(
                clientMutationId = input.clientMutationId,
                success = true,
            )
        }

    /**
     * Clear database mutation (Komikku-style only).
     *
     * Behavior:
     * - Deletes manga rows that are not in library (inLibrary == false).
     * - If sourceIds provided, restricts candidates to those sources.
     * - If keepReadManga == true, excludes manga that have any read chapters or lastPageRead != 0.
     *
     * This performs only the database deletions comparable to Komikku's clear database flow.
     * It does NOT delete downloaded files, create backups, or pause services.
     */
    @RequireAuth
    fun clearDatabase(input: ClearDatabaseInput): CompletableFuture<ClearDatabasePayload?> =
        future {
            val keepRead = input.keepReadManga ?: true
            val sourceIds = input.sourceIds?.map { it.toLong() } ?: emptyList()

            transaction {
                // 1) Find candidate manga IDs (not in library), optionally restricted by source ids
                val candidateIds =
                    if (sourceIds.isEmpty()) {
                        MangaTable
                            .selectAll()
                            .where { MangaTable.inLibrary eq false }
                            .map { it[MangaTable.id].value }
                    } else {
                        MangaTable
                            .selectAll()
                            .where { (MangaTable.inLibrary eq false) and (MangaTable.sourceReference inList sourceIds) }
                            .map { it[MangaTable.id].value }
                    }

                if (candidateIds.isEmpty()) {
                    return@transaction
                }

                // 2) If keepRead is true, find manga IDs that have read chapters or lastPageRead != 0 and exclude them
                val idsToDelete =
                    if (keepRead) {
                        val idsWithRead =
                            ChapterTable
                                .selectAll()
                                .where { (ChapterTable.isRead eq true) or (ChapterTable.lastPageRead neq 0) }
                                .map { it[ChapterTable.manga].value }
                                .toSet()

                        candidateIds.filterNot { it in idsWithRead }
                    } else {
                        candidateIds
                    }

                if (idsToDelete.isEmpty()) {
                    return@transaction
                }

                // 3) Delete mangas (schema FK cascades will remove related chapters/pages/category mappings)
                MangaTable.deleteWhere { MangaTable.id inList idsToDelete }
            }

            ClearDatabasePayload(clientMutationId = input.clientMutationId, success = true)
        }
}

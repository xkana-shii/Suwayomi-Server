@file:Suppress("RedundantNullableReturnType", "unused")

package suwayomi.tachidesk.graphql.mutations

import eu.kanade.tachiyomi.source.local.LocalSource
import eu.kanade.tachiyomi.source.local.image.LocalCoverManager
import eu.kanade.tachiyomi.source.local.io.LocalSourceFileSystem
import eu.kanade.tachiyomi.source.model.SManga
import io.javalin.http.UploadedFile
import org.jetbrains.exposed.v1.core.LikePattern
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.like
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import suwayomi.tachidesk.graphql.directives.RequireAuth
import suwayomi.tachidesk.graphql.types.MangaMetaType
import suwayomi.tachidesk.graphql.types.MangaType
import suwayomi.tachidesk.graphql.types.MetaInput
import suwayomi.tachidesk.manga.impl.Library
import suwayomi.tachidesk.manga.impl.LocalMangaDetailsService
import suwayomi.tachidesk.manga.impl.Manga
import suwayomi.tachidesk.manga.impl.MangaList
import suwayomi.tachidesk.manga.impl.update.IUpdater
import suwayomi.tachidesk.manga.impl.util.getThumbnailDownloadPath
import suwayomi.tachidesk.manga.impl.util.storage.ImageResponse
import suwayomi.tachidesk.manga.model.table.MangaMetaTable
import suwayomi.tachidesk.manga.model.table.MangaStatus
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.toDataClass
import suwayomi.tachidesk.server.ApplicationDirs
import suwayomi.tachidesk.server.JavalinSetup.future
import uy.kohesive.injekt.injectLazy
import java.io.File
import java.time.Instant
import java.util.concurrent.CompletableFuture

/**
 * TODO Mutations
 * - Download x(all = -1) chapters
 * - Delete read/all downloaded chapters
 */
class MangaMutation {
    private val updater: IUpdater by injectLazy()
    private val applicationDirs: ApplicationDirs by injectLazy()

    data class UpdateMangaPatch(
        val inLibrary: Boolean? = null,
    )

    data class UpdateMangaPayload(
        val clientMutationId: String?,
        val manga: MangaType,
    )

    data class UpdateMangaInput(
        val clientMutationId: String? = null,
        val id: Int,
        val patch: UpdateMangaPatch,
    )

    data class UpdateMangasPayload(
        val clientMutationId: String?,
        val mangas: List<MangaType>,
    )

    data class UpdateMangasInput(
        val clientMutationId: String? = null,
        val ids: List<Int>,
        val patch: UpdateMangaPatch,
    )

    private suspend fun updateMangas(
        ids: List<Int>,
        patch: UpdateMangaPatch,
    ) {
        transaction {
            if (patch.inLibrary != null) {
                MangaTable.update({ MangaTable.id inList ids }) { update ->
                    patch.inLibrary.also {
                        update[inLibrary] = it
                        if (it) {
                            update[inLibraryAt] = Instant.now().epochSecond
                        }
                    }
                }
            }
        }.apply {
            if (patch.inLibrary != null) {
                transaction {
                    // try to initialize uninitialized in library manga to ensure that the expected data is available (chapter list, metadata, ...)
                    val mangas =
                        transaction {
                            MangaTable
                                .selectAll()
                                .where { (MangaTable.id inList ids) and (MangaTable.initialized eq false) }
                                .map { MangaTable.toDataClass(it) }
                        }

                    updater.addMangasToQueue(mangas)
                }

                ids.forEach {
                    Library.handleMangaThumbnail(it, patch.inLibrary)
                }
            }
        }
    }

    // --- Manga Details Editing ---

    data class UpdateMangaDetailsPatch(
        val title: String? = null,
        val author: String? = null,
        val artist: String? = null,
        val description: String? = null,
        val genre: List<String>? = null,
        val status: MangaStatus? = null,
    )

    data class UpdateMangaDetailsInput(
        val clientMutationId: String? = null,
        val id: Int,
        val patch: UpdateMangaDetailsPatch,
    )

    data class UpdateMangaDetailsPayload(
        val clientMutationId: String?,
        val manga: MangaType,
    )

    data class ResetMangaMetadataToSourceInput(
        val clientMutationId: String? = null,
        val id: Int,
        val resetCover: Boolean = true,
    )

    data class ResetMangaMetadataToSourcePayload(
        val clientMutationId: String?,
        val manga: MangaType,
    )

    private val localMangaDetailsService = LocalMangaDetailsService()

    @RequireAuth
    fun updateMangaDetails(input: UpdateMangaDetailsInput): CompletableFuture<UpdateMangaDetailsPayload?> {
        val (clientMutationId, id, patch) = input

        return future {
            // Step 1: Update database (skip if no fields to update)
            val row =
                transaction {
                    MangaTable.selectAll().where { MangaTable.id eq id }.firstOrNull()
                        ?: throw IllegalArgumentException("Manga with id $id not found")
                }

            val metaUpdates = linkedMapOf<String, String>()
            val metaDeletes = mutableListOf<String>()

            if (patch.title != null) {
                if (patch.title.isBlank()) {
                    metaDeletes += Manga.OVERRIDE_TITLE_KEY
                } else {
                    metaUpdates[Manga.OVERRIDE_TITLE_KEY] = patch.title
                }
            }

            if (patch.author != null) {
                if (patch.author.isBlank()) {
                    metaDeletes += Manga.OVERRIDE_AUTHOR_KEY
                } else {
                    metaUpdates[Manga.OVERRIDE_AUTHOR_KEY] = patch.author
                }
            }

            if (patch.artist != null) {
                if (patch.artist.isBlank()) {
                    metaDeletes += Manga.OVERRIDE_ARTIST_KEY
                } else {
                    metaUpdates[Manga.OVERRIDE_ARTIST_KEY] = patch.artist
                }
            }

            if (patch.description != null) {
                if (patch.description.isBlank()) {
                    metaDeletes += Manga.OVERRIDE_DESCRIPTION_KEY
                } else {
                    metaUpdates[Manga.OVERRIDE_DESCRIPTION_KEY] = patch.description
                }
            }

            if (patch.genre != null || patch.status != null) {
                metaUpdates[Manga.METADATA_MODE_KEY] = Manga.MODE_CUSTOM
            }

            val shouldUpdateTitle = patch.title != null && patch.title.isNotBlank()
            val shouldUpdateAuthor = patch.author != null
            val shouldUpdateArtist = patch.artist != null
            val shouldUpdateDescription = patch.description != null
            val shouldUpdateGenre = patch.genre != null
            val shouldUpdateStatus = patch.status != null

            val hasDbAssignments =
                shouldUpdateTitle ||
                    shouldUpdateAuthor ||
                    shouldUpdateArtist ||
                    shouldUpdateDescription ||
                    shouldUpdateGenre ||
                    shouldUpdateStatus

            if (hasDbAssignments) {
                transaction {
                    MangaTable.update({ MangaTable.id eq id }) { update ->
                        if (shouldUpdateTitle) {
                            update[title] = patch.title!!
                        }
                        if (shouldUpdateAuthor) {
                            update[author] = patch.author?.takeIf(String::isNotBlank)
                        }
                        if (shouldUpdateArtist) {
                            update[artist] = patch.artist?.takeIf(String::isNotBlank)
                        }
                        if (shouldUpdateDescription) {
                            update[description] = patch.description?.takeIf(String::isNotBlank)
                        }
                        if (shouldUpdateGenre) {
                            update[genre] = patch.genre!!.joinToString(", ")
                        }
                        if (shouldUpdateStatus) {
                            update[status] = patch.status!!.value
                        }
                    }
                }
            }

            // Step 2: Write details.json for local source manga
            if (metaUpdates.isNotEmpty()) {
                Manga.modifyMangasMetas(mapOf(id to metaUpdates))
            }

            if (metaDeletes.isNotEmpty()) {
                transaction {
                    MangaMetaTable.deleteWhere {
                        (MangaMetaTable.ref eq id) and (MangaMetaTable.key inList metaDeletes)
                    }
                }
            }

            val sourceId = row[MangaTable.sourceReference]
            if (sourceId == LocalSource.ID) {
                val mangaUrl = row[MangaTable.url]
                val fileSystem = LocalSourceFileSystem(applicationDirs)
                val mangaDir = fileSystem.getMangaDirectory(mangaUrl)

                if (mangaDir != null) {
                    val existing = localMangaDetailsService.readDetails(mangaDir)
                    val merged =
                        localMangaDetailsService.mergeDetails(
                            existing = existing,
                            title = patch.title,
                            author = patch.author,
                            artist = patch.artist,
                            description = patch.description,
                            genre = patch.genre,
                            status = patch.status?.value,
                        )
                    localMangaDetailsService.writeDetailsWithLock(mangaDir, merged)
                }
            }

            // Step 3: Return updated manga
            Manga.fetchManga(id)

            val manga =
                transaction {
                    MangaType(MangaTable.selectAll().where { MangaTable.id eq id }.first())
                }

            UpdateMangaDetailsPayload(
                clientMutationId = clientMutationId,
                manga = manga,
            )
        }
    }

    // --- Cover Image Upload ---
    @RequireAuth
    fun resetMangaMetadataToSource(input: ResetMangaMetadataToSourceInput): CompletableFuture<ResetMangaMetadataToSourcePayload?> {
        val (clientMutationId, id, resetCover) = input

        return future {
            val keysToDelete =
                mutableListOf(
                    Manga.METADATA_MODE_KEY,
                    Manga.METADATA_PROVIDER_KEY,
                    Manga.METADATA_EXTERNAL_ID_KEY,
                    Manga.OVERRIDE_TITLE_KEY,
                    Manga.OVERRIDE_AUTHOR_KEY,
                    Manga.OVERRIDE_ARTIST_KEY,
                    Manga.OVERRIDE_DESCRIPTION_KEY,
                ).apply {
                    if (resetCover) {
                        add(Manga.COVER_MODE_KEY)
                    }
                }

            transaction {
                MangaMetaTable.deleteWhere {
                    (MangaMetaTable.ref eq id) and (MangaMetaTable.key inList keysToDelete)
                }
            }

            if (resetCover) {
                Manga.clearThumbnail(id)
            }

            Manga.fetchManga(id)

            val manga =
                transaction {
                    MangaType(MangaTable.selectAll().where { MangaTable.id eq id }.first())
                }

            ResetMangaMetadataToSourcePayload(
                clientMutationId = clientMutationId,
                manga = manga,
            )
        }
    }

    data class UploadMangaCoverInput(
        val clientMutationId: String? = null,
        val id: Int,
        val cover: UploadedFile,
    )

    data class UploadMangaCoverPayload(
        val clientMutationId: String?,
        val manga: MangaType,
    )

    @RequireAuth
    fun uploadMangaCover(input: UploadMangaCoverInput): CompletableFuture<UploadMangaCoverPayload?> {
        val (clientMutationId, id, cover) = input

        return future {
            val imageBytes = cover.content().use { it.readBytes() }
            val mimeType = cover.contentType()

            val row =
                transaction {
                    MangaTable.selectAll().where { MangaTable.id eq id }.firstOrNull()
                        ?: throw IllegalArgumentException("Manga with id $id not found")
                }

            Manga.clearThumbnail(id)

            val thumbnailDir = applicationDirs.thumbnailDownloadsRoot
            File(thumbnailDir).let { if (!it.exists()) it.mkdir() }
            val filePath = getThumbnailDownloadPath(id)
            ImageResponse.saveImage(filePath, imageBytes.inputStream(), mimeType)

            transaction {
                MangaTable.update({ MangaTable.id eq id }) { update ->
                    update[thumbnail_url] = MangaList.proxyThumbnailUrl(id)
                    update[thumbnailUrlLastFetched] = System.currentTimeMillis()
                }
            }

            Manga.modifyMangasMetas(
                mapOf(
                    id to
                        mapOf(
                            Manga.COVER_MODE_KEY to Manga.MODE_CUSTOM,
                        ),
                ),
            )

            val sourceId = row[MangaTable.sourceReference]
            if (sourceId == LocalSource.ID) {
                val mangaUrl = row[MangaTable.url]
                val fileSystem = LocalSourceFileSystem(applicationDirs)
                val coverManager = LocalCoverManager(fileSystem)
                val sManga = SManga.create().apply { url = mangaUrl }
                coverManager.update(sManga, imageBytes.inputStream())
            }

            val manga =
                transaction {
                    MangaType(MangaTable.selectAll().where { MangaTable.id eq id }.first())
                }

            UploadMangaCoverPayload(
                clientMutationId = clientMutationId,
                manga = manga,
            )
        }
    }

    @RequireAuth
    fun updateManga(input: UpdateMangaInput): CompletableFuture<UpdateMangaPayload?> {
        val (clientMutationId, id, patch) = input

        return future {
            updateMangas(listOf(id), patch)

            val manga =
                transaction {
                    MangaType(MangaTable.selectAll().where { MangaTable.id eq id }.first())
                }

            UpdateMangaPayload(
                clientMutationId = clientMutationId,
                manga = manga,
            )
        }
    }

    @RequireAuth
    fun updateMangas(input: UpdateMangasInput): CompletableFuture<UpdateMangasPayload?> {
        val (clientMutationId, ids, patch) = input

        return future {
            updateMangas(ids, patch)

            val mangas =
                transaction {
                    MangaTable.selectAll().where { MangaTable.id inList ids }.map { MangaType(it) }
                }

            UpdateMangasPayload(
                clientMutationId = clientMutationId,
                mangas = mangas,
            )
        }
    }

    data class FetchMangaInput(
        val clientMutationId: String? = null,
        val id: Int,
    )

    data class FetchMangaPayload(
        val clientMutationId: String?,
        val manga: MangaType,
    )

    @RequireAuth
    fun fetchManga(input: FetchMangaInput): CompletableFuture<FetchMangaPayload?> {
        val (clientMutationId, id) = input

        return future {
            Manga.fetchManga(id)

            val manga =
                transaction {
                    MangaTable.selectAll().where { MangaTable.id eq id }.first()
                }
            FetchMangaPayload(
                clientMutationId = clientMutationId,
                manga = MangaType(manga),
            )
        }
    }

    data class SetMangaMetaInput(
        val clientMutationId: String? = null,
        val meta: MangaMetaType,
    )

    data class SetMangaMetaPayload(
        val clientMutationId: String?,
        val meta: MangaMetaType,
    )

    @RequireAuth
    fun setMangaMeta(input: SetMangaMetaInput): SetMangaMetaPayload? {
        val (clientMutationId, meta) = input

        Manga.modifyMangaMeta(meta.mangaId, meta.key, meta.value)

        return SetMangaMetaPayload(clientMutationId, meta)
    }

    data class DeleteMangaMetaInput(
        val clientMutationId: String? = null,
        val mangaId: Int,
        val key: String,
    )

    data class DeleteMangaMetaPayload(
        val clientMutationId: String?,
        val meta: MangaMetaType?,
        val manga: MangaType,
    )

    @RequireAuth
    fun deleteMangaMeta(input: DeleteMangaMetaInput): DeleteMangaMetaPayload? {
        val (clientMutationId, mangaId, key) = input

        val (meta, manga) =
            transaction {
                val meta =
                    MangaMetaTable
                        .selectAll()
                        .where { (MangaMetaTable.ref eq mangaId) and (MangaMetaTable.key eq key) }
                        .firstOrNull()

                MangaMetaTable.deleteWhere { (MangaMetaTable.ref eq mangaId) and (MangaMetaTable.key eq key) }

                val manga =
                    transaction {
                        MangaType(MangaTable.selectAll().where { MangaTable.id eq mangaId }.first())
                    }

                if (meta != null) {
                    MangaMetaType(meta)
                } else {
                    null
                } to manga
            }

        return DeleteMangaMetaPayload(clientMutationId, meta, manga)
    }

    data class SetMangaMetasItem(
        val mangaIds: List<Int>,
        val metas: List<MetaInput>,
    )

    data class SetMangaMetasInput(
        val clientMutationId: String? = null,
        val items: List<SetMangaMetasItem>,
    )

    data class SetMangaMetasPayload(
        val clientMutationId: String?,
        val metas: List<MangaMetaType>,
        val mangas: List<MangaType>,
    )

    @RequireAuth
    fun setMangaMetas(input: SetMangaMetasInput): SetMangaMetasPayload? {
        val (clientMutationId, items) = input

        val metaByMangaId =
            items
                .flatMap { item ->
                    val metaMap = item.metas.associate { it.key to it.value }
                    item.mangaIds.map { mangaId -> mangaId to metaMap }
                }.groupBy({ it.first }, { it.second })
                .mapValues { (_, maps) -> maps.reduce { acc, map -> acc + map } }

        Manga.modifyMangasMetas(metaByMangaId)

        val allMangaIds = metaByMangaId.keys
        val allMetaKeys = metaByMangaId.values.flatMap { it.keys }.distinct()

        val (updatedMetas, mangas) =
            transaction {
                val updatedMetas =
                    MangaMetaTable
                        .selectAll()
                        .where { (MangaMetaTable.ref inList allMangaIds) and (MangaMetaTable.key inList allMetaKeys) }
                        .map { MangaMetaType(it) }

                val mangas =
                    MangaTable
                        .selectAll()
                        .where { MangaTable.id inList allMangaIds }
                        .map { MangaType(it) }
                        .distinctBy { it.id }

                updatedMetas to mangas
            }

        return SetMangaMetasPayload(clientMutationId, updatedMetas, mangas)
    }

    data class DeleteMangaMetasItem(
        val mangaIds: List<Int>,
        val keys: List<String>? = null,
        val prefixes: List<String>? = null,
    )

    data class DeleteMangaMetasInput(
        val clientMutationId: String? = null,
        val items: List<DeleteMangaMetasItem>,
    )

    data class DeleteMangaMetasPayload(
        val clientMutationId: String?,
        val metas: List<MangaMetaType>,
        val mangas: List<MangaType>,
    )

    @RequireAuth
    fun deleteMangaMetas(input: DeleteMangaMetasInput): DeleteMangaMetasPayload? {
        val (clientMutationId, items) = input

        items.forEach { item ->
            require(!item.keys.isNullOrEmpty() || !item.prefixes.isNullOrEmpty()) {
                "Either 'keys' or 'prefixes' must be provided for each item"
            }
        }

        val (allDeletedMetas, allMangaIds) =
            transaction {
                val deletedMetas = mutableListOf<MangaMetaType>()
                val mangaIds = mutableSetOf<Int>()

                items.forEach { item ->
                    val keyCondition: Op<Boolean>? =
                        item.keys?.takeIf { it.isNotEmpty() }?.let { MangaMetaTable.key inList it }

                    val prefixCondition: Op<Boolean>? =
                        item.prefixes
                            ?.filter { it.isNotEmpty() }
                            ?.map { (MangaMetaTable.key like LikePattern("$it%")) as Op<Boolean> }
                            ?.reduceOrNull { acc, op -> acc or op }

                    val metaKeyCondition =
                        if (keyCondition != null && prefixCondition != null) {
                            keyCondition or prefixCondition
                        } else {
                            keyCondition ?: prefixCondition!!
                        }

                    val condition = (MangaMetaTable.ref inList item.mangaIds) and metaKeyCondition

                    deletedMetas +=
                        MangaMetaTable
                            .selectAll()
                            .where { condition }
                            .map { MangaMetaType(it) }

                    MangaMetaTable.deleteWhere { condition }
                    mangaIds += item.mangaIds
                }

                deletedMetas to mangaIds
            }

        val mangas =
            transaction {
                MangaTable
                    .selectAll()
                    .where { MangaTable.id inList allMangaIds }
                    .map { MangaType(it) }
                    .distinctBy { it.id }
            }

        return DeleteMangaMetasPayload(clientMutationId, allDeletedMetas, mangas)
    }
}

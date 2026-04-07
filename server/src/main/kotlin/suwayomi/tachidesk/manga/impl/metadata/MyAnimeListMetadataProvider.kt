package suwayomi.tachidesk.manga.impl.metadata

import androidx.core.net.toUri
import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.network.awaitSuccess
import eu.kanade.tachiyomi.network.parseAs
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import suwayomi.tachidesk.manga.impl.track.tracker.myanimelist.dto.MALManga
import uy.kohesive.injekt.injectLazy

class MyAnimeListMetadataProvider : MetadataProvider {
    private val networkHelper: NetworkHelper by injectLazy()
    private val json: Json by injectLazy()

    override val name: String = "MyAnimeList"

    override suspend fun search(
        query: String,
        author: String?,
    ): List<MetadataSearchResult> {
        val url =
            "https://api.myanimelist.net/v2/manga"
                .toUri()
                .buildUpon()
                // MAL API throws a 400 when the query is over 64 characters...
                .appendQueryParameter("q", query.take(64))
                .appendQueryParameter("nsfw", "true")
                .build()
                .toString()

        // NOTE: MyAnimeList API is OAuth-protected; without user auth headers this likely returns 401.
        val response: MalSearchResponse =
            with(json) {
                networkHelper.client
                    .newCall(GET(url))
                    .awaitSuccess()
                    .parseAs()
            }

        // Fetch details for each node id
        return response.data.map { node ->
            val details = getMangaDetails(node.node.id)
            MetadataSearchResult(
                externalId = details.id.toString(),
                title = details.title,
                author = null,
                coverUrl = details.covers?.large,
                year = details.startDate?.take(4)?.toIntOrNull(),
                description = details.synopsis,
            )
        }
    }

    override suspend fun getDetails(externalId: String): MetadataDetails {
        val details = getMangaDetails(externalId.toInt())

        return MetadataDetails(
            title = details.title,
            author = null,
            artist = null,
            description = details.synopsis,
            genre = null,
            status = mapStatus(details.status),
            coverUrl = details.covers?.large,
        )
    }

    private suspend fun getMangaDetails(id: Int): MALManga {
        val url =
            "https://api.myanimelist.net/v2/manga"
                .toUri()
                .buildUpon()
                .appendPath(id.toString())
                .appendQueryParameter(
                    "fields",
                    "id,title,synopsis,num_chapters,mean,main_picture,status,media_type,start_date",
                ).build()
                .toString()

        return with(json) {
            networkHelper.client
                .newCall(GET(url))
                .awaitSuccess()
                .parseAs()
        }
    }

    private fun mapStatus(status: String?): Int =
        when (status) {
            // MAL values are typically: "finished", "currently_publishing", "not_yet_published"
            "currently_publishing" -> 1
            "finished" -> 2
            "on_hiatus" -> 6
            "discontinued" -> 5
            else -> 0
        }
}

// --- MAL Search DTOs (for metadata provider only) ---

@Serializable
data class MalSearchResponse(
    val data: List<MalSearchNode>,
)

@Serializable
data class MalSearchNode(
    val node: MalSearchItem,
)

@Serializable
data class MalSearchItem(
    val id: Int,
)

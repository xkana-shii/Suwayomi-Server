package suwayomi.tachidesk.manga.impl.metadata

import androidx.core.net.toUri
import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.network.awaitSuccess
import eu.kanade.tachiyomi.network.parseAs
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import suwayomi.tachidesk.manga.impl.track.Track.htmlDecode
import uy.kohesive.injekt.injectLazy
import java.util.Locale

class MangaBakaMetadataProvider : MetadataProvider {
    private val networkHelper: NetworkHelper by injectLazy()
    private val json: Json by injectLazy()

    override val name: String = "MangaBaka"

    override suspend fun search(
        query: String,
        author: String?,
    ): List<MetadataSearchResult> {
        val url =
            "https://api.mangabaka.dev/v1/series/search"
                .toUri()
                .buildUpon()
                .appendQueryParameter("q", query)
                .appendQueryParameter("type_not", "novel")
                .build()
                .toString()

        val results: MangaBakaMetadataSearchResponse =
            with(json) {
                networkHelper.client
                    .newCall(GET(url))
                    .awaitSuccess()
                    .parseAs()
            }

        return results.data
            .filter { it.state != "merged" }
            .map { item ->
                MetadataSearchResult(
                    externalId = (item.mergedWith ?: item.id).toString(),
                    title = item.title,
                    author = item.authors?.joinToString(", "),
                    coverUrl = item.cover.x350.x3,
                    year =
                        item.published.startDate
                            ?.take(4)
                            ?.toIntOrNull(),
                    description = item.description?.htmlDecode() ?: "",
                )
            }
    }

    override suspend fun getDetails(externalId: String): MetadataDetails {
        val item = fetchSeriesData(externalId.toLong())

        return MetadataDetails(
            title = item.title,
            author = item.authors?.joinToString(", ")?.takeIf { it.isNotBlank() },
            artist = item.artists?.joinToString(", ")?.takeIf { it.isNotBlank() },
            description = item.description?.htmlDecode() ?: "",
            genre =
                item.tagsV2
                    ?.map { it.name }
                    ?.distinct()
                    ?.toMutableList()
                    ?.apply { add(item.type.replaceFirstChar { it.titlecase(Locale.getDefault()) }) }
                    ?.takeIf { it.isNotEmpty() },
            status = mapStatus(item.status),
            coverUrl = item.cover.raw.url ?: item.cover.x350.x3,
        )
    }

    private suspend fun fetchSeriesData(seriesId: Long): MangaBakaMetadataSeries {
        val url = "https://api.mangabaka.dev/v1/series/$seriesId"

        val result: MangaBakaMetadataSeriesResponse =
            with(json) {
                networkHelper.client
                    .newCall(GET(url))
                    .awaitSuccess()
                    .parseAs()
            }

        return result.data
    }

    private fun mapStatus(status: String?): Int =
        when (status?.lowercase(Locale.getDefault())) {
            "ongoing" -> 1
            "completed" -> 2
            "hiatus" -> 6
            "cancelled" -> 5
            else -> 0
        }
}

@Serializable
private data class MangaBakaMetadataSeriesResponse(
    val data: MangaBakaMetadataSeries,
)

@Serializable
private data class MangaBakaMetadataSearchResponse(
    val data: List<MangaBakaMetadataSeries>,
)

@Serializable
private data class MangaBakaMetadataSeries(
    val id: Long,
    val title: String,
    val cover: MangaBakaMetadataCover,
    val authors: List<String>?,
    val artists: List<String>?,
    val description: String?,
    val published: MangaBakaMetadataPublishData,
    val status: String,
    val type: String,
    val rating: Double?,
    @SerialName("total_chapters")
    val totalChapters: String?,
    val state: String? = null,
    @SerialName("merged_with")
    val mergedWith: Long? = null,
    @SerialName("tags_v2")
    val tagsV2: List<MangaBakaMetadataTag>? = null,
)

@Serializable
private data class MangaBakaMetadataTag(
    val name: String,
)

@Serializable
private data class MangaBakaMetadataCover(
    val raw: MangaBakaMetadataRawCover,
    val x150: MangaBakaMetadataScaledCover,
    val x250: MangaBakaMetadataScaledCover,
    val x350: MangaBakaMetadataScaledCover,
)

@Serializable
private data class MangaBakaMetadataRawCover(
    val url: String?,
)

@Serializable
private data class MangaBakaMetadataScaledCover(
    val x1: String?,
    val x2: String?,
    val x3: String?,
)

@Serializable
private data class MangaBakaMetadataPublishData(
    @SerialName("start_date")
    val startDate: String?,
)

package suwayomi.tachidesk.manga.impl.metadata

import androidx.core.net.toUri
import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.network.awaitSuccess
import eu.kanade.tachiyomi.network.parseAs
import kotlinx.serialization.json.Json
import suwayomi.tachidesk.manga.impl.track.Track.htmlDecode
import suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.dto.MangaBakaItem
import suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.dto.MangaBakaItemResult
import suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.dto.MangaBakaSearchResult
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

        val results: MangaBakaSearchResult =
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
                    year = item.published.startDate?.take(4)?.toIntOrNull(),
                    description = item.description?.htmlDecode()?.trim(),
                )
            }
    }

    override suspend fun getDetails(externalId: String): MetadataDetails {
        val item = fetchSeriesData(externalId.toLong())

        return MetadataDetails(
            title = item.title,
            author = item.authors?.joinToString(", ")?.takeIf { it.isNotBlank() },
            artist = item.artists?.joinToString(", ")?.takeIf { it.isNotBlank() },
            description = item.description?.htmlDecode()?.trim(),
            genre = null,
            status = mapStatus(item.status),
            coverUrl = item.cover.raw.url ?: item.cover.x350.x3,
        )
    }

    private suspend fun fetchSeriesData(seriesId: Long): MangaBakaItem {
        val url = "https://api.mangabaka.dev/v1/series/$seriesId"

        val result: MangaBakaItemResult =
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

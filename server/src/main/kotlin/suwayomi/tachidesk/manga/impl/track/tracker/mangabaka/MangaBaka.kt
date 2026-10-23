package suwayomi.tachidesk.manga.impl.track.tracker.mangabaka

import io.github.reactivecircus.cache4k.Cache
import kotlinx.serialization.json.Json
import suwayomi.tachidesk.manga.impl.track.tracker.DeletableTracker
import suwayomi.tachidesk.manga.impl.track.tracker.Tracker
import suwayomi.tachidesk.manga.impl.track.tracker.extractToken
import suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.dto.MangaBakaOAuth
import suwayomi.tachidesk.manga.impl.track.tracker.model.Track
import suwayomi.tachidesk.manga.impl.track.tracker.model.TrackSearch
import uy.kohesive.injekt.injectLazy
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.hours

class MangaBaka(
    id: Int,
) : Tracker(id, "MangaBaka"),
    DeletableTracker {
    private val json: Json by injectLazy()

    private val interceptors = ConcurrentHashMap<Int, MangaBakaInterceptor>()
    private val apis =
        Cache
            .Builder<Int, MangaBakaApi>()
            .expireAfterAccess(1.hours)
            .build()

    fun interceptor(userId: Int): MangaBakaInterceptor =
        interceptors.getOrPut(userId) {
            MangaBakaInterceptor(userId, this)
        }

    suspend fun api(userId: Int): MangaBakaApi =
        apis.get(userId) {
            MangaBakaApi(id, client, interceptor(userId))
        }

    override val supportsReadingDates: Boolean = true
    override val supportsPrivateTracking: Boolean = true

    // private val scorePreference = trackPreferences.mangabakaScoreType

    override fun getLogo(): String = "/static/tracker/manga_baka.png"

    override fun getStatusList(): List<Int> = listOf(READING, COMPLETED, PAUSED, DROPPED, PLAN_TO_READ, REREADING, CONSIDERING)

    override fun getStatus(status: Int): String? =
        when (status) {
            CONSIDERING -> "Considering"
            COMPLETED -> "Completed"
            DROPPED -> "Dropped"
            PAUSED -> "Paused"
            PLAN_TO_READ -> "Plan to read"
            READING -> "Reading"
            REREADING -> "Rereading"
            else -> null
        }

    override fun getReadingStatus(): Int = READING

    override fun getRereadingStatus(): Int = REREADING

    override fun getCompletionStatus(): Int = COMPLETED

    override fun getScoreList(userId: Int): List<String> =
        when (trackPreferences.getScoreType(userId, this)) {
            // 5, 10, ..., 95, 100
            STEP_5 -> IntRange(0, 100).step(5).map(Int::toString)

            // 10, 20, ..., 90, 100
            STEP_10 -> IntRange(0, 100).step(10).map(Int::toString)

            // 20, 40, ..., 80, 100
            STEP_20 -> IntRange(0, 100).step(20).map(Int::toString)

            // 25, 50, 75, 100
            STEP_25 -> IntRange(0, 100).step(25).map(Int::toString)

            // 1, 2, ..., 99, 100 (also used before the score type is known, i.e. while not logged in)
            else -> IntRange(0, 100).map(Int::toString)
        }

    override fun displayScore(
        userId: Int,
        track: Track,
    ): String = track.score.toInt().toString()

    override suspend fun update(
        userId: Int,
        track: Track,
        didReadChapter: Boolean,
    ): Track {
        if (track.status == PLAN_TO_READ || track.status == CONSIDERING) {
            track.started_reading_date = 0
        }

        if (track.status != COMPLETED && didReadChapter) {
            val mangaItem = api(userId).fetchSeriesData(track.remote_id)
            if (track.total_chapters > 0 && track.last_chapter_read.toInt() == track.total_chapters &&
                (mangaItem.status == "completed" || mangaItem.status == "cancelled")
            ) {
                track.status = COMPLETED
                track.finished_reading_date = System.currentTimeMillis()
            } else if (track.status != REREADING) {
                track.status = READING
                if (track.last_chapter_read == 1.0) {
                    track.started_reading_date = System.currentTimeMillis()
                }
            }
            return api(userId).updateLibManga(track, knownSeriesData = mangaItem)
        }

        return api(userId).updateLibManga(track)
    }

    override suspend fun bind(
        userId: Int,
        track: Track,
        hasReadChapters: Boolean,
    ): Track {
        val remoteTrack = api(userId).findLibManga(track)
        return if (remoteTrack != null) {
            track.copyPersonalFrom(remoteTrack, copyRemotePrivate = false)
            track.title = remoteTrack.title
            track.total_chapters = remoteTrack.total_chapters
            track.tracking_url = "${MangaBakaApi.BASE_URL}/${remoteTrack.remote_id}"
            track.remote_id = api(userId).resolveId(remoteTrack.remote_id)

            if (track.status != COMPLETED) {
                when (track.status) {
                    REREADING -> track.status
                    READING -> if (!hasReadChapters) track.status else READING
                    PAUSED, DROPPED, PLAN_TO_READ, CONSIDERING -> track.status
                    else -> if (hasReadChapters) READING else track.status
                }
            } else {
                try {
                    val seriesData = api(userId).fetchSeriesData(track.remote_id)
                    val isCompletedRemotely = seriesData.status == "completed" || seriesData.status == "cancelled"
                    val reachedEnd = track.total_chapters > 0 && track.last_chapter_read.toInt() == track.total_chapters
                    if (!(isCompletedRemotely && reachedEnd)) {
                        track.status = if (hasReadChapters) READING else PLAN_TO_READ
                        track.finished_reading_date = 0
                    }
                } catch (_: Exception) {
                }
            }

            update(userId, track, hasReadChapters)
        } else {
            // Set default fields if it's not found in the list
            track.tracking_url = "${MangaBakaApi.BASE_URL}/${track.remote_id}"
            track.status = if (hasReadChapters) READING else PLAN_TO_READ
            track.score = 0.0

            api(userId).addLibManga(track).also { track.title = it.title }
        }
    }

    override suspend fun search(
        userId: Int,
        query: String,
    ): List<TrackSearch> {
        val normalized =
            TRACKER_PATTERNS
                .firstNotNullOfOrNull { (tracker, prefix) ->
                    tracker
                        .find(query)
                        ?.groupValues
                        ?.get(1)
                        ?.let { "$prefix$it" }
                } ?: query

        return api(userId).search(normalized)
    }

    override suspend fun refresh(
        userId: Int,
        track: Track,
    ): Track {
        val remoteTrack = api(userId).findLibManga(track)
        track.remote_id = remoteTrack?.remote_id ?: track.remote_id
        track.tracking_url = "${MangaBakaApi.BASE_URL}/${track.remote_id}"

        if (remoteTrack != null) {
            track.copyPersonalFrom(remoteTrack)
            track.title = remoteTrack.title
            track.total_chapters = remoteTrack.total_chapters

            if (track.status == COMPLETED) {
                try {
                    val resolvedId = api(userId).resolveId(track.remote_id)
                    val seriesData = api(userId).fetchSeriesData(resolvedId)
                    val isCompletedRemotely = seriesData.status == "completed" || seriesData.status == "cancelled"
                    val reachedEnd = track.total_chapters > 0 && track.last_chapter_read.toInt() == track.total_chapters
                    if (!(isCompletedRemotely && reachedEnd)) {
                        track.status = if (track.last_chapter_read > 0.0) READING else PLAN_TO_READ
                        track.finished_reading_date = 0
                    }
                } catch (_: Exception) {
                }
            }
        }
        return track
    }

    override fun authUrl(): String = MangaBakaApi.authUrl().toString()

    override suspend fun authCallback(
        userId: Int,
        url: String,
    ) {
        val code = url.extractToken("code") ?: throw IOException("cannot find token")
        login(userId, "user", code)
    }

    // [Tracker.login] logs out again when this throws
    override suspend fun loginImpl(
        userId: Int,
        username: String,
        password: String,
    ) {
        val oauth = api(userId).getAccessToken(password)
        interceptor(userId).setAuth(oauth)
        saveCredentials(userId, "user", oauth.accessToken)
        val scoreType =
            when (val scoreStep = api(userId).getScoreStepSize()) {
                1 -> STEP_1
                5 -> STEP_5
                10 -> STEP_10
                20 -> STEP_20
                25 -> STEP_25
                else -> throw Exception("Unknown score step size $scoreStep")
            }
        trackPreferences.setScoreType(userId, this, scoreType)
    }

    fun saveToken(
        userId: Int,
        oauth: MangaBakaOAuth?,
    ) {
        trackPreferences.setTrackToken(userId, this, json.encodeToString(oauth))
    }

    fun restoreToken(userId: Int): MangaBakaOAuth? =
        try {
            json.decodeFromString<MangaBakaOAuth>(trackPreferences.getTrackToken(userId, this)!!)
        } catch (_: Exception) {
            null
        }

    override suspend fun logout(userId: Int) {
        super.logout(userId)
        trackPreferences.setTrackToken(userId, this, null)
        interceptor(userId).setAuth(null)
    }

    /*
    override suspend fun getMangaMetadata(track: Track): TrackMangaMetadata {
        return api.getMangaMetadata(track)
    }
     */

    override suspend fun delete(
        userId: Int,
        track: Track,
    ) {
        api(userId).deleteLibManga(track)
    }

    companion object {
        const val READING = 1
        const val COMPLETED = 2
        const val PAUSED = 3
        const val DROPPED = 4
        const val PLAN_TO_READ = 5
        const val REREADING = 6
        const val CONSIDERING = 7

        const val STEP_1 = "STEP_1"
        const val STEP_5 = "STEP_5"
        const val STEP_10 = "STEP_10"
        const val STEP_20 = "STEP_20"
        const val STEP_25 = "STEP_25"

        private val TRACKER_PATTERNS =
            listOf(
                Regex("""myanimelist\.net/manga/(\d+)""") to "mal:",
                Regex("""mangaupdates\.com/series/([^/?#]+)""") to "mu:",
                Regex("""anilist\.co/manga/(\d+)""") to "al:",
            )
    }
}

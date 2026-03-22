package suwayomi.tachidesk.manga.impl.track.tracker.mangabaka

import dev.icerock.moko.resources.StringResource
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.json.Json
import org.jetbrains.kotlin.kotlinx.collections.immutable.ImmutableList
import org.jetbrains.kotlin.kotlinx.collections.immutable.toImmutableList
import suwayomi.tachidesk.manga.impl.track.tracker.DeletableTracker
import suwayomi.tachidesk.manga.impl.track.tracker.Tracker
import suwayomi.tachidesk.manga.impl.track.tracker.bangumi.dto.BGMOAuth
import suwayomi.tachidesk.manga.impl.track.tracker.extractToken
import suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.dto.MangaBakaOAuth
import suwayomi.tachidesk.manga.impl.track.tracker.model.Track
import suwayomi.tachidesk.manga.impl.track.tracker.model.TrackSearch
import uy.kohesive.injekt.injectLazy
import java.io.IOException
import kotlin.compareTo
import kotlin.text.get
import kotlin.text.set
import kotlin.text.toLong

class MangaBaka(id: Int) : Tracker(id, "MangaBaka"), DeletableTracker {

    private val json: Json by injectLazy()

    private val interceptor by lazy { MangaBakaInterceptor(this) }
    private val api by lazy { MangaBakaApi(id, client, interceptor) }

    override val supportsReadingDates: Boolean = true
    override val supportsPrivateTracking: Boolean = true

    override fun getLogo(): String = "/static/tracker/mangabaka.png"

    override fun getStatusList(): List<Int> {
        return listOf(READING, COMPLETED, PAUSED, DROPPED, PLAN_TO_READ, REREADING, CONSIDERING)
    }

    override fun getStatus(status: Int): String? = when (status) {
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

    override fun getScoreList(): ImmutableList<String> {
        return when (trackPreferences.getScoreType(this)) {
            // 1, 2, ..., 99, 100
            STEP_1 -> IntRange(0, 100).map(Int::toString).toImmutableList()
            // 5, 10, ..., 95, 100
            STEP_5 -> IntRange(0, 100).step(5).map(Int::toString).toImmutableList()
            // 10, 20, ..., 90, 100
            STEP_10 -> IntRange(0, 100).step(10).map(Int::toString).toImmutableList()
            // 20, 40, ..., 80, 100
            STEP_20 -> IntRange(0, 100).step(20).map(Int::toString).toImmutableList()
            // 25, 50, 75, 100
            STEP_25 -> IntRange(0, 100).step(25).map(Int::toString).toImmutableList()
            else -> throw Exception("Unknown score type")
        }
    }

    override fun displayScore(track: Track): String = track.score.toInt().toString()

    override suspend fun update(
        track: Track,
        didReadChapter: Boolean,
    ): Track {
        if (track.status == PLAN_TO_READ || track.status == CONSIDERING) {
            track.started_reading_date = 0
        }

        val mangaItem = api.getMangaItem(track.remote_id)

        if (track.status != COMPLETED && didReadChapter) {
            if (
                track.total_chapters > 0 &&
                track.last_chapter_read.toLong() == track.total_chapters &&
                mangaItem.status == "completed"
            ) {
                track.status = COMPLETED
            } else if (track.status != REREADING) {
                track.status = READING
            }
        }

        return api.updateLibManga(track)
    }

    override suspend fun bind(
        track: Track,
        hasReadChapters: Boolean,
    ): Track {
        val remoteTrack = api.findLibManga(track)
        return if (remoteTrack != null) {
            track.copyPersonalFrom(remoteTrack, copyRemotePrivate = false)
            track.title = remoteTrack.title
            track.remote_id = remoteTrack.remote_id
            track.tracking_url = "${MangaBakaApi.BASE_URL}/${track.remote_id}"
            track.total_chapters = remoteTrack.total_chapters

            if (track.status != COMPLETED) {
                val isRereading = track.status == REREADING
                track.status = if (!isRereading && hasReadChapters) READING else track.status
            }

            update(track, hasReadChapters)
        } else {
            // Set default fields if it's not found in the list
            track.tracking_url = "${MangaBakaApi.BASE_URL}/${track.remote_id}"
            track.status = if (hasReadChapters) READING else PLAN_TO_READ
            track.score = 0.0

            api.addLibManga(track).also { track.title = it.title }
        }
    }

    override suspend fun search(query: String): List<TrackSearch> {
        return api.search(query)
    }

    override suspend fun refresh(track: Track): Track {
        val remoteTrack = api.findLibManga(track)
        track.remote_id = remoteTrack?.remote_id ?: track.remote_id
        track.tracking_url = "${MangaBakaApi.BASE_URL}/${track.remote_id}"
        if (remoteTrack != null) {
            track.copyPersonalFrom(remoteTrack)
            track.title = remoteTrack.title
            track.total_chapters = remoteTrack.total_chapters
        }
        return track
    }

    override suspend fun login(username: String, password: String) = login(password)

    suspend fun login(code: String) {
        try {
            val oauth = api.getAccessToken(code)
            interceptor.setAuth(oauth)
            saveCredentials("user", oauth.accessToken)
            val scoreType = when (val scoreStep = api.getScoreStepSize()) {
                1 -> STEP_1
                5 -> STEP_5
                10 -> STEP_10
                20 -> STEP_20
                25 -> STEP_25
                else -> throw Exception("Unknown score step size $scoreStep")
            }
            trackPreferences.setScoreType(this, scoreType)
        } catch (_: Exception) {
            logout()
        }
    }

    fun saveToken(oauth: MangaBakaOAuth?) {
        trackPreferences.setTrackToken(this, json.encodeToString(oauth))
    }

    fun restoreToken(): MangaBakaOAuth? {
        return try {
            json.decodeFromString<MangaBakaOAuth>(trackPreferences.getTrackToken(this)!!)
        } catch (_: Exception) {
            null
        }
    }

    override fun logout() {
        super.logout()
        trackPreferences.setTrackToken(this, null)
        interceptor.setAuth(null)
    }

    /* override suspend fun getMangaMetadata(track: DomainTrack): TrackMangaMetadata {
        return api.getMangaMetadata(track)
    } */

    override suspend fun delete(track: Track) {
        api.deleteLibManga(track)
    }

    companion object {
        const val READING = 1L
        const val COMPLETED = 2L
        const val PAUSED = 3L
        const val DROPPED = 4L
        const val PLAN_TO_READ = 5L
        const val REREADING = 6L
        const val CONSIDERING = 7L

        const val STEP_1 = "STEP_1"
        const val STEP_5 = "STEP_5"
        const val STEP_10 = "STEP_10"
        const val STEP_20 = "STEP_20"
        const val STEP_25 = "STEP_25"
    }
}

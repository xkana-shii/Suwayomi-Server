package suwayomi.tachidesk.manga.impl.track.tracker.mangabaka

import kotlinx.serialization.json.Json
import okhttp3.Interceptor
import okhttp3.Response
import suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.dto.MangaBakaOAuth
import suwayomi.tachidesk.server.generated.BuildConfig
import uy.kohesive.injekt.injectLazy
import kotlin.getValue

class MangaBakaInterceptor(private val mangaBaka: MangaBaka) : Interceptor {

    private val json: Json by injectLazy()

    private var oauth: MangaBakaOAuth? = mangaBaka.restoreToken()

    override fun intercept(chain: Interceptor.Chain): Response {
        val originalRequest = chain.request()

        var currentAuth = oauth ?: throw Exception("Not authenticated with MangaBaka")

        if (currentAuth.isExpired()) {
            val response = chain.proceed(MangaBakaApi.refreshTokenRequest(currentAuth.refreshToken))
            if (response.isSuccessful) {
                currentAuth = json.decodeFromString(response.body.string())
                setAuth(currentAuth)
            } else {
                response.close()
            }
        }

        return originalRequest.newBuilder()
            .header("User-Agent", "Suwayomi/Suwayomi-Server/${BuildConfig.VERSION} (${BuildConfig.GITHUB})")
            .addHeader("Authorization", "Bearer ${currentAuth.accessToken}")
            .build()
            .let(chain::proceed)
    }

    fun setAuth(oauth: MangaBakaOAuth?) {
        this.oauth = oauth

        mangaBaka.saveToken(oauth)
    }
}

package suwayomi.tachidesk.manga.impl.track.tracker.mangabaka.dto

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class MangaBakaUserInfo(
    // incomplete DTO since this is the only part we need
    @SerialName("rating_steps")
    val ratingSteps: Int,
)

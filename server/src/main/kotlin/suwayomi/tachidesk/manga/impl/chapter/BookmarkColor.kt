package suwayomi.tachidesk.manga.impl.chapter

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

/**
 * fork: colored bookmarks (Komikku port). The ids are Komikku's, so backups exchange colors unchanged; the color is
 * only meaningful while the chapter is bookmarked, 0 is the default bookmark color.
 */
object BookmarkColor {
    const val DEFAULT = 0
    const val RED = 1
    const val ORANGE = 2
    const val YELLOW = 3
    const val GREEN = 4
    const val BLUE = 5
    const val PURPLE = 6

    fun isValid(color: Int): Boolean = color in DEFAULT..PURPLE
}

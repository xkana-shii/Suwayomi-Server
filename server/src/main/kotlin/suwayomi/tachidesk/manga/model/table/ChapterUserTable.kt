package suwayomi.tachidesk.manga.model.table

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ReferenceOption
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.dao.id.IntIdTable
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.isNull
import org.jetbrains.exposed.v1.core.leftJoin
import org.jetbrains.exposed.v1.core.or
import suwayomi.tachidesk.global.model.table.UserAccountTable

object ChapterUserTable : IntIdTable() {
    val chapter = reference("chapter", ChapterTable, ReferenceOption.CASCADE)
    val user = reference("user_id", UserAccountTable, ReferenceOption.CASCADE)

    val isRead = bool("read").default(false)
    val isBookmarked = bool("bookmark").default(false)

    // fork (Komikku port): bookmark color, see suwayomi.tachidesk.manga.impl.chapter.BookmarkColor
    val bookmarkColor = integer("bookmark_color").default(0)

    // fork (Komikku port): derived visibility, recomputed by suwayomi.tachidesk.manga.impl.chapter.ChapterVisibilityService
    val isBlacklisted = bool("blacklisted").default(false)
    val isMergeHidden = bool("merge_hidden").default(false)
    val lastPageRead = integer("last_page_read").default(0)
    val lastReadAt = long("last_read_at").default(0)

    val isDownloaded = bool("is_downloaded").default(false)
    val isDownloadRequested = bool("is_download_requested").default(false)

    val koreaderHash = varchar("koreader_hash", 32).nullable()

    // syncyomi
    val version = long("version").default(0)
    val isSyncing = bool("is_syncing").default(false)
    val lastModifiedAt = long("last_modified_at").default(0)

    init {
        uniqueIndex(chapter, user)
    }
}

fun ChapterTable.getWithUserData(userId: Int) =
    leftJoin(
        ChapterUserTable,
        onColumn = { ChapterTable.id },
        otherColumn = { ChapterUserTable.chapter },
        additionalConstraint = { ChapterUserTable.user eq userId },
    )

/**
 * fork: the chapter is not hidden by the chapter blacklist or Smart Scanlator Merge for the joined user
 * (see [suwayomi.tachidesk.manga.impl.chapter.ChapterVisibilityService]); chapters without user data are visible.
 */
fun chapterVisibleOp(): Op<Boolean> =
    (ChapterUserTable.isBlacklisted.isNull() or (ChapterUserTable.isBlacklisted eq false)) and
        (ChapterUserTable.isMergeHidden.isNull() or (ChapterUserTable.isMergeHidden eq false))

package suwayomi.tachidesk.graphql.types

import org.jetbrains.exposed.v1.core.ResultRow
import suwayomi.tachidesk.graphql.server.primitives.Node
import suwayomi.tachidesk.manga.model.table.ChapterUserTable

class ChapterUserType(
    val isRead: Boolean,
    val isBookmarked: Boolean,
    val lastPageRead: Int,
    val lastReadAt: Long,
    val isDownloaded: Boolean,
    val chapterId: Int,
    // fork (Komikku port)
    val bookmarkColor: Int = 0,
    val isBlacklisted: Boolean = false,
    val isMergeHidden: Boolean = false,
) : Node {
    constructor(row: ResultRow) : this(
        row[ChapterUserTable.isRead],
        row[ChapterUserTable.isBookmarked],
        row[ChapterUserTable.lastPageRead],
        row[ChapterUserTable.lastReadAt],
        row[ChapterUserTable.isDownloaded],
        row[ChapterUserTable.chapter].value,
        row[ChapterUserTable.bookmarkColor],
        row[ChapterUserTable.isBlacklisted],
        row[ChapterUserTable.isMergeHidden],
    )
}

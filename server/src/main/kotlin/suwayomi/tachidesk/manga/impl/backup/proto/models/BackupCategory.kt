package suwayomi.tachidesk.manga.impl.backup.proto.models

import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoNumber

// Scalars need defaults (proto3 peers omit zero values); see BackupManga.
// A data class so that SyncManager's category list comparison is structural.
@Serializable
data class BackupCategory(
    @ProtoNumber(1) @EncodeDefault var name: String = "",
    @ProtoNumber(2) var order: Int = 0,
    // @ProtoNumber(3) val updateInterval: Int = 0, 1.x value not used in 0.x
    // Bump by 100 to specify this is a 0.x value
    @ProtoNumber(100) var flags: Int = 0,
    // syncyomi
    @ProtoNumber(601) var version: Long = 0,
    @ProtoNumber(602) var uid: Long = 0,
    @ProtoNumber(603) var lastModifiedAt: Long = 0,
    // suwayomi
    @ProtoNumber(9000) var meta: Map<String, String> = emptyMap(),
    // fork (Komikku port): nested categories and Smart Categories, Komikku's numbers so backups are exchangeable
    /** this category's id in the backup, referenced by [parentId] */
    @ProtoNumber(3) var id: Long = 0,
    /** the parent's [id] in this backup (0 = top level, or unknown without [hierarchyKnown]) */
    @ProtoNumber(901) var parentId: Long = 0,
    /** Smart Category binding (unified tracker status) */
    @ProtoNumber(902) var trackStatus: Int? = null,
    /** the parent's sync uid (0 = top level or unknown) */
    @ProtoNumber(904) var parentUid: Long = 0,
    /** set by backups that know about the hierarchy: without it a 0 parent means "unknown" rather than "top level" */
    @ProtoNumber(907) var hierarchyKnown: Boolean = false,
)

package suwayomi.tachidesk.manga.model.table

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.jetbrains.exposed.v1.core.ReferenceOption
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.dao.id.IntIdTable
import suwayomi.tachidesk.global.model.table.UserAccountTable
import suwayomi.tachidesk.manga.model.dataclass.CategoryDataClass
import suwayomi.tachidesk.manga.model.dataclass.IncludeOrExclude

object CategoryTable : IntIdTable() {
    val name = varchar("name", 64)
    val order = integer("sort_order").default(0)
    val isDefault = bool("is_default").default(false)
    val includeInUpdate = integer("include_in_update").default(IncludeOrExclude.UNSET.value)
    val includeInDownload = integer("include_in_download").default(IncludeOrExclude.UNSET.value)
    val isDefaultCategory = bool("is_default_category").default(false)

    // Tachiyomi library sort/display bitmask, passthrough for sync
    val flags = integer("flags").default(0)

    val version = long("version").default(0)
    val uid = long("uid").default(0)
    val lastModifiedAt = long("last_modified_at").default(0)
    val isSyncing = bool("is_syncing").default(false)
    val user = reference("user_id", UserAccountTable, ReferenceOption.CASCADE)

    // fork (Komikku port): nested categories (null = top level) and Smart Category tracker status binding
    val parentId = integer("parent_id").nullable()
    val trackStatus = integer("track_status").nullable()

    // fork (Komikku port): hidden categories keep their manga and settings but are left out of the library tabs
    val hidden = bool("hidden").default(false)

    // fork (Komikku port): library update interval in days (null = inherit / global schedule) and the last scheduled
    // update that covered this category (epoch seconds)
    val updateInterval = integer("update_interval").nullable()
    val lastScheduledUpdateAt = long("last_scheduled_update_at").default(0)

    init {
        index(false, id, user)
    }
}

fun CategoryTable.toDataClass(categoryEntry: ResultRow) =
    CategoryDataClass(
        id = categoryEntry[id].value,
        order = categoryEntry[order],
        name = categoryEntry[name],
        default = categoryEntry[isDefault],
        includeInUpdate = IncludeOrExclude.fromValue(categoryEntry[includeInUpdate]),
        includeInDownload = IncludeOrExclude.fromValue(categoryEntry[includeInDownload]),
        isDefaultCategory = categoryEntry[isDefaultCategory],
        version = categoryEntry[version],
        uid = categoryEntry[uid],
        lastModifiedAt = categoryEntry[lastModifiedAt],
        parentId = categoryEntry[parentId],
        trackStatus = categoryEntry[trackStatus],
        hidden = categoryEntry[hidden],
        updateInterval = categoryEntry[updateInterval],
        lastScheduledUpdateAt = categoryEntry[lastScheduledUpdateAt],
    )

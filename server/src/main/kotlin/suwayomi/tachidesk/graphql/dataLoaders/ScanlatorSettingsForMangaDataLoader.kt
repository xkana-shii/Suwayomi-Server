/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

package suwayomi.tachidesk.graphql.dataLoaders

import com.expediagroup.graphql.dataloader.KotlinDataLoader
import graphql.GraphQLContext
import org.dataloader.DataLoader
import org.dataloader.DataLoaderFactory
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.graphql.server.getAttribute
import suwayomi.tachidesk.graphql.server.primitives.inIds
import suwayomi.tachidesk.graphql.types.MangaScanlatorSettingsType
import suwayomi.tachidesk.manga.impl.chapter.ChapterVisibilityService
import suwayomi.tachidesk.manga.impl.chapter.ScanlatorSettings
import suwayomi.tachidesk.manga.model.table.ChapterTable
import suwayomi.tachidesk.manga.model.table.ChapterUserTable
import suwayomi.tachidesk.manga.model.table.chapterVisibleOp
import suwayomi.tachidesk.server.JavalinSetup
import suwayomi.tachidesk.server.JavalinSetup.future
import suwayomi.tachidesk.server.settings.userConfig
import suwayomi.tachidesk.server.settings.value
import suwayomi.tachidesk.server.user.requireUser

/** fork (Komikku port) */
class ScanlatorSettingsForMangaDataLoader : KotlinDataLoader<Int, MangaScanlatorSettingsType> {
    override val dataLoaderName = "ScanlatorSettingsForMangaDataLoader"

    override fun getDataLoader(graphQLContext: GraphQLContext): DataLoader<Int, MangaScanlatorSettingsType> =
        DataLoaderFactory.newDataLoader { ids ->
            future {
                val userId = graphQLContext.getAttribute(JavalinSetup.Attribute.TachideskUser).requireUser()
                val global = userConfig.smartScanlatorMerge.value(userId)
                val settingsByManga = ScanlatorSettings.getAll(userId, ids)
                val chaptersByManga = ChapterVisibilityService.loadChapters(ids)
                ids.map { mangaId ->
                    MangaScanlatorSettingsType.of(
                        mangaId,
                        settingsByManga[mangaId] ?: ScanlatorSettings(),
                        chaptersByManga[mangaId].orEmpty(),
                        global,
                    )
                }
            }
        }
}

/** fork (Komikku port): the bookmark colors of a manga's bookmarked, visible chapters (library color filter) */
class BookmarkColorsForMangaDataLoader : KotlinDataLoader<Int, List<Int>> {
    override val dataLoaderName = "BookmarkColorsForMangaDataLoader"

    override fun getDataLoader(graphQLContext: GraphQLContext): DataLoader<Int, List<Int>> =
        DataLoaderFactory.newDataLoader { ids ->
            future {
                val userId = graphQLContext.getAttribute(JavalinSetup.Attribute.TachideskUser).requireUser()
                val colorsByManga =
                    transaction {
                        ChapterTable
                            .innerJoin(ChapterUserTable)
                            .select(ChapterTable.manga, ChapterUserTable.bookmarkColor)
                            .where {
                                (ChapterTable.manga inIds ids) and (ChapterUserTable.user eq userId) and
                                    (ChapterUserTable.isBookmarked eq true) and chapterVisibleOp()
                            }.withDistinct()
                            .groupBy({ it[ChapterTable.manga].value }, { it[ChapterUserTable.bookmarkColor] })
                    }
                ids.map { colorsByManga[it].orEmpty().sorted() }
            }
        }
}

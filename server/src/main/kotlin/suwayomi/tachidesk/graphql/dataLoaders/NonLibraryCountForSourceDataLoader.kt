package suwayomi.tachidesk.graphql.dataLoaders

import com.expediagroup.graphql.dataloader.KotlinDataLoader
import graphql.GraphQLContext
import org.dataloader.DataLoader
import org.dataloader.DataLoaderFactory
import org.jetbrains.exposed.v1.core.Slf4jSqlDebugLogger
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.count
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.isNull
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.graphql.server.getAttribute
import suwayomi.tachidesk.graphql.server.primitives.inIds
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.manga.model.table.MangaUserTable
import suwayomi.tachidesk.manga.model.table.getWithUserData
import suwayomi.tachidesk.server.JavalinSetup
import suwayomi.tachidesk.server.JavalinSetup.future
import suwayomi.tachidesk.server.user.requireUser

/**
 * Number of mangas of a source that are not in the requesting user's library.
 */
class NonLibraryCountForSourceDataLoader : KotlinDataLoader<Long, Int> {
    override val dataLoaderName = "NonLibraryCountForSourceDataLoader"

    override fun getDataLoader(graphQLContext: GraphQLContext): DataLoader<Long, Int> =
        DataLoaderFactory.newDataLoader { ids ->
            future {
                val userId = graphQLContext.getAttribute(JavalinSetup.Attribute.TachideskUser).requireUser()
                transaction {
                    addLogger(Slf4jSqlDebugLogger)
                    val count = MangaTable.id.count()
                    val countBySourceId =
                        MangaTable
                            .getWithUserData(userId)
                            .select(MangaTable.sourceReference, count)
                            .where {
                                (MangaTable.sourceReference inIds ids) and
                                    (MangaUserTable.inLibrary.isNull() or (MangaUserTable.inLibrary eq false))
                            }.groupBy(MangaTable.sourceReference)
                            .associate { it[MangaTable.sourceReference] to it[count].toInt() }

                    ids.map { countBySourceId[it] ?: 0 }
                }
            }
        }
}

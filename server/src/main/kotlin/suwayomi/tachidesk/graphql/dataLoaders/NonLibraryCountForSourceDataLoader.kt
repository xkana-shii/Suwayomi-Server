package suwayomi.tachidesk.graphql.dataLoaders

import com.expediagroup.graphql.dataloader.KotlinDataLoader
import graphql.GraphQLContext
import org.dataloader.DataLoader
import org.dataloader.DataLoaderFactory
import org.jetbrains.exposed.v1.core.Slf4jSqlDebugLogger
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.count
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import suwayomi.tachidesk.manga.model.table.MangaTable
import suwayomi.tachidesk.server.JavalinSetup.future

class NonLibraryCountForSourceDataLoader : KotlinDataLoader<Long, Int> {
    override val dataLoaderName = "NonLibraryCountForSourceDataLoader"

    override fun getDataLoader(graphQLContext: GraphQLContext): DataLoader<Long, Int> =
        DataLoaderFactory.newDataLoader { ids ->
            future {
                transaction {
                    addLogger(Slf4jSqlDebugLogger)
                    // Query grouped counts by sourceReference for mangas not in library
                    val countExpr = MangaTable.id.count()
                    val countsBySource =
                        MangaTable
                            .select(MangaTable.sourceReference, countExpr)
                            .where { (MangaTable.sourceReference inList ids) and (MangaTable.inLibrary eq false) }
                            .groupBy(MangaTable.sourceReference)
                            .associate { row ->
                                val src = row[MangaTable.sourceReference]
                                val cnt = row[countExpr] ?: 0L
                                src to cnt.toInt()
                            }

                    ids.map { countsBySource[it] ?: 0 }
                }
            }
        }
}

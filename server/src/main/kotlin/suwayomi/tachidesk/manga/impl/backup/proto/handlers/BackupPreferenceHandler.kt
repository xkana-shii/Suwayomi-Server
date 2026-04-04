package suwayomi.tachidesk.manga.impl.backup.proto.handlers

import eu.kanade.tachiyomi.source.sourcePreferences
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import suwayomi.tachidesk.manga.impl.backup.BackupFlags
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupPreference
import suwayomi.tachidesk.manga.impl.backup.proto.models.BackupSourcePreferences
import suwayomi.tachidesk.manga.impl.backup.proto.models.BooleanPreferenceValue
import suwayomi.tachidesk.manga.impl.backup.proto.models.FloatPreferenceValue
import suwayomi.tachidesk.manga.impl.backup.proto.models.IntPreferenceValue
import suwayomi.tachidesk.manga.impl.backup.proto.models.LongPreferenceValue
import suwayomi.tachidesk.manga.impl.backup.proto.models.PreferenceValue
import suwayomi.tachidesk.manga.impl.backup.proto.models.StringPreferenceValue
import suwayomi.tachidesk.manga.impl.backup.proto.models.StringSetPreferenceValue
import suwayomi.tachidesk.manga.model.table.SourceTable

object BackupPreferenceHandler {
    private fun sourceKey(sourceId: Long): String = "source_$sourceId"

    fun backup(flags: BackupFlags): List<BackupSourcePreferences> {
        if (!flags.includeClientData) return emptyList()

        return transaction {
            SourceTable.selectAll().map { it[SourceTable.id].value }
        }.mapNotNull { sourceId ->
            val key = sourceKey(sourceId)
            val prefs = sourcePreferences(key).all.toBackupPreferences()
            prefs.takeIf { it.isNotEmpty() }?.let { BackupSourcePreferences(sourceKey = key, prefs = it) }
        }
    }

    fun restore(backupSourcePreferences: List<BackupSourcePreferences>) {
        backupSourcePreferences.forEach { sourcePref ->
            val sourceId = sourcePref.sourceKey.removePrefix("source_").toLongOrNull() ?: return@forEach
            sourcePreferences(sourceKey(sourceId)).edit().apply {
                sourcePref.prefs.forEach { pref ->
                    when (val v = pref.value) {
                        is IntPreferenceValue -> putInt(pref.key, v.value)
                        is LongPreferenceValue -> putLong(pref.key, v.value)
                        is FloatPreferenceValue -> putFloat(pref.key, v.value)
                        is StringPreferenceValue -> putString(pref.key, v.value)
                        is BooleanPreferenceValue -> putBoolean(pref.key, v.value)
                        is StringSetPreferenceValue -> putStringSet(pref.key, v.value.toMutableSet())
                    }
                }
                apply()
            }
        }
    }
}

@Suppress("UNCHECKED_CAST")
private fun Map<String, *>.toBackupPreferences(): List<BackupPreference> =
    mapNotNull { (key, value) ->
        val prefValue: PreferenceValue =
            when (value) {
                is Int -> IntPreferenceValue(value)
                is Long -> LongPreferenceValue(value)
                is Float -> FloatPreferenceValue(value)
                is String -> StringPreferenceValue(value)
                is Boolean -> BooleanPreferenceValue(value)
                is Set<*> -> (value as? Set<String>)?.let { StringSetPreferenceValue(it) } ?: return@mapNotNull null
                else -> return@mapNotNull null
            }
        BackupPreference(key = key, value = prefValue)
    }.sortedBy { it.key }

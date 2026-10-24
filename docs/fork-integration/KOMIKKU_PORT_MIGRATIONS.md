# Komikku port — schema, backup and migration notes

## Database (ForkSchema, idempotent, runs after upstream migrations; no numbered migrations)

| Table | Column | Type / default | Meaning |
|---|---|---|---|
| CHAPTERUSER | BOOKMARK_COLOR | INT, 0 | bookmark color (0 = default) |
| CHAPTERUSER | BLACKLISTED | BOOLEAN, false | derived: hidden by the blacklist |
| CHAPTERUSER | MERGE_HIDDEN | BOOLEAN, false | derived: duplicate hidden by Smart Scanlator Merge |
| MANGAUSER | BLACKLISTED_CHAPTERS | text NULL | JSON list of blacklist keys |
| MANGAUSER | SCANLATOR_PRIORITY | text NULL | JSON list |
| MANGAUSER | SCANLATOR_RANGE_RULES | text NULL | JSON list of `from:to:scanlator` |
| MANGAUSER | SMART_MERGE_OVERRIDE | BOOLEAN NULL | NULL = follow the global setting |
| CATEGORY | PARENT_ID | INT NULL | NULL = top level (index `FORK_CATEGORY_PARENT_ID`) |
| CATEGORY | TRACK_STATUS | INT NULL | Smart Category binding (unified status 1–7) |

All columns are additive with defaults that reproduce the previous behaviour; no backfill is needed. Text columns
use the upstream `UNLIMITED_TEXT` helper (H2 `VARCHAR`, PostgreSQL `TEXT`). The derived flags are recomputed from
the rules and never carry information of their own.

Per-user setting: `smartScanlatorMerge` (user setting proto **1000**; fork user settings start at 1000 so upstream
additions never collide). Client settings live in global/manga meta (`webUI_*` keys) like the other WebUI settings.

## Backup (protobuf) — Komikku's numbers, so backups are exchangeable

| Message | Field | Notes |
|---|---|---|
| BackupChapter | 14 `bookmarkColor: Int?` | null = unknown → keep the local color |
| BackupChapter | 15 `fillermark` | unchanged: restores as a fillermark (not converted to the blacklist) |
| BackupManga | 614 `scanlatorPriority`, 615 `blacklistedChapters`, 616 `scanlatorRangeRules` | |
| BackupManga | 618 `smartScanlatorMergeOverride` | "enabled" / "disabled" / "global"; null = not stated |
| BackupManga | 620 `scanlatorSettingsPresent` | states that 614–616 are authoritative (empty lists may clear) |
| BackupCategory | 3 `id`, 901 `parentId`, 904 `parentUid`, 902 `trackStatus`, 907 `hierarchyKnown` | Mihon's `id` field plus Komikku's hierarchy fields |

No existing proto number was reused, no chapterFlags bit was added, and the known `0x100` collision (fork
fillermark filter vs. Mihon sort) was not extended. Restores: rules and colors are only overwritten when the backup
states them; categories are restored parent first and matched by name among the children of their restored parent
(a backup without hierarchy prefers top-level categories); invalid parents (cycles) are ignored.

## Rollback

- Code: the backup branches named in KOMIKKU_FEATURE_PORTS.md.
- Data: the new columns are ignored by the previous server version (Exposed only reads declared columns), so a
  database used by this version still opens with the previous one; Smart Category memberships and blacklist rules
  simply stop being applied. Taking a backup before upgrading (as described in MIGRATION_AND_ROLLBACK.md) remains
  the safe path.
- Backups written by this version restore on the previous version: unknown proto fields are skipped.

## Verified on disposable databases

All runtime checks used H2 databases in temporary/scratch directories (the test suite's database and a scratch
server root). The real library was not touched. PostgreSQL was not executed.

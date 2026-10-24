# Komikku port — schema, backup and migration notes

## Database (ForkSchema, idempotent, runs after upstream migrations; no numbered migrations)

| Table | Column | Type / default | Meaning |
|---|---|---|---|
| CHAPTERUSER | BOOKMARK_COLOR | INT, 0 | bookmark color (0 = default) |
| CHAPTERUSER | BLACKLISTED | BOOLEAN, false | derived: hidden by the blacklist |
| CHAPTERUSER | MERGE_HIDDEN | BOOLEAN, false | derived: duplicate hidden by Smart Scanlator Merge |
| CHAPTERUSER | ~~FILLERMARK~~ | — | removed after the fillermark migration (below) |
| MANGAUSER | BLACKLISTED_CHAPTERS | text NULL | JSON list of blacklist keys |
| MANGAUSER | SCANLATOR_PRIORITY | text NULL | JSON list |
| MANGAUSER | SCANLATOR_RANGE_RULES | text NULL | JSON list of `from:to:scanlator` |
| MANGAUSER | SMART_MERGE_OVERRIDE | BOOLEAN NULL | NULL = follow the global setting |
| CATEGORY | PARENT_ID | INT NULL | NULL = top level (index `FORK_CATEGORY_PARENT_ID`) |
| CATEGORY | TRACK_STATUS | INT NULL | Smart Category binding (unified status 1–7) |
| CATEGORY | HIDDEN | BOOLEAN, false | hidden category (left out of library tabs) |
| CATEGORY | UPDATE_INTERVAL | INT NULL | automatic update interval in days (NULL = inherit / global) |
| CATEGORY | LAST_SCHEDULED_UPDATE_AT | BIGINT, 0 | last scheduled update covering the category (epoch s) |

All new columns are additive with defaults that reproduce the previous behaviour. Text columns use the upstream
`UNLIMITED_TEXT` helper (H2 `VARCHAR`, PostgreSQL `TEXT`). The derived flags are recomputed from the rules and never
carry information of their own. Categories are per user, so hidden state, intervals and last runs are per user.

Per-user server setting: `smartScanlatorMerge` (user setting proto **1000**; fork user settings start at 1000 so
upstream additions never collide). Client settings live in the user's global/manga/category meta (`webUI_*` keys):
chapter filters incl. bookmark colors and blacklist/merge reveal, library and Updates color filters, per-manga
preferred tracker, global preferred tracker, auto-refresh, default/last bookmark color, show hidden categories and
the subcategory refresh scope.

## Fillermarks → chapter blacklist

**Why:** fillermarks and the ported chapter blacklist were two overlapping exclusion systems. The blacklist is
now the only one; the fillermark feature is removed.

**Migration** (ForkSchema, on every start until done):

1. Legacy `CHAPTER.FILLERMARK` (old fork databases) is moved to `CHAPTERUSER.FILLERMARK` for the user that owned
   the pre-multi-user data, as before.
2. Transaction A: every `CHAPTERUSER.FILLERMARK = TRUE` row (user, manga, chapter) gets a blacklist entry in that
   user's `MANGAUSER.BLACKLISTED_CHAPTERS` (the row is created if missing). The entry is a hybrid key — URL first,
   then number, scanlator and normalized name — so it follows the chapter through URL changes and never matches
   another chapter with the same number or name (ambiguous fallbacks resolve to nothing). Chapters already
   blacklisted by an existing entry get no second entry; existing entries are kept. `BLACKLISTED` is set on the rows.
   Committed.
3. Transaction B: every remaining fillermark is resolved against its user's blacklist. Only if all resolve, the
   obsolete meta keys (`webUI_fillermarked`, `webUI_hasFillermarkedChapters`, `webUI_deleteChaptersWithFillermark`)
   are deleted and `CHAPTERUSER.FILLERMARK` is dropped. Otherwise nothing is removed and the migration retries on the
   next start (logged as a warning).

An interrupted start resumes: step 2 is idempotent (no duplicates), step 3 only runs on verified data. Fillermarks
can only exist on chapters that exist (their rows are deleted with the chapter), so every fillermark has a URL and is
resolvable; an unresolvable one would keep the column instead of being dropped silently.

**Removed APIs:** `ChapterType.isFillermarked`, `ChapterUserType.isFillermarked`, `MangaType.fillermarkCount`,
`MangaUserType.fillermarkCount`, chapter condition/filter `isFillermarked`, `UpdateChapterPatch.isFillermarked`,
`copyFillermarksToBlacklist`, the REST `fillermark` form parameter, `ChapterDataClass.fillermarked`. The WebUI's
fillermark actions, reader button, filters, deletion setting and the manual "Blacklist fillermarked chapters" button
are removed.

**Reversibility:** not reversible in place — the column is dropped. The blacklist entries remain and are exported in
backups (proto 615), so the exclusions are never lost.

## Backup (protobuf)

| Message | Field | Notes |
|---|---|---|
| BackupChapter | 14 `bookmarkColor: Int?` | null = unknown → keep the local color |
| BackupChapter | 15 `fillermark` | **read only**: never written anymore; a legacy `true` becomes a blacklist entry of the restoring user (hybrid key from the backup chapter's URL, number, scanlator and name), added to the stated or local blacklist, never replacing it |
| BackupManga | 614 `scanlatorPriority`, 615 `blacklistedChapters`, 616 `scanlatorRangeRules` | |
| BackupManga | 618 `smartScanlatorMergeOverride` | "enabled" / "disabled" / "global"; null = not stated |
| BackupManga | 620 `scanlatorSettingsPresent` | states that 614–616 are authoritative |
| BackupManga | 9000 `meta` | per-manga client settings incl. the preferred tracker and chapter filters |
| BackupCategory | 3 `id`, 901 `parentId`, 904 `parentUid`, 902 `trackStatus`, 907 `hierarchyKnown` | Mihon's `id` plus Komikku's hierarchy fields |
| BackupCategory | 900 `hidden: Boolean?` | Komikku's flag; null = not stated (keep local) |
| BackupCategory | 903 `updateInterval: Int?` | days; restored when the backup knows the hierarchy |

Field numbers and types were compared with Komikku `main` (BackupChapter 14 `Int?`, 15 `Boolean`; BackupManga
614–616 `List<String>`, 618 `String?`, 620 `Boolean`; BackupCategory 3/901/904 `Long`, 902/903 `Int?`, 900/907
`Boolean`) and with the fork's existing Mihon/SY/J2K fields (BackupManga 108, 110, 602, 603, 800–805; BackupCategory
601–603, 9000): no collisions. All new fields are optional varints or length-delimited lists, so older readers skip
them. `BackupCategory.order` is `Int` here and `Long` in Komikku — the same varint wire type.

Not backed up: `LAST_SCHEDULED_UPDATE_AT` (device/run state, as in Komikku) and the derived visibility flags.

## chapterFlags `0x100` collision (resolved)

The fork used chapterFlags bits `0x80` (fillermarked only) and `0x100` (not fillermarked); Mihon uses `0x100` for
"sort by chapter number". With fillermarks removed:

- export never sets `0x80`/`0x100` from a fillermark filter; the stored flags (e.g. Mihon's sort bits from a
  restore) pass through unchanged;
- restore never interprets `0x80`/`0x100` as a fillermark filter; `0x100` keeps Mihon's meaning;
- no new chapterFlags bit is used; blacklist data travels in proto 614–620.

Unavoidable ambiguity: a backup exported by an older fork version with the "exclude fillermarked" filter carries
`0x100`; it is now read as Mihon's sort-by-number (the filter no longer exists, so nothing else is affected).

## Rollback

- Code: `backup/main-pre-komikku-ports-20261024` / `backup/master-pre-komikku-ports-20261024` (before the ports),
  `backup/komikku-ports-pre-continuation-20261024` (first port, with fillermarks).
- Data: restore the database copy taken before the upgrade. Starting an older server on a migrated database is not a
  safe rollback: the first-port server would recreate an empty fillermark column (the exclusions remain as blacklist
  entries), and a pre-port server ignores all port data (blacklists, colors, hierarchy) and would not hide anything.
- Backups written by this version restore on older versions: unknown fields are skipped (fillermarks are then not
  recreated — they are blacklist entries in proto 615).

## Verified on disposable databases

All runtime checks used H2 databases in the test directory and in a scratch server root. An end-to-end upgrade was
run: a previous-build server created fillermarks (including one on a merge-hidden chapter and one in another manga)
next to an existing blacklist entry; the new server, started on a copy, migrated them, kept the existing entry,
dropped the column and reported correct unread counts. The real library was not touched. PostgreSQL was not
executed (no disposable instance available); the SQL uses only constructs already used by upstream migrations
(`ADD COLUMN IF NOT EXISTS`, `DROP COLUMN IF EXISTS`, `CREATE INDEX IF NOT EXISTS`, `UNLIMITED_TEXT`, `BIGINT`).

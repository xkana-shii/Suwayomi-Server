# Komikku port — audit

Source of truth: the Komikku fork (`xkana-shii/komikku`, local clone, `main` at `0e831cfbc7`, read only — nothing was
changed there). Main was rebuilt on 2026-10-20 and contains the most complete versions of the features; older
branches (`fix/smart-scanlator-merge-visibility`, `smart-categories`, `fillermark+`) predate it and were only used
for history. Komikku's own code is marked with `KNS` comments.

Targets: Suwayomi-Server `main` @ `2971354b` and Suwayomi-WebUI `master` @ `df11a44d` (the integrated
upstream-20261023 state). Launcher: not needed (no launcher-visible change).

## Feature map

| # | Komikku implementation | Storage in Komikku | Suwayomi architecture it maps to |
|---|---|---|---|
| 1 | `ChapterSmartMerge.kt` (`applySmartScanlatorMerge`, range rules, notice detection), `ScanlatorPreferencesDraft/Changes`, `SequentialWriter`, `ChapterVisibility` (next chapter, downloads, Updates, library counts) | `manga.user_scanlator_priority`, `user_scanlator_range_rules`, `user_smart_merge_override`; pref `pref_smart_scanlator_merge` | Per-user rules in `MANGAUSER`; global switch as a per-user server setting; derived per-user flags in `CHAPTERUSER` so SQL counts/queries see them; WebUI scanlator filter dialog |
| 2 | `ChapterBlacklist.kt` (URL, hybrid v1/v2 and legacy keys, unique-match resolution), `chapterListVisibility` (reveal), legacy fillermark → URL keys | `manga.user_blacklisted_chapters` | Same keys in `MANGAUSER.BLACKLISTED_CHAPTERS`; derived `CHAPTERUSER.BLACKLISTED`; reveal per manga in WebUI meta |
| 3 | `BookmarkColor` (ids 0–6, palette), `ChapterBookmarks.toggle/setColor`, color carry-over on re-upload, `BookmarkColorDropdownMenu`, `BookmarkColorFilterDialog` | `chapter.user_bookmark_color` | `CHAPTERUSER.BOOKMARK_COLOR` (per user); `updateChapters` patch; WebUI color menu / filter |
| 4 | `SyncCategoriesWithTrackStatus.updatedMembership`, `SyncCategoryTrackStatus`, `TrackStatus` mapping | `category.track_status` | `CATEGORY.TRACK_STATUS`; hook on every track record write in `Track.kt` |
| 5 | `CategoryTree` (forest, cycle-safe walks, sibling-unique names), `LibraryCategoryTree` (top-level tabs, subtree items, chips), set-categories dialog | `category.parent_id` (0 = root) | `CATEGORY.PARENT_ID` (NULL = root, because Suwayomi's default category is a real row), GraphQL hierarchy, WebUI settings tree, library tabs |
| 6 | `MetadataAutofill`, tracker `getMangaMetadata`, `UpdateTracks`/`UnifiedTrackerEdits`, preferred tracker | prefs | Fork's existing `MetadataProvider`s (MangaBaka, MangaUpdates, MyAnimeList, AniList) — their ids are the trackers' remote ids, verified per provider; unified edits on top of `Track.update` |
| 7 | `OpenRefresh` (refresh details/chapters on open, read once per entry) | prefs | WebUI-side: shows cached data, refreshes in the background, deduplicated across remounts |
| 8 | Chapter filters incl. bookmark colors, blacklist reveal | prefs | WebUI chapter list options (manga meta with global defaults, like the existing filters) |

## Fillermark audit (revised 2026-10-24)

The fork's fillermark (`CHAPTERUSER.FILLERMARK`, filter bits `0x80/0x100`, proto 15) has the same bits and proto
number as Komikku's old-fork fillermark, which Komikku's `OldForkDatabaseConverter` and backup restore turn into
exact blacklist keys (requirement 4.2 of Komikku's tracker: "No standalone Fillermark UI — superseded, migration
only"). The first port kept both systems apart; on the user's decision the fillermark is now **replaced by the
blacklist**, like in Komikku:

- every fillermark migrates into a blacklist entry of its user (hybrid key instead of Komikku's `url:` key, so the
  entry also survives a source URL change without false matches);
- legacy backups restore proto 15 as blacklist entries; new backups never write it;
- the fillermark UI, API, settings and the manual conversion button are removed;
- the `0x80/0x100` chapterFlags collision with Mihon's sort bits is resolved by dropping the fillermark meaning.

Details: KOMIKKU_PORT_MIGRATIONS.md.

## Continuation audit (2026-10-24)

Komikku `main` advanced to `0f1c9998dd` (fix: persist scanlator preference drags immediately). Its intent — every
priority change is written at once — already holds in the port (each move is its own queued mutation). The
remaining Komikku features were read from `docs/kns-feature-implementation.md` and the code:

| Komikku item | Komikku behaviour | Port |
|---|---|---|
| 2.2-e / batch 11 library color filter | Edit beside the Bookmarked tri-state; colors refine Bookmarked (include) only; a manga passes with an included color and no excluded color | Same rule; colors of visible bookmarked chapters (`MangaType.bookmarkColors`). Komikku counts hidden chapters' colors and lists that as a known gap; the port uses visible chapters |
| batch 11/12 Updates color filter | Own Updates keys, same tri-state rule per chapter | Same, filtered by the server (`isBookmarked` + `bookmarkColor in/notIn`) |
| 4.1-f chapter color filter | Refines Bookmarked | Changed from an independent filter to the same refining rule |
| batch 5 preferred tracker | Per-manga map plus global priority tracker (device-only in Komikku) | Per-manga preference in the user's manga meta (backed up), global preference in global meta |
| 1.2-k hidden categories | Hidden (or below a hidden ancestor) categories contribute nothing to tabs; "show hidden" setting; membership kept | Same, plus visible subtree counts |
| 1.5-a/b/d update scope | Interval inherited from the nearest ancestor; category refresh covers the subtree; whole category / selected subcategory scope; manga due through any category, a global one always wins; dedup by manga id | Same on Suwayomi's updater; update inclusion inherits the same way; last runs per category in the database (per user) |

## Komikku bugs and how the port avoids them

| Komikku problem | Port |
|---|---|
| Scanlator order reset after refresh / new scanlators disturbing priorities | Saved priority is never rewritten on refresh; the shown order is "saved order narrowed to present scanlators, then new ones alphabetically"; edits merge back keeping unseen scanlators' positions (`ChapterVisibility.effectivePriority/mergePriority`, tested) |
| Global setting not persisting | Server-side per-user setting (`smartScanlatorMerge`, backed up with user settings) |
| Settings lost when leaving the screen | Every change is written immediately through a sequential queue; no Save button; failures are reported and the editor returns to the saved state |
| Ambiguous blacklist matches hiding unrelated chapters | Keys only resolve when the match is unique (URL first, then name, then number + scanlator) |
| History continuing from the first chapter when the last read is hidden | Server queries for first unread chapter exclude hidden chapters; the reader keeps the open (revealed) chapter navigable and skips hidden neighbours |

## Shared infrastructure decision

Visibility is materialized per user (`CHAPTERUSER.BLACKLISTED`, `MERGE_HIDDEN`) and recomputed on chapter refresh,
rule changes, the global toggle and restore. This keeps every consumer consistent with one rule: unread and bookmark
counts, first unread chapter, OPDS unread counts, auto-download of new chapters, the Updates feed, and the WebUI's
single `filterChapters` path (chapter list, reader, download helpers). Hidden chapters are never deleted.

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

## Fillermark audit

The Suwayomi fork's fillermark (`CHAPTERUSER.FILLERMARK`, filter bits `0x80/0x100`, proto 15) has the same bits and
proto number as Komikku's old-fork fillermark, which Komikku converts into exact blacklist keys. In Suwayomi,
however, fillermarks are **a marker with their own tri-state filter and download/deletion settings**, not a hide
rule. They are therefore **not equivalent** and were kept as they are:

- no data is migrated, replaced or removed; proto 15 still restores as fillermarks (Komikku would convert it);
- the blacklist is a separate feature; an explicit per-manga action "Blacklist fillermarked chapters" copies them
  into the blacklist on request without touching the fillermarks.

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

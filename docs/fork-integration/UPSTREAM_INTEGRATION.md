# Upstream integration (October 2026)

All work is local, on `integration/upstream-20261023` in each repository. Nothing was pushed.
The fork was ~4–5 months (not a year) behind upstream.

## Summary

| | Server | WebUI | Launcher |
|---|---|---|---|
| Original fork head | `main` `8abb3147` | `master` `7500ae6f` | `master` `5329176` |
| Merge base | `a403b1c5` (2026-05-31) | `4a3f0210` (2026-06-02) | `3bbed82` (2026-05-19) |
| Upstream target | `master` `6b481e8b62938b9c060eb210b934e0fbfdfa466b` (2026-10-21) | `master` `c3465cb4ea15a8778fa6ded50e68bddd18052145` (2026-10-21) | `master` `12d5a0548d80e97a2f5d1b531e6b2ab5dc9124b9` (2026-10-08) |
| Upstream commits integrated | 248 | 307 | 30 |
| Fork commits | 98 (incl. merges) | 83 | 12 |
| Strategy | one `--no-ff` merge, hunk-by-hunk resolution (diff3), then focused fix commits | same | same |
| Conflicted files | 28 | 22 | 3 |

A single merge per repository was chosen over cherry-picking: it keeps upstream ancestry (future syncs are
normal merges again) and every conflict was resolved against the base, never by taking whole files from one side.

## Server

### Major upstream changes
- **User accounts** (#623): read/bookmark/progress/download state moved to `CHAPTERUSER`, library state to
  `MANGAUSER`, metas/categories/tracking per user; GraphQL `user { … }` sub-objects; permissions
  (`@RequirePermissions`); all mutations get `@GraphQLIgnore userId`.
- SyncYomi (protocol v2), extension API 1.6 and **extension stores** (replacing `extensionRepos`),
  Exposed 1.4, graphql-kotlin 11, Javalin 7.2, JCEF/JBR 25, many query-performance changes (`inIds`, indexes),
  download rename-on-update, CBZ preservation, OPDS fixes, per-user settings.

### Conflict resolutions and adaptations
See the merge commit message and `FORK_FEATURE_INVENTORY.md`. The non-obvious ones:
- **Fillermark storage**: moved to `CHAPTERUSER`; fork migration expressed as `ForkSchema` (post-migration,
  idempotent). The migration runner (`exposed-migrations`) requires consecutive numbers and only runs versions
  above the stored maximum, so a numbered fork migration would silently skip the next upstream migration.
- **`Backup` field order**: fork fields moved to the end; upstream constructs `Backup(...)` positionally (sync code
  and export), which would otherwise have written `meta` into `backupPreferences`.
- **Download naming**: upstream's new `updateChapterDownloadDir` now moves legacy and hashed dirs to the hashed name.
- **Shared metadata overrides** under per-user meta (see S6).
- **graphql-kotlin 11**: Kotlin defaults no longer make input fields optional → fork cover flags made nullable.

### Fork bugs found and fixed during integration (separate commits)
- `2eeef8fa` details.json edits lost on Windows (`File.renameTo` does not overwrite).
- `0dcc15f1` MangaBaka score list threw while not logged in (reproduced on the old fork: `fetchChapters` failed).
- `5c33d6b2` cover flags optional again.
- In the merge: download dir followed the remote title while an override title was stored; ArchiveProvider only
  deleted one candidate name; `delete()` reported failure when nothing existed.

### Intentionally not carried over
- The fork's inline copy of the *old* CBZ download filename (upstream's improved name is used).
- The fork's `error()` message format change in `bundler.sh` (cosmetic, lost colons).
- The fork's edits to `M0001_Initial`/`M0006_AnimeTablesBatch3` (replaced by `ForkSchema`).
- Automated backups are no longer created through the async path: upstream now writes one automated backup per
  user and needs the success result for scheduling. Manual async backups (`createBackupAsync`) are unchanged.

### Commits
`193e4114` merge · `2eeef8fa` · `0dcc15f1` · `ed01d3d6` tests · `5c33d6b2` · `9c62fd07` WebUI pin · (+ docs commit)

## WebUI

### Major upstream changes
User accounts / login, app initializer, two-pane settings, appbar search suggestions/history, fuzzy library
search, library filter OR-logic + reset, grouped updates & history, download queue grouping, manga notes,
markdown descriptions (tiptap), chapter update notifications, debug information, MUI 9.3, Apollo 4.2, graphql 17.

### Resolutions
- Overlapping features (updates grouping, history, tab count, markdown, category dialog) — see inventory W3–W6, W14.
- `About.tsx` was mis-merged by git (the fork's dialog landed inside the links list); rebuilt by hand.
- GraphQL types regenerated against the merged fork server; new server flag `includeUserSettings` added to the UI.
- English catalog re-extracted from source (`pnpm i18n:extract`).
- Pre-commit hook: `oxlint --no-error-on-unmatched-pattern` so commits of only generated files pass.

### Commits
`1b4a5886` merge · `948ec32b` types/hook · `df11a44d` mapping file

## Launcher

### Upstream
User-accounts settings, sync/OPDS/database settings, config migration, JDK 25 build, dependency updates.

### Resolutions / fixes
- Window height: upstream 660px (fork wanted taller than 600 too).
- `d8b2672` missing-jar download race, atomic download, update target path.
- `b7c7fa7` launcher and server used different data dirs by default.

### Commits
`03f741d` merge · `d8b2672` · `b7c7fa7`

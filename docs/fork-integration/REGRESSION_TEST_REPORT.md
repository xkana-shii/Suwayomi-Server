# Regression test report

Environment: Windows 11, Oracle JDK 21.0.8, Node 24.14.0, pnpm 11.1.2. All runtime tests used disposable data
directories in a temp/scratch folder; no real user data was used.

## Commands and results

| Repo | Command | Result |
|---|---|---|
| Server | `./gradlew :server:test ktlintCheck` | **381 tests, 0 failures**, ktlint clean |
| Server | `./gradlew :server:test --tests "*ForkFeaturesTest"` | 5/5 (new fork regression tests) |
| Server | `./gradlew :server:shadowJar` | `Suwayomi-Server-v2.4.2539.jar` |
| WebUI | `pnpm tsc` (tsgo) | pass |
| WebUI | `pnpm lint` (oxlint) | pass |
| WebUI | `pnpm format` (oxfmt) | applied, pre-commit hooks pass |
| WebUI | `pnpm gql:codegen` against the merged server | pass (all fork operations validate against the schema) |
| WebUI | `vite build` | pass |
| Launcher | `./gradlew formatKotlin lintKotlin shadowJar` | pass, `Suwayomi-Launcher-1.0.14.jar` |
| Old fork | `./gradlew :server:shadowJar` on `backup/main-pre-upstream-20261023` (worktree) | `v2.2.2285`, used to create old data |

The WebUI has no unit/component test suite (`pnpm test` is a placeholder upstream).

## Failures found and fixed during testing
- Upstream `ChapterTest.chapterNameChangeInvalidatingDownloadClearsPerUserState`: blocked the unhashed rename
  target; adapted to the fork's hashed names (intent unchanged).
- `LocalMangaDetailsServiceTest` (2 fork tests): real Windows bug (details.json not replaced) → fixed.
- `ForkFeaturesTest` backup restore: MangaBaka "Unknown score type" → fixed (also reproduced on the old fork).
- E2E: `resetMangaMetadataToSource` required `resetCover` after graphql-kotlin 11 → made optional again.

## Runtime checks (end-to-end)

| Scenario | How | Result |
|---|---|---|
| Fresh install | New jar, empty dir | Migrations 1–67 + ForkSchema, API answers |
| Upgrade of old fork DB | Old fork server populated a library via its API (local source: folders + CBZ with ComicInfo; read, bookmark, fillermark incl. fillermark-only chapter, category, meta, metadata override), then new server on a copy | **57/57** checks (ids, titles, overrides, categories, meta, all chapter states, user.* fields, counts) |
| Upgrade through the launcher | New launcher (`--launch`, isolated in-memory Java preferences pointing to a copy) | Server started with the right root dir, **57/57** checks, `server.conf` migrated |
| Settings preservation | Old `server.conf` with non-default values | per-user settings and `maxPagesInParallel = 15` preserved |
| Refresh | `fetchManga` / `fetchChapters` | overrides and fillermarks survive |
| Reader | page list + image request, progress | 2 pages, PNG served, `lastPageRead` saved; WebUI reader resumed on saved page |
| Fork APIs | fillermark toggle/filter, metadata edit/reset, nonLibraryCount, clearDatabase, createBackupAsync/createStatus, validateBackup, installMissingExtensionsFromBackup, restore of old fork backup, shutdownServer | all pass |
| WebUI (browser) | custom WebUI `r3637`/`r3639` served by the upgraded server | library, categories, manga page (fork icons, stored filter, markdown, edit button, notes), reader + fillermark button, updates (fork grouping, compact rows, unread dots), history, library settings, about + shutdown; mobile width (375px) manga page |
| WebUI update protection | flavor WEBUI, channel PREVIEW, network on, before the fork release existed | only fork URLs contacted; installed custom WebUI unchanged |
| WebUI auto-install | same settings, empty install, after the fork CI published `r3639` (pushed 2026-10-23 20:30) | `r3639` resolved via the fork mapping file, downloaded from `xkana-shii/Suwayomi-WebUI-preview`, MD5 verified, served |
| Direct server launch | `java -jar` | works |

## Feature coverage

Per-feature statuses are in `FORK_FEATURE_INVENTORY.md`. Counts:
- Server 28 items: 14 verified working end-to-end (5 of them also by automated tests), 4 verified through automated
  tests only, 8 need manual testing, 2 not fork features (upstream behaviour adopted).
- WebUI 18 items: 9 verified working, 1 automated (type-check), 7 manual, 1 stale files (no action).
- Launcher 5 items: 3 verified working, 2 manual.

## Not tested / requires manual verification
- **Downloads at runtime** (hashed names, parallel pages, CBZ): local-source downloads are skipped by upstream and
  no online extension was installed in the disposable environment. Naming/legacy lookup is unit tested.
- **Tracking logins** (MangaBaka OAuth, others), **SyncYomi**, metadata provider search (network/accounts).
- **PostgreSQL** (fork schema SQL is written for both databases, only H2 was executed).
- **Electron**, **JCEF WebView** (CEF was disabled), **system tray** (disabled in tests).
- **Launcher GUI interactions** (Server Update tab, settings edits) and the missing-jar auto download.
- **GitHub Actions workflows** (not run; reviewed only) and `bundler.sh` packaging (syntax check only).
- Upgrade of *your* real database (intentionally not touched — follow `MIGRATION_AND_ROLLBACK.md`).

## Known issues
- The fork's Mihon mapping uses bit `0x100` for "fillermarked = false", which Mihon uses for "sort by chapter
  number" (pre-existing). The integration only applies it when the WebUI filter is set, but a Mihon backup with that
  sort order will show up as a fillermark filter in the WebUI.
- `zh_Hans.po`, `zh_Hant.po`, `src/i18n/locales.json` in the WebUI are stale, unused files from the fork.
- STABLE WebUI channel points at upstream revision tags that do not exist in the fork release repo (pre-existing).

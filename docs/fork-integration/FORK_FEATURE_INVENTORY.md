# Fork feature inventory

Inventory of everything that distinguishes the xkana-shii forks from upstream Suwayomi, how each item was
carried through the October 2026 upstream integration, and how it was verified.

Pre-integration refs (preserved as `backup/*-pre-upstream-20261023` branches):

| Repository | Original branch / commit | Merge base with upstream | Integration branch |
|---|---|---|---|
| Suwayomi-Server | `main` @ `8abb3147` | `a403b1c5` (2026-05-31) | `integration/upstream-20261023` |
| Suwayomi-WebUI | `master` @ `7500ae6f` | `4a3f0210` (2026-06-02) | `integration/upstream-20261023` |
| Suwayomi-Launcher | `master` @ `5329176` | `3bbed82` (2026-05-19) | `integration/upstream-20261023` |

Classification: **F** = fork-specific, **O** = overlaps with something upstream added since, **U** = upstream behaviour the fork had only
re-formatted or copied (not a feature).

Validation status legend: **VW** verified working (end-to-end run), **VT** verified through automated tests,
**MT** implemented, requires manual testing, **BE** blocked by environment, **BR** broken / incomplete.

## Server

| # | Feature | Cls | Original implementation | Original behaviour | Upstream equivalent | Integration approach | Validation | Status |
|---|---|---|---|---|---|---|---|---|
| S1 | Fillermarks | F | `ChapterTable.isFillermarked` (+ M0001 edit), ChapterType/Query/Mutation, `Chapter.kt`, `MangaController`, `FillermarkedChapterCountForMangaDataLoader`, backup `BackupChapter.fillermark` (proto 15) | Per-chapter "filler" flag like a bookmark; queryable, filterable, counted, preserved over chapter re-insertion, in backups | None. Upstream moved read/bookmark to per-user `CHAPTERUSER` | Adapted: column now `CHAPTERUSER.FILLERMARK`; `isFillermarked` kept top-level **and** `user.isFillermarked`; count merged into `ChapterFlagCountForMangaDataLoader` (`fillermarkCount`, `user.fillermarkCount`); REST `fillermark` form param kept | `ForkFeaturesTest` (set, URL-change preservation, backup), upgrade E2E (57 checks), WebUI reader toggle | VW + VT |
| S2 | Fork schema migration | F | Fork edited `M0001_Initial` / `M0006` to add the column | Only new databases got the column | — | New `ForkSchema` step (idempotent, after upstream migrations, outside the numbered chain so it never blocks future upstream migrations); moves legacy `CHAPTER.FILLERMARK` to user 1, creating missing `CHAPTERUSER` rows; M0001/M0006 restored to upstream | `ForkSchema` unit test, upgrade of a real old-fork DB | VW + VT |
| S3 | Hashed chapter download names | F | `DirName.kt` (`truncateToBytes`, md5 of chapter url), candidate resolution, `ChapterDownloadHelper.migrateDownloadedChapterFilename` | `<scanlator>_<name>_<md5(url)[0:6]>`, byte-truncated; legacy unhashed downloads still found and renamed on manga fetch | Upstream added rename-on-chapter-update with unhashed names | Adapted to upstream's suspend DirName; upstream rename now moves legacy/hashed dirs to the new hashed name; providers resolve candidates; delete removes every candidate (folder **and** cbz); migration on fetch limited to downloaded chapters | `ForkFeaturesTest`, adapted upstream `ChapterTest` | VT (runtime download needs an online source: MT) |
| S4 | Parallel page downloads (`maxPagesInParallel`, proto 500, 10–20) | F | `ChaptersFilesProvider` semaphore + aggregated progress, `ServerConfig` | Pages of a chapter downloaded concurrently | None | Kept (auto-merged into suspend provider); setting kept with same proto number | Compile + config migration test (value 15 preserved) | MT |
| S5 | `serverUpdateChannel` setting (proto 501) | F | `ServerConfig` | Used by the launcher's server updater | None | Kept | Config migration test | VW |
| S6 | Manga metadata editing (title/author/artist/description/genre/status) incl. local `details.json` | F | `MangaMutation.updateMangaDetails`, `LocalMangaDetailsService`, override meta keys in `Manga.kt` | Overrides survive source refreshes; genre/status frozen in custom mode; local source writes details.json | None | Adapted to per-user mutations (`userId` injected); override meta keys treated as **shared** (read regardless of user, deleted for all users) because they change the shared manga row; download dir now follows the stored override title (fixes a fork bug where the dir followed the remote title) | E2E: edit, refresh, reset; `LocalMangaDetailsServiceTest` | VW + VT |
| S7 | Cover upload / external cover | F | `uploadMangaCover`, `Manga.getMangaThumbnail` prefers downloaded thumbnail | Custom cover served instead of source cover | None | Adapted (per-user context) | Compile; WebUI dialog present | MT |
| S8 | Metadata providers (MangaBaka, MangaUpdates, MAL, AniList) + `searchMetadataProvider` / `applyMetadataMatch` | F | `manga/impl/metadata/*`, `MetadataMutation` | Search external DBs, apply details/cover | None | Adapted; `includeCover` made optional again (graphql-kotlin 11) | Compile; needs network | MT |
| S9 | `resetMangaMetadataToSource` | F | `MangaMutation` | Removes overrides, optional cover reset | None | Adapted; `resetCover` optional again | E2E | VW |
| S10 | MangaBaka tracker (OAuth/PKCE, score steps) | F | `track/tracker/mangabaka/*`, `TrackerManager` id 11, `PkceUtil.generateS256Codes`, `Requests.PATCH`, logo | Track on MangaBaka | None | Ported to per-user tracker API (per-user interceptor/API cache, user-scoped token & score type); **bug fix**: score list no longer throws when not logged in (broke tracker lists/backup restore for everyone not logged into MangaBaka, also on the old fork) | `ForkFeaturesTest` backup restore with tracking, old-fork reproduction | VT (login: MT) |
| S11 | Mihon/TachiyomiSY backup interop | F | `BackupMangaHandler`, `BackupManga` (108 excludedScanlators, 110 notes, 602/603 SY, 800–805 J2K), `Backup` 104/105, `BackupPreferenceHandler` | excludedScanlators and chapter filter flags ⇄ WebUI meta; source preferences in backups | Upstream added proto 106/109/111/112 (SyncYomi) and stores raw `chapterFlags` | Merged field sets; fork fields kept last in `Backup` so upstream's positional constructor stays correct; WebUI filter meta overrides only the matching bits of upstream's stored `chapterFlags` | `ForkFeaturesTest`, old-fork backup restored on new server | VW + VT |
| S12 | Bulk-prefetch backup export with progress | F | `BackupMangaHandler.backup(flags, progress)` | One transaction for all related tables, per-manga progress | Upstream rewrote export (per user, `since` for SyncYomi) | Re-implemented on upstream's user-scoped queries incl. `since`, using `inIds` | `ForkFeaturesTest`, upstream backup/sync tests | VT |
| S13 | Async tracked backup creation | F | `ProtoBackupExport.createBackupAsync`, `BackupMutation.createBackupAsync`, `BackupQuery.createStatus`, `BackupCreateStatus` | Background creation with progress state | None | Adapted per user, same permission rules as `createBackup`; automated backups use upstream's per-user files (see notes) | E2E (`SUCCESS`) | VW |
| S14 | Install missing extensions from a backup | F | `BackupMutation.installMissingExtensionsFromBackup` | Finds and installs extensions providing missing sources | None | Ported from `extensionRepos`/`ExtensionGithubApi` to `ExtensionStoreService`; requires `INSTALL_EXTENSIONS` | E2E (no-missing path) | VW (install path: MT) |
| S15 | `shutdownServer` mutation | F | `InfoMutation` | Stop the server from the UI | None | Kept; now requires `MANAGE_SETTINGS` (admin passes) | E2E via WebUI and API | VW |
| S16 | `clearDatabase` mutation | F | `InfoMutation` | Delete non-library manga (optionally by source, keeping read ones) | None | Library/read state is per user now: deletes manga in nobody's library, keeps those anyone read; requires `MANAGE_SETTINGS` | E2E | VW |
| S17 | `nonLibraryCount` on sources | F | `NonLibraryCountForSourceDataLoader` | Count of a source's non-library manga | None | Rewritten for `MangaUserTable` (per requesting user) | E2E | VW |
| S18 | Duplicate-chapter detection by normalized name | F | `HasDuplicateChaptersForMangaDataLoader` | Duplicates = equal lower-cased names (not chapter numbers) | Upstream still uses chapter numbers | Fork semantics kept, upstream `inIds` + narrower select | Compile/tests | MT |
| S19 | ComicInfo chapter metadata for local source | F | `LocalSource.applyComicInfoToChapterIfPresent`, `SChapter.copyFromComicInfo` | Title/number/translator/date from ComicInfo.xml in folders/cbz/cbr | Upstream reads ComicInfo for manga only | Kept | E2E: titles, scanlator, dates from generated cbz | VW |
| S20 | Fork release URLs | F | `AppUpdate`, `WebUIFlavor.WEBUI`, `downloadWebUI`, `bundler.sh`, `Suwayomi Launcher.bat` | Updates/WebUI/launcher come from xkana-shii repos | — | Kept; bundled WebUI pin set to fork `r3639`; see compatibility matrix | E2E: only fork repos contacted | VW |
| S21 | Windows build workflow | F | `build_push.yml`, `publish.yml` | Windows-only preview builds to fork preview repo with `BOT_PAT`, ref-based checkout | — | Kept, upstream action bumps applied | Diff review (CI not run) | MT |
| S22 | Bundler hardening (fork launcher URL, token, retries, release check) | F | `scripts/bundler.sh` | — | Upstream added its own error handling | Merged on top of upstream; executable bit restored (fork had dropped it, workflow chmod kept) | `bash -n` | MT |
| S23 | Windows AWT system tray | F | `server/util/systemtray/*` | Native AWT tray on Windows, Dorkbox elsewhere | Upstream changed Dorkbox tray | Upstream tray changes carried into `DorkboxSystemTrayHandler` | Compile | MT |
| S24 | Updater logging | F | `Updater.kt` | Per-manga success and summary log lines | — | Kept on upstream's `updateMangaAndChapters` | Compile | MT |
| S25 | `DocumentationDsl` 11-parameter handler, `Track.htmlDecode().trim()` | F | — | Needed by the REST `fillermark` param / trimmed descriptions | — | Kept | Compile | VT |
| S26 | `.run/server.run.xml`, `.gitignore` additions, README badges | F | — | Dev convenience / branding | — | Kept | — | VW |
| S27 | CBZ download file name (`<title> - [scanlator] <name>.cbz`) | U | inline copy in `ChapterDownloadHelper` | Same as old upstream | Upstream improved naming (#2100) | **Upstream naming adopted** (the fork had only copied the old format) | — | VW |
| S28 | ktlint-only reformatting of ServerConfig etc. | U | — | — | — | Upstream formatting taken | — | — |

## WebUI

| # | Feature | Cls | Original implementation | Upstream equivalent | Integration approach | Validation | Status |
|---|---|---|---|---|---|---|---|
| W1 | Fillermarks UI (chapter action, card eye icon, reader buttons desktop+mobile, library/chapter filters, "allow deleting fillermarked chapters" download setting) | F | `Chapter.constants/types`, `ChapterActionMenuItems`, `ChapterCard`, `ReaderFillermarkButton`, `Library.types`, `CategoryMetadata`, `DownloadSettings`, `Reader.utils`, fragments/mutations | None | Kept; `ChapterAction` union merged with upstream's `download_cancel` | Browser E2E: icons, stored filter, reader toggle persisted | VW |
| W2 | Unread dots on chapter cards | F | `ChapterCard`, `ChapterCardMetadata.showUnreadDot` | None | Kept; on upstream's new scanlator/name layout the dot sits in front of the chapter-name line | Browser E2E | VW |
| W3 | Updates grouped per manga per day (lowest chapter first, arrow toggle, compact extra rows) | O | `Updates.tsx` | Upstream added grouping (first unread first, "N more" button, full cards) | Upstream data grouping/virtualization + fork ordering and look | Browser E2E | VW |
| W4 | History: each manga once | O | `History.tsx` | Upstream: latest per manga **per day**, expandable | Combined: once per manga (fork) in its latest day, with upstream's expandable list for that day | Browser E2E | VW |
| W5 | Active library tab shows filtered count | O | `Library.tsx` | Upstream shows `filtered/total` | Fork display kept with upstream's loading guard | Code review | MT |
| W6 | Markdown descriptions (react-markdown, GFM, line breaks, sanitizing, normalization) | O | `MarkdownRenderer.tsx`, `DescriptionGenre.tsx` | Upstream: tiptap viewer, GFM disabled | Fork renderer kept for descriptions; upstream notes (tiptap) shown above it | Browser E2E (line breaks) | VW |
| W7 | Edit metadata dialog (fields, cover upload, provider search/apply, reset) | F | `EditMangaMetadataDialog.tsx`, mutations | None | Kept; nested ternary refactored for the new lint rules | Browser: button present; API E2E | VW (provider search: MT) |
| W8 | Backup: "Install all" missing extensions + re-validation | F | `BackupValidationDialog`, `Backup.tsx`, `RequestManager` | None | Kept | API E2E | VW |
| W9 | About: shut down server | F | `About.tsx` | Upstream added debug info | Combined | Browser E2E (server stopped) | VW |
| W10 | Library settings: clear database dialog (per source, keep read), duplicates options | F | `LibrarySettings.tsx` | Upstream added notifications setting | Combined | Browser: entries present; API E2E | VW |
| W11 | Library duplicates (image-hash & tracker workers, description worker changes) | F | `LibraryDuplicates*` | — | Kept (auto-merged) | Type-check | MT |
| W12 | Source browse selection mode / add selection to library, checkbox filter hover | F | `SourceMangas.tsx`, `MangaCard.tsx`, `CheckBoxFilter.tsx` | — | Kept (auto-merged) | Type-check | MT |
| W13 | Tracker card long-press copy | F | `TrackerActiveCard.tsx` | — | Kept | Type-check | MT |
| W14 | Category selection dialog size (850px, 80% width) | O | `CategorySelect.tsx` | Upstream removed the 435px cap | Fork size kept | Code review | MT |
| W15 | Small styling (list avatar 80px, options panel radius) | F | — | — | Kept | — | MT |
| W16 | Workflows: fork preview repo, `BOT_PAT`, ref checkout, automatic release versions | F | `.github/workflows/*` | — | Kept | Diff review | MT |
| W17 | Chinese catalogs `zh_Hans.po` / `zh_Hant.po`, `src/i18n/locales.json` | F (stale) | Added by fork commit `bef93b67` | Live catalogs are `zh-Hans.po` / `zh-Hant.po` | Left untouched; they are unused January snapshots of the old file layout — candidates for removal | — | — |
| W18 | New: `includeUserSettings` backup flag in backup dialogs | — | — | Required by the merged server API | Added | Type-check | VT |

## Launcher

| # | Feature | Cls | Original implementation | Integration approach | Validation | Status |
|---|---|---|---|---|---|---|
| L1 | Server updater ("Server Update" tab, channel Stable/Preview, fork releases) | F | `ServerUpdater.kt`, `ui/ServerUpdate.kt`, `serverUpdateChannel` | Kept; **fixes**: atomic download via `.part`, update target is the jar the launcher starts (was CWD-relative) | Build/lint | MT |
| L2 | Download the server jar when missing | F | `LauncherViewModel.init` | **Fixed**: was asynchronous while the jar was required immediately (always failed); now synchronous from the default channel | Build/lint | MT |
| L3 | Default data dir "Suwayomi" | F | `LauncherViewModel` | **Fixed**: launcher and server used different dirs when no root dir was set; "Suwayomi" is used only if it exists, else the server default "Tachidesk", and the resolved dir is always passed to the server | Launcher E2E (root dir passed) | VW |
| L4 | Icon, taller window | O | `Main.kt`, resources | Icon kept; upstream's 660px height used (fork 650) | Build | VW |
| L5 | Version bump workflow + `Implementation-Version` | F | `bump_version.sh`, `build_release.yml`, `build.gradle.kts` | Kept | Build (`1.0.14` jar) | VW |

# Fork changelog — upstream integration (October 2026)

## Upstream features gained
- User accounts (logins, roles/permissions, per-user library, progress, categories, trackers and settings).
- SyncYomi synchronization (protocol v2, delta merge), sync button in the WebUI.
- Extension stores (replacing extension repos), extension API 1.6, jar extensions.
- WebUI: two-pane settings, search suggestions and history, fuzzy library search, OR-logic library filters with
  reset, grouped download queue, manga notes, chapter update notifications, debug information, platform info.
- OPDS: CBZ downloads, search/explore/genre feed fixes, skip-metadata-feed option.
- Configurable PostgreSQL schema and connection pool size; mutation to clear WebView cache/cookies.

## Upstream fixes incorporated
Extension install/update deadlocks and errors, download queue ordering, stale data loaders, library/category count
mismatches, unread counts, fallback upload dates, chapter-list update keeping downloads, SOCKS proxy for WebView,
WebSocket auth/CORS, many GraphQL performance improvements (indexes, `inIds`, pagination), backup/sync robustness.

## Existing fork features preserved
Fillermarks everywhere, unread dots, metadata editing and external metadata providers, cover upload, MangaBaka
tracker, Mihon/SY/J2K backup fields, source preferences in backups, async backups, install-missing-extensions,
shutdown server, clear database, non-library counts, ComicInfo chapter metadata, hashed download names, parallel page
downloads, Windows AWT tray, fork update/release channels, launcher server updater, fork workflows and branding.

## Fork features adapted to upstream
- Fillermarks are per user (stored with read/bookmark state); old values migrate to the existing (admin) user.
- Updates/History/library tab count/markdown descriptions combine upstream's new implementation with the fork's
  behaviour (see inventory W3–W6).
- Metadata overrides are shared between users; clear database / shutdown need the "manage settings" permission.
- Automated backups are written per user (upstream); manual async backups unchanged.
- CBZ download file names use upstream's improved format.

## Compatibility improvements
- Fork schema changes run outside the numbered migrations, so future upstream syncs stay merge-only.
- The server only accepts the integrated fork WebUI (≥ r3639); old fork servers won't download the new WebUI.
- Fork mutation flags `resetCover` / `includeCover` stay optional for older clients.

## Bug fixes
- details.json edits were lost on Windows.
- MangaBaka broke tracker lists/backup restore for users not logged into MangaBaka.
- Download folder followed the remote title while an override title was shown.
- Deleting a CBZ download could leave a legacy-named copy behind.
- Launcher: missing server jar never downloaded; failed downloads could corrupt the jar; Server Update tab wrote
  relative to the working directory; launcher and server used different data dirs by default.

## Database and migration changes
- Upstream migrations 56–67 (one-way). `ForkSchema`: `CHAPTERUSER.FILLERMARK`, legacy `CHAPTER.FILLERMARK` moved
  and dropped. See `MIGRATION_AND_ROLLBACK.md`.

## Remaining limitations
See "Not tested" and "Known issues" in `REGRESSION_TEST_REPORT.md`.

# Fork changelog — Komikku feature ports (October 2026)

Ported from the Komikku fork (details: KOMIKKU_FEATURE_PORTS.md, KOMIKKU_PORT_AUDIT.md):
- Smart Scanlator Merge with global/per-manga settings, scanlator priority, range rules and an auto-saving
  scanlator settings dialog.
- Chapter blacklist (URL-first keys, reveal mode, backup/restore), respected by counts, Updates, reader and downloads.
- Colored bookmarks (Komikku palette, long press / context menu, default and last color) and a tri-state bookmark
  color filter.
- Smart Categories bound to tracker statuses, scoped by category branch.
- Nested categories: hierarchy management, tree-aware Set categories, library tabs with subcategory chips.
- Tracker metadata autofill (MyAnimeList, AniList, MangaUpdates, MangaBaka) and one edit for all trackers.
- Optional background refresh of opened manga.
- New chapter filters: bookmark colors, blacklisted chapters, merged duplicates.

Backups use Komikku's proto numbers for the new data (see KOMIKKU_PORT_MIGRATIONS.md).

### Continuation (October 24, 2026)

- **Fillermarks removed**: every fillermark is migrated automatically into a blacklist entry of its user (verified
  before the old column is dropped); legacy backups restore fillermarks as blacklist entries; the fillermark UI,
  API and settings are gone. The chapterFlags `0x100` collision with Mihon's sort is resolved.
- Bookmark color filters in Library and Updates (Edit beside "Bookmarked"); the chapter color filter now refines
  "Bookmarked" like in Komikku.
- Per-manga preferred tracker for metadata autofill; global preference in Tracking settings.
- Hidden categories; per-category automatic update intervals; category refreshes cover subcategories; library
  refresh scope (whole category / selected subcategory).
- Tests run on another port with `-PtestServerPort`.

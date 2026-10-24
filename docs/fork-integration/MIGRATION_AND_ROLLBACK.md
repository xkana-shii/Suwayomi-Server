# Upgrading from the old fork, and rolling back

**The upgrade is one-way.** On first start the new server runs upstream migrations 56–67 (SyncYomi, extension
stores, **user accounts**, indexes) and the fork schema step. Migration 66 moves read/bookmark/progress/library state
into per-user tables and drops the old columns; the fork step moves fillermarks and drops `CHAPTER.FILLERMARK`.
The old fork server cannot open the upgraded database. The only rollback is restoring the backup taken before.

## 0. Find your data directory

The data directory (database, `server.conf`, downloads, local manga, extensions, settings) is:
- the launcher's "Root directory" setting if set — on this machine the launcher preference points to
  **`D:\Suwayomi\Suwayomi Data`**;
- otherwise the server default `%LOCALAPPDATA%\Tachidesk` (this folder also exists here, without a database).

Check the "Root directory" field in the launcher before doing anything else.

## 1. Back up (mandatory)

1. Stop Suwayomi completely (tray icon → Quit, or close the launcher-started process). The H2 database must not be
   in use while it is copied.
2. Copy the **whole data directory** somewhere else, e.g. `D:\Suwayomi-backup-2026-10\Suwayomi Data`.
   It contains `database.mv.db`, `server.conf`, `settings\*.xml` (tracker logins, source preferences, user settings
   backfill), `extensions`, `local`, `downloads`, `backups`, `webUI`, `userCovers`.
   If your downloads directory or local source directory is configured outside the data directory, they are not
   modified by the upgrade, but back them up if you want a full snapshot.
3. Also create a backup from the old WebUI (Settings → Backup → Create backup, all flags). It can be restored into
   the new server (tested), but it does not include everything the directory copy does.
4. Copy the old installation folder (launcher jar, `bin\Suwayomi-Server.jar`, `jre`) so the old version can be started
   again.
5. If you use PostgreSQL: `pg_dump -Fc -f suwayomi-before-upgrade.dump <database>` before starting the new server.

## 2. Install

1. Build or download the new artifacts (server jar `Suwayomi-Server-v2.4.2539.jar`+, WebUI `r3639`, launcher).
2. Replace `bin\Suwayomi-Server.jar` in the installation folder with the new server jar and `Suwayomi-Launcher.jar`
   with the new launcher. The new server needs Java 21+ (the bundled `jre` from a new Windows build is Java 25).
3. WebUI:
   - after the fork CI has published WebUI `r3639` to the fork preview repo, keep `webUIFlavor = WEBUI` and
     `webUIChannel = PREVIEW` (your current settings) and the server installs it automatically; or
   - copy a locally built WebUI (`pnpm build`, plus a `revision` file containing `r3639`) into
     `<data dir>\webUI` and set `server.webUIFlavor = "CUSTOM"` so it is never replaced.

## 3. Migrate

Start the server (launcher or `java -jar`). The log shows:
```
Database version before migrations: 55
Running migration version 56: SyncYomi … 67: AddChapterIndexes
ForkSchema: adding CHAPTERUSER.FILLERMARK
ForkSchema: moving legacy CHAPTER.FILLERMARK values to CHAPTERUSER
```
Existing data is assigned to user 1 (`admin`; name/password taken from `authUsername`/`authPassword` if set).
With `authMode = NONE` (your setting) nothing changes for you. The launcher also adds the new settings to
`server.conf`; settings that became per-user (auto-download, update exclusions, KOReader sync, OPDS) are moved, with
their values, to the user settings.

## 4. Check success

- Log: no `Error up-to-database migration` / `DbMigrationFailure`; the lines above present.
- Library: same entries, categories, unread counts; fillermarked chapters still show the eye icon; a manga's stored
  chapter filter still applies.
- Open a chapter you were reading: it resumes at the saved page.
- Settings → About shows server `v2.4.2539`+ and WebUI `r3639`.
- Optional API check: `{ mangas(condition:{inLibrary:true}) { totalCount } }` equals your old library size.

## 5. If it fails

- The server stops with `DbMigrationFailure` on a failed migration. Each migration runs in its own transaction and
  is only recorded when it completes, but schema statements may already be applied (H2 commits DDL immediately);
  upstream made M0066 re-runnable and the fork step is idempotent, yet a partially migrated database should be
  treated as damaged. Keep the log
  (`<data dir>\logs`), do not retry repeatedly, restore the backup (step 6) and report the log.
- If the server starts but data looks wrong, stop it immediately (further use writes to the migrated DB) and restore.

## 6. Restore the old version

1. Stop the new server.
2. Delete (or move aside) the current data directory and copy the backup from step 1.2 back to the same path.
3. Put the old `bin\Suwayomi-Server.jar` / launcher from step 1.4 back.
4. Start as before. PostgreSQL: `pg_restore --clean -d <database> suwayomi-before-upgrade.dump`.

Do **not** start the old server on the upgraded database: it is not downgrade compatible.

## Tested (disposable data only)

- Database created and populated by the old fork build (`backup/main-pre-upstream-20261023`, `v2.2.2285`), then
  opened by the new server directly and through the new launcher: 57/57 data checks passed in both runs.
- Old fork `server.conf` with non-default values: values preserved (including fork `maxPagesInParallel`).
- Old fork backup file restored into the upgraded server.
- Your real data in `D:\Suwayomi\Suwayomi Data` and `%LOCALAPPDATA%\Tachidesk` was not opened or modified.

## Komikku ports and fillermark removal (October 2026)

Starting the Komikku-port server migrates every fillermark into the chapter blacklist and then removes the
fillermark column and settings (KOMIKKU_PORT_MIGRATIONS.md). This is one-way: take a backup (database copy and a
Suwayomi backup) before the first start. A server and WebUI from before the port do not understand the new data;
roll back by restoring the database copy, not by starting an older server on the migrated database.

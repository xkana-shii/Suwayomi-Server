# Compatibility matrix

## Revisions that belong together

| Component | Branch / commit | Version | Upstream base |
|---|---|---|---|
| Suwayomi-Server | `integration/upstream-20261023` (`9c62fd07` + docs) | `v2.4.2539` → revision `r2539`+ | `6b481e8b` |
| Suwayomi-WebUI | `integration/upstream-20261023` `df11a44d` | revision `r3639` | `c3465cb4` |
| Suwayomi-Launcher | `integration/upstream-20261023` `b7c7fa7` | `1.0.14` (CI bumps on push) | `12d5a05` |

Server revisions are `git rev-list --count HEAD` of the server repo; WebUI revisions the same for the WebUI repo.
Fast-forwarding `main`/`master` to the integration branches keeps these numbers.

## How the server picks a WebUI (and why upstream's never replaces the fork's)

- `WebUIFlavor.WEBUI` (default) points to `xkana-shii/Suwayomi-WebUI-preview` (downloads, latest release) and
  `xkana-shii/Suwayomi-WebUI/master/versionToServerVersionMapping.json`. Upstream repositories are never contacted
  (verified: an E2E run only requested the fork URLs and kept the installed WebUI when they failed).
- A downloaded WebUI must be ≥ `webUIRevisionTag` (`buildSrc/.../Constants.kt`, now **`r3639`**), so the old fork
  WebUI builds (≤ `r3329`, which use the pre-integration API) are never installed on the new server.
- The mapping file's `PREVIEW` entry now requires server ≥ **`r2539`**, so old fork servers do not download the new
  WebUI. Old servers read this file from the fork's `master` branch, so it only takes effect after pushing.
- `WebUIFlavor.CUSTOM`: the server serves `<rootDir>/webUI` as-is and never checks for updates — use it for a
  locally built WebUI that must never be touched.
- `webUIChannel = BUNDLED` / stable releases (`publish.yml` → `downloadWebUI`) bundle exactly `r3639`, which must
  exist as a release in `xkana-shii/Suwayomi-WebUI-preview` before a stable server release. The upstream-only
  entries in the mapping file (e.g. `r3518`) do not exist in the fork's release repo — the STABLE WebUI channel was
  already unusable with the fork before this integration; use PREVIEW (your current setting) or CUSTOM.

Required release order: WebUI preview release `r3639` → server release → launcher release.
Status 2026-10-24: the integration heads were pushed to `origin/master` and the fork CI published WebUI preview
`r3639`, server preview `v2.4.2539` and launcher `1.0.15`; a fresh server installed `r3639` from the fork release.

## Runtime / build requirements

| | Requirement | Notes |
|---|---|---|
| Server runtime | Java 21+ (`jvmTarget` 21); bundles use Zulu/JBR 25 | Tested here with Oracle JDK 21.0.8 |
| Server build | Gradle 9.8.1 (wrapper), JDK 21+, ~2 GB Gradle heap (`org.gradle.jvmargs=-Xmx2048M`) | CI uses JDK 25 |
| WebUI | Node `>=24.20.0` (`.nvmrc` 24.20.0), pnpm 11.1.2 | Built here with Node 24.14.0 (engine warning only) |
| Launcher | Gradle 9.8.0, JDK 21+ to build, target Java 8 bytecode; starts the server with `<launcher dir>/jre/bin/java[w]` or `java` on PATH | |
| Database | H2 (default) or PostgreSQL; fork schema step written for both | PostgreSQL path not executed here |

## Custom cross-repository integration

| Integration | Where |
|---|---|
| Server bundles/updates the fork WebUI | `WebUITypes.kt`, `Constants.kt`, `server/build.gradle.kts`, WebUI mapping file |
| Server updates point to fork releases | `AppUpdate.kt` |
| Launcher downloads/updates the fork server jar | `ServerUpdater.kt` (`xkana-shii/Suwayomi-Server[-preview]` latest release) |
| Windows bundle downloads the fork launcher | `scripts/bundler.sh`, `scripts/resources/Suwayomi Launcher.bat` |
| Fork GraphQL API used by the WebUI | Komikku ports (scanlator settings, blacklist, bookmark colors, category hierarchy, Smart Categories, tracker autofill/unified edits; fillermarks removed 2026-10-24 — a WebUI older than the Komikku-port branch cannot load manga pages on this server), metadata editing/providers, cover upload, shutdown, clearDatabase, nonLibraryCount, installMissingExtensionsFromBackup |
| Shared setting | `serverUpdateChannel` (server config, edited by the launcher) |

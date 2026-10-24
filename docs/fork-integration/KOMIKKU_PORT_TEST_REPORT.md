# Komikku port — test report

Environment: Windows 11, JDK 21, Node 24.14.0, pnpm 11.1.2. Only disposable data was used: the test suite's H2
database and a scratch server root with a local-source library created for the test.

## Automated

| Repo | Command | Result |
|---|---|---|
| Server | `./gradlew :server:test ktlintCheck` | **406 tests, 0 failures**, ktlint clean |
| Server | `--tests "*ChapterVisibility*"` | 14/14 |
| Server | `--tests "*NestedAndSmartCategoriesTest"` | 5/5 |
| Server | `--tests "*TrackerEditsAndMetadataTest"` | 3/3 |
| Server | `--tests "*KomikkuPortBackupTest"` | 3/3 |
| WebUI | `pnpm gql:codegen` (against the new server) | all operations validate |
| WebUI | `pnpm tsc`, `pnpm lint`, `oxfmt --check`, `pnpm build` | pass |

New server tests cover:
- merge selection (priority, last covering range rule, source order ties, all uploads of the chosen scanlator,
  notices and unnumbered chapters), range rule parsing;
- blacklist keys (URL exact, hybrid surviving a URL change, ambiguous legacy/hybrid keys hiding nothing, name
  disambiguation of notices), blacklisted chapters never reported as merge hidden;
- priority stability (new scanlators appended alphabetically, unseen scanlators keep their position);
- per-user isolation of blacklist and visibility, unread counts, global toggle → recompute, manga override, refresh
  with a new scanlator and a changed URL keeping rules and flags, blacklist entry never duplicated or lost;
- category tree walks with cycles/orphans, sibling-unique names, no moves into the own subtree or below the default
  category, children moving up on delete;
- Smart Category scoping by reachability, anchoring, deterministic status across trackers, per-user isolation, no
  filing of non-library manga, unchanged membership without a usable status;
- score conversion across scales, validation of unified edits, autofill tracker support and tag normalization;
- backup round trip of scanlator rules, colors and category hierarchy/bindings, restore without the fields keeping
  local data, legacy fillermarks becoming blacklist entries, repeated restore matching nested categories with duplicate names.

## Runtime (disposable server + built WebUI, in-app browser)

| Scenario | Result |
|---|---|
| Scanlator settings dialog: merge On, reorder, range rule; no Save | auto-saved, survives page reload and a source refresh; server flags and unread count correct |
| Blacklist from the chapter menu | the blacklisted upload disappears and the other scanlator's upload of that number appears; Updates feed and unread count exclude it |
| Reveal blacklisted chapters | shown dimmed with blocked icon; filter state persisted |
| Bookmark color via context menu | color menu with all colors, green bookmark shown in the list, stored per user |
| Reader navigation | next chapter skips the blacklisted upload |
| Nested categories | library tabs = top-level categories with subtree counts and subcategory chips; settings tree, edit dialog with parent and Smart binding at 375 px width |
| Library settings → Chapters | renders the global merge toggle, open refresh, bookmark color defaults |

## Not tested / manual verification needed

- Tracker metadata autofill and unified tracker edits against real trackers (no accounts in the disposable
  environment); covered by unit tests up to the network call.
- Automatic refresh with an online source (local source only); long-press on a real touch device.
- PostgreSQL; multi-user through the WebUI (multi-user is covered by server tests).
- Upgrading your real database (not touched — take a backup first, see MIGRATION_AND_ROLLBACK.md).

## Continuation (2026-10-24): fillermark replacement and remaining features

Tests ran with `-PtestServerPort=4598` because a real Suwayomi instance of the user was listening on the default
port 4567 (it was not touched; the test bootstrap aborts when the port is taken).

| Repo | Command | Result |
|---|---|---|
| Server | `./gradlew :server:test ktlintCheck -PtestServerPort=4598` | **420 tests, 0 failures**, ktlint clean |
| Server | `--tests "*FillermarkMigrationTest"` | 3/3 |
| Server | `--tests "*KomikkuPortGqlTest"` | 4/4 (GraphQL end to end, two users) |
| Server | `--tests "*CategoryUpdateScheduleTest"` | 4/4 |
| Server | `--tests "*ForkFeaturesTest" "*KomikkuPortBackupTest"` | 4/4, 3/3 |
| WebUI | `pnpm gql:codegen`, `pnpm tsc`, `pnpm lint`, `oxfmt --check`, `pnpm build` | pass |

New coverage:
- fillermark migration: per-user ownership (two users), same number and name with a different URL not matched,
  multiple scanlators, a chapter without number, existing blacklist kept without duplicates, idempotent repeated
  start, resume after an interrupted run, column and settings removed only after verification, a migrated entry
  following its chapter through a URL change; legacy `CHAPTER.FILLERMARK` databases;
- legacy backups: proto 15 restored as blacklist entries without duplicates on repeated restore; proto 15 no longer
  exported; `0x2|0x20|0x100` round trip keeps Mihon's sort bit and creates no fillermark meta;
- GraphQL as two users: bookmark colors per user, hidden chapters' colors left out of `bookmarkColors`, Updates
  filter by color, unread counts after blacklisting, removing a bookmark removes its color, the per-user
  `smartScanlatorMerge` setting via `setUserSettings`, rapid priority/range-rule edits ending in the last state,
  overlapping range rules (later wins), a new scanlator appended without disturbing the saved order, hidden
  categories and intervals, visible subtree counts with a hidden child, another user unable to change a category;
- merge: a preferred upload appearing later takes over, a missing one falls back to the priority, chapters without a
  valid number never merged; a re-uploaded chapter keeps each user's bookmark color;
- unified tracker edits: a tracker without credentials is reported as skipped (never as updated);
- update schedule: inherited inclusion and intervals, subtree refresh, due categories, global category wins;
- backups: hidden state and update intervals round trip.

Runtime (scratch root, built WebUI):

| Scenario | Result |
|---|---|
| Upgrade of a library with fillermarks (previous build) by the new server | 3 fillermarks → blacklist entries, existing entry kept, column dropped, unread counts correct |
| Bookmark colors, hidden category, visible counts, Updates color filter (API) | as expected |
| Category refresh of a parent | the child category is part of the update |
| WebUI settings pages (Library, Downloads, Tracking) | new settings present, no fillermark controls |
| WebUI manga page after the upgrade | loads (after the stale PWA cache of the scratch origin was cleared); chapter rows and menus could not be inspected: the in-app browser pane was hidden, which stops virtualized lists from rendering |

Not verified / manual checks needed:
- Visual checks of chapter menus, color menus, the Library/Updates filter popovers, hidden-category library tabs
  and mobile layouts after this continuation (pane hidden). The first port's browser checks (scanlator dialog,
  blacklist, colors, reader navigation, nested categories at 375 px) still apply to unchanged code.
- Long press on a real touch device.
- Real tracker accounts (autofill and remote edits); only the no-credentials and validation paths are tested.
- Automatic refresh against an online source; scheduled automatic updates over real time (the schedule rules are
  unit tested, the local-source manga in the scratch library are skipped by the updater).
- PostgreSQL.

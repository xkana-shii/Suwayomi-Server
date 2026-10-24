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
  local data, fillermarks staying fillermarks, repeated restore matching nested categories with duplicate names.

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

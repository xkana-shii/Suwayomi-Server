# Komikku feature ports

Branches: Server `feature/komikku-ports`, WebUI `feature/komikku-ports` (local only). Backups of the starting
points: `backup/main-pre-komikku-ports-20261024` (Server), `backup/master-pre-komikku-ports-20261024` (WebUI).

## 1. Smart Scanlator Merge

- Per chapter number one scanlator is shown: the scanlator of the **last** range rule covering the number, else the
  first in the manga's priority, else the first in source order; all uploads of that scanlator stay. Unnumbered
  chapters and notices (announcement/hiatus/dated posts without a chapter prefix) are never merged.
- Global switch: per-user setting `smartScanlatorMerge` (Library settings → Chapters). Per manga: Global / On / Off.
- WebUI: chapter filters → **Scanlators** opens the scanlator settings (merge mode, Mihon-like ordering with up/down
  buttons, show/hide per scanlator = the existing excluded-scanlators filter, coverage per scanlator, range rules,
  Blacklist tab). No Save: changes are auto-saved in order (`SequentialWriter`), optimistic, with error toast and
  rollback to the saved state.
- API: `MangaType.scanlatorSettings`, `updateMangaScanlatorSettings`, `ChapterType.isMergeHidden`,
  `ChapterFilter.isMergeHidden / isHidden`.
- "Show merged duplicates" filter reveals hidden duplicates in the list (marked, dimmed); reading and downloads
  never use them.

## 2. Chapter blacklist

- Chapter menu / selection: **Blacklist chapter** / **Remove from blacklist**. New entries use Komikku's hybrid v2
  key (URL + number + scanlator + name); URL, hybrid v1 and legacy keys from Komikku backups are understood.
- Blacklist tab of the scanlator settings: hide/reveal switch, entries (also those whose chapter disappeared), remove,
  and "Blacklist fillermarked chapters".
- "Show blacklisted chapters" filter reveals them in the list (blocked icon, dimmed); unread counts, Updates, reader
  navigation and downloads keep excluding them.

## 3. Colored bookmarks

- Komikku palette (Default = theme color, Red, Orange, Yellow, Green, Blue, Purple), stored per user.
- A click on a bookmark control bookmarks with the default color (Library settings: default color, or remember the
  last picked color) or removes the bookmark; **long press** (touch) or the **context menu** (right click, menu key,
  Shift+F10) opens the color menu with a check on the current color. Used in the chapter menu, selection actions
  and the reader bookmark button.
- Colors are shown in the chapter list, reader controls, reader chapter list, Updates and History.
- Chapter filters: **Bookmark colors** with an Edit dialog of tri-state checkboxes (include / exclude / ignore).
- Existing bookmarks have color 0 (default). Re-uploaded chapters keep the bookmark color with the bookmark.

## 4. Smart Categories

- A category can be bound to a unified tracker status (Reading, Rereading, Plan to read, On hold, Completed,
  Dropped). On every track record write, library manga are filed into matching bound categories:
  top-level bindings are always in scope, subcategory bindings only when the manga reaches their parent; manual
  memberships and other branches are untouched; removals keep the manga anchored in its branch. Without a usable
  status (untracked or no mappable status) nothing changes. Status across several trackers: the first tracker by id
  with a mappable status (deterministic). Syncing only writes memberships, so it cannot loop.

## 5. Nested categories and Set Categories

- `parentId` on categories (NULL = top level), names unique among siblings, cycle-safe moves, deleting a category
  moves its children up. Default category cannot be nested.
- Category settings: indented, collapsible tree (collapsed state per browser), add subcategory, edit dialog with
  parent and Smart Category binding, drag reordering among siblings.
- Set categories: depth-first indented tree (bulk tri-state assignment unchanged).
- Library: tabs are top-level categories showing their whole subtree once (`subtreeMangaCount`), subcategory chips
  narrow a tab to a branch. Existing flat categories are top level; nothing is reinterpreted by name.

## 6. Tracker metadata autofill and editing

- Metadata editor: **Autofill from tracker** for MyAnimeList, AniList, MangaUpdates and MangaBaka bindings (fork
  metadata providers, by the binding's remote id). Preferred tracker setting or a choice when several are bound;
  fills the fields for review only (saved as the usual local overrides when the user saves); tags are trimmed,
  de-duplicated and merged after the existing ones.
- Tracking dialog: **Edit all trackers** (status, chapter, score 0–10 → each tracker's nearest score, dates) with
  per-tracker Updated / Skipped (no equivalent, no dates, not logged in) / Failed results.
- The MangaBaka unauthenticated score-type fix is unchanged.

## 7. Automatic refresh of opened manga

- Library settings → Chapters → **Refresh opened entries** (off by default). The page shows the saved data and
  refreshes details and chapters in the background, at most once per entry per 10 minutes, deduplicated across
  rerenders and remounts. A failed refresh keeps the data; the warning icon retries it. Scanlator rules, blacklist
  and bookmarks survive refreshes (server recomputes visibility).

## 8. Advanced chapter filters

Read/unread, downloaded, bookmarked, fillermarked (existing) plus bookmark colors (tri-state), show blacklisted, show
merged duplicates and the scanlator settings. They compose with AND, persist per manga with the existing global
defaults mechanism, and work on desktop and mobile.

## Not ported / differences

- Library-wide and Updates-wide bookmark color filters (Komikku has them) — only the chapter list filter.
- Komikku's per-manga preferred tracker map — a global preferred tracker instead.
- Komikku's hidden categories (KMK `hidden`) and subcategory update scopes — update/download inclusion stays per
  category as before.
- Komikku's per-manga/tracker last-failure map for unified edits — results are returned per request instead.

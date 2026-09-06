# pkg/util/hint parity audit (baseline a85e0fd5df)

Full-file audit of Go `pkg/util/hint` (hint.go, hint_processor.go,
hint_query_block.go) against `rust/crates/tidb-hint` (statement.rs,
processor.rs, query_block.rs, plan.rs).

## Fixed this batch (behavior)

1. `NTH_PLAN(<1)` now clamps `force_nth_plan` to -1 after assigning the
   value (Go hint.go:521-525), so `/*+ NTH_PLAN(0) */` no longer leaves an
   "enabled-looking" 0 that made `task_map_need_backup()` return true where
   Go returns false. Regression: `nth_plan_zero_clamps_to_disabled`.
2. The HYPO_INDEX database key is lowercased like Go's `DBName.L`
   (hint.go:364) — both the checker input and the added-hypo index map key.
   Regression: `hypo_index_database_key_is_lowercased`.
3. An empty-name `qb_name()` no longer occupies the query-block-name slot
   (Go hint_query_block.go leaves `qbName` empty), so a later named
   `qb_name(x)` registers without a spurious "more than two query names"
   warning.

## Deliberate divergences (documented at sites)

- `contains_table_hint` matches case-insensitively: Go compares the
  SQL-written hint name case-sensitively against lowercase caller names, so
  an uppercase-written `/*+ USE_PLAN_CACHE() */` is silently missed there.
  This crate normalizes hint names to canonical uppercase at parse time and
  keeps the case-insensitive match so the hint works in every written case.
  (A case-sensitive "fix" broke the plan-cache hint surface.)
- `NO_INDEX_LOOKUP_PUSHDOWN()` with no args skips where Go indexes
  `Tables[0]` and panics; the skip is the panic-path substitution.
- `fill_default_database` normalizes the LEADING element tree (Go only fills
  the flattened `Tables` copies), so LEADING binding text gains explicit
  `db.` prefixes — a Rust-only normalization, recorded here.
- The inapplicable-hint warning restores hint names with backquotes where
  Go uses flag-0 raw names; differs only when identifiers need quoting.

## Open modeling item

- `READ_FROM_STORAGE`: Go's parser emits one hint per engine group
  (`read_from_storage(tiflash[t1]), read_from_storage(tikv[t2])`), while
  this workspace's parser keeps one hint with two groups and the restore
  space-joins them. PlanHints contents are equivalent; the restore text and
  RemoveDuplicatedHints keys differ. Aligning needs a tidb-parser/tidb-ast
  change and is recorded as an open item.

## Verified matching (highlights)

All 39 ParsePlanHints switch arms (names, arity, defaults, warnings);
ParseStmtHints last-wins/duplicate texts, MEMORY_QUOTA removal + unlimited
warning, RESOURCE_GROUP, set_var first-wins with 3126 texts, restricted
filtering + 13-name warn list, warning codes 1105/3126/1815; hint
processor block/index counters, insert-case skip, view 3-pass handling,
`sel_`/`upd_1`/`del_1` offsets, unknown-qb warnings, GenerateQBName incl.
the "Unexpected NodeType %d" text; hint struct/restore formats and the
`matchTiKVOrTiFlash` pre-Matched copy quirk; CollectUnmatchedHintWarnings
order and alias-suffix texts; RemoveDuplicatedHints first-wins on
lowercased restore.

## Validation

- `cargo test -p tidb-hint --lib` (2 new regressions),
  `cargo test -p tidb-session --lib` unchanged from the pre-batch baseline
  (the known shared-branch failure set), `cargo fmt`, `git diff --check`,
  `make lint`.

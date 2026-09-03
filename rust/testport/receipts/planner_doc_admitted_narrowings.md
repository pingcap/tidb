# Rust `tidb-planner` doc-admitted narrowing inventory

Status: audit-only inventory. Every entry below is a narrowing, refusal, or
tolerance the Rust code itself declares in a comment. Each entry still owes
an individual Go comparison (`origin/master` `a85e0fd5df`) before it can be
called either parity or a fixable divergence; none is claimed fixed or
parity here. Comparison is limited to the code's own declarations.

## Inventory

- `find_best_task.rs:173-180` — merge-join enumeration forms refused where
  Go builds them; `:243` — physical-property compatibility refused ("a
  property Go would accept may be refused here", NAMED RESIDUE).
- `enforce.rs:55-59`, `:78-82` — MPP exchanger enforcement refused while the
  `t.HashCols` input is unported (`EnforceExchanger` narrowing).
- `task.rs:468`, `:1584-1586` — task conversions needing `OriginSchema`
  refuse by name; TopN/Limit over unported children refuse instead of
  mis-building.
- `plan_builder.rs:118-120` — the `windowAggMap` half of
  `resolveWindowFunction` narrowed; `:2092`, `:2843-2845` — InSubquery and
  lateral ORDER BY-LIMIT refusals.
- `plan_builder/window.rs:90-116`, `:838-861`, `:1163`, `:1244` — window
  frame/offset/nth-argument refusals where Go folds or errors later.
- `plan_builder/cte.rs:38-116` — two named narrowed fields (`ConsumerCount`,
  `limitLP`) plus five recursive-path refusals.
- `prepared_dml.rs:485`, `:1016` — non-simple assignment expressions
  refused; overflow kept as a refusal.
- `ranger/points.rs:1407-1434` — cast conversion "TOLERATING" note: which
  events are tolerated versus `return nil`.
- `predicate_partition.rs:46` — AntiSemiJoin "additionally refuses to
  derive" (pushes left-side predicates only).
- `selectivity_greedy.rs:207` — `None` versus `Some(0.0)` stats-refusal
  semantics.
- `fix_control.rs:287` — hex-literal parse refuses default-silently for
  "Rust-only planner consumers".
- `cardinality/row_count_estimator.rs:24` — untested branch, "divergence
  would be silent".
- `read_only_scan.rs:1493-1504` — double-read/cache-table attach refusals;
  FOR UPDATE refused early.
- `final_mode_agg.rs:513-515` — Go logs a WARNING where the Rust side
  refuses through a channel.
- `txn_mode.rs:124` — general divergence-cost note.
- `physical/mod.rs:1795`/`:1810` — `table_plan_explain(...).unwrap_or_default()`
  in explain strings only.

## Disposition

These stay until their owning Go surface is walked individually. The
boundary sweeps in `planner_rule_child_access.md` deliberately did NOT
touch them: they are feature-surface gaps (unported fields, MPP/TiFlash
modes, hint plumbing), not child-access refusals, and "fixing" them without
the owning surface would invent behavior Go does not have.

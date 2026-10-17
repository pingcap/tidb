# Plan digest and normalized plan for the statement summary

This ExecPlan is a living document. Keep the Progress, Surprises, Decision Log and Outcomes sections current at every stopping point.

## Purpose and outcome

TiDB identifies the shape of an execution plan by a plan digest: a sha256 hash of the plan's normalized text, in which constants and statistics-dependent details are removed. The statement summary tables (`information_schema.statements_summary` and its history, cluster, and `statements_summary_evicted` relatives), bindings created from history (`CREATE BINDING FROM HISTORY USING PLAN DIGEST`), the slow log and TopSQL all key on it. Before this change the Rust node always reported an empty `plan_digest`, so a query that appears in the summary could not be matched to its plan, and a binding could not be created from history.

After this change, running a statement that the summary records produces the same 64-hex `plan_digest` as Go TiDB on this branch whenever both engines choose the same plan. Observe it by running the corpus topic `planner/core/issuetest/planner_issue` (its two statements in `TestLongQueryBindingFromHistory` read `plan_digest` from `cluster_statements_summary`) and by the unit tests added with this change.

## Context for a reader new to the repository

Go's package `pkg/util/plancodec` is already ported as `rust/crates/tidb-util/src/plancodec.rs`: it has the plan type id table (`id.go`), `compress`/`decompress` (snappy then base64), `encode_plan_node`, `normalize_plan_node`, `decode_plan`, `decode_normalized_plan` and the binary plan decoder. The part that is missing is on the planner side, in Go `pkg/planner/core/encode.go`:

`NormalizeFlatPlan` walks the select-plan part of a flattened physical plan (the flatten is done with `buildSideFirst=false`, so children keep their natural order, index 0 first). For every operator it writes one line through `plancodec.NormalizePlanNode`: the depth, the numeric plan type id, the task type (root, or cop with the store type left out for TiKV) and the operator's `ExplainNormalizedInfo()`. The sha256 of the whole text is the plan digest. For a DML statement only the select-plan part is hashed; an operator that is not a physical plan makes the result empty.

`ExplainNormalizedInfo()` is implemented by fifteen Go operators; every other operator inherits the base implementation, which returns the empty string. The normalized text replaces constants with `?` (through `expression.SortedExplainNormalizedExpressionList`), replaces the partitions of an access object with `partition:?`, drops range and statistics information from scans, and names a child by its plan type rather than its explain id. When the global variable `tidb_ignore_inlist_plan_digest` is on, selections and projections use `SortedExplainExpressionListIgnoreInlist` instead, which collapses `in (...)` lists.

Rust renders explain text in `rust/crates/tidb-executor/src/explain.rs`: `physical_explain_roots` builds `ExplainOperator` trees (build side first, like Go's explain), `physical_operator_info` produces each operator's text, `physical_operator_name` its type, `physical_access` its access object, and `process_plan_info` returns a `ProcessPlanInfo` that the session turns into the statement summary's `LazyStatement` in `rust/crates/tidb-session/src/observation.rs`. `LazyStatement::plan_digest` currently returns an empty string.

## Plan of work

Milestone 1 adds the normalized renderers. In `explain.rs`, add `physical_normalized_info` covering the fifteen Go operators: PhysicalSelection, PhysicalProjection, PhysicalTopN, hash and stream aggregation, PhysicalHashJoin, PhysicalMergeJoin, PhysicalIndexJoin (also used for index hash join and, with the merge flag, index merge join), PhysicalTableReader (empty), PhysicalIndexReader (`index:` plus the index plan's type), PhysicalTableScan, PhysicalIndexScan, PointGetPlan and BatchPointGetPlan. Add the normalized variant of an access object (`partition:?`), `sorted_explain_normalized_expression_list` already exists in `tidb-expr`; add the in-list variant and honor `tidb_ignore_inlist_plan_digest`.

Milestone 2 adds the normalization walk. Add `normalize_physical_plan` in `explain.rs` that mirrors Go's `FlattenPhysicalPlan(p, false)` plus `NormalizeFlatPlan`: skip the DML root, walk children in order, then the reader sub-plans (table plan with the reader's store type, index plan on TiKV, index lookup and index merge table sides marked as the index-lookup probe child, which inherits to descendants and turns a table scan into `TableRowIDScan`), skip a plan that is not physical, and return the text with its sha256 digest.

Milestone 3 wires it. Add `normalized_plan` and `plan_digest` to `ProcessPlanInfo`, compute them in `process_plan_info_with_brief`, and make `LazyStatement::plan_digest` and the summary record's `plan_digest` return the digest. The Rust code has no slow-log or TopSQL plan-digest hook yet, so the summary is the only consumer. `CREATE BINDING FROM HISTORY` stays refused: Go also needs `GenHintsFromFlatPlan` to turn a recorded plan back into hints, which is a separate port.

Milestone 4 proves it against the Go oracle. Write a differential probe set (the `difftool` flow described in the memory note go-oracle-probe-harness) that runs a spread of statements through Go and Rust and compares `plan_digest` byte for byte, and add regression tests with the Go digests as expected values.

## Concrete steps

Work from the repository root. Build and test with `cd rust && cargo test -p tidb-executor --lib explain` and `cargo test -p tidb-server --lib plan_digest`. Re-run the corpus topic with `cd rust && INTEGRATION_SHOW_DIVERGENCES=1 INTEGRATION_TOPIC=planner/core/issuetest/planner_issue cargo test -p tidb-server --lib corpus_unistore -- --ignored --nocapture` and expect the topic to report zero divergences. Before committing run the locked server build `cd rust && cargo build --locked -p tidb-server`.

## Validation and acceptance

For every probe statement on which Go and Rust produce the same plan, the plan digest equals Go's (23 of 24 in the first probe set and 27 of 30 in the second; the rest are plan divergences listed under Surprises). `select plan_digest from information_schema.statements_summary` returns 64 hex characters for a recorded SELECT, an empty string for a statement without a physical plan (for example `SET`), and the select-plan digest for an INSERT ... SELECT. `tidb_decode_plan` on the recorded plan prints the same tree as `EXPLAIN`. The gate (`nextest` over the main crate set, the in-process corpus replay) reports no new failures.

## Idempotence and recovery

All steps are additive; a failed build is recovered by reverting the touched files with git. The probe flow restores the corpus source file it temporarily extends.

## Progress

- [x] (2026-10-16) Confirmed the plancodec package itself is already ported in `tidb-util`; the gap is the planner-side normalization and digest.
- [x] (2026-10-16) Milestone 1: normalized operator renderers (`physical_normalized_info` and helpers in `tidb-executor/src/explain.rs`).
- [x] (2026-10-16) Milestone 2: normalization walk and digest (`normalize_physical_plan`).
- [x] (2026-10-16) Milestone 3: wiring into `ProcessPlanInfo` and the statement summary; `tidb_ignore_inlist_plan_digest` threads from the session through `StmtContext`.
- [x] (2026-10-16) Milestone 4: differential probes against the Go oracle and the regression test `statement_summary_plan_digests_match_go` (23 Go digests) in `unistore_cop.rs`; unit tests for the two `tidb-expr` fixes.
- [ ] Follow-ups, each its own task: the plan divergences below; `CREATE BINDING FROM HISTORY`; runtime info in the recorded sample plan.

## Surprises and discoveries

The summary's `plan` column in Rust is far shorter than Go's because Go's sampled plan includes runtime execution info (actRows, execution info, memory, disk); that is a separate gap from the digest and is recorded here so it is not mistaken for a digest bug.

Two shared helpers were wrong and are fixed here: `sorted_explain_normalized_expression_list` de-duplicated through a set although Go's `slices.Sort` keeps duplicates, and a cast rendered under its signature name (`cast_decimal(...)`) with a different type string where Go prints `cast(arg, decimal(10,0) BINARY)`. Go's recorded plan also drops the `PhysicalShuffleReceiverStub` (no type, no id) and keeps its data source at the stub's depth.

The remaining digest differences all come from plan divergences the digest merely exposes. `select * from a where id in (select id from b)` keeps a `StreamAgg` below the join where Go eliminates it because `b.id` is a primary key (the direct forms `select distinct id from b` and `group by id` already match). A CTE joined to a table chooses an index join where Go chooses a hash join. `SELECT ... FOR UPDATE` on a point get adds a `SelectLock` operator in autocommit where Go builds a plain `Point_Get` (and `LOCK IN SHARE MODE` is accepted where Go raises 1235 without `tidb_enable_noop_functions`).

## Decision log

Normalization is implemented as its own walk over `PhysicalPlan` rather than as a mode of `physical_explain_operator`, because Go's normalized walk keeps the natural child order while the explain builder reorders children build side first. Reusing `physical_operator_name` and `physical_access` keeps the type names and access objects identical to the explain output.

## Outcomes and retrospective

The Rust node now records a plan digest in the statement summary that matches Go's wherever the two engines agree on the plan. The digest turned out to be a sensitive probe of plan divergences, which is useful in itself. The recorded sample plan still lacks runtime information, and bindings from history remain unsupported.

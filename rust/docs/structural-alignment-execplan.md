# Align the Rust node's structure with Go TiDB, in dependency order

This ExecPlan is a living document. Keep Progress, Surprises and discoveries, Decision log and Outcomes current at every stopping point.

## Purpose and outcome

The Rust node is meant to answer every statement the way Go TiDB (the "oracle", run from a scratch worktree of this branch) does. For a long stretch the work fixed one wrong answer at a time. Each fix was real, but most of them were emulations of a structural difference: Rust decides something at a different time, in a different place, or twice, where Go decides it once. Emulations pile up and then have to be deleted when the structure is corrected. The user directive of 2026-10-17 is to correct the structure first, in dependency order, accepting temporary regressions, and to stop adding emulations for anything a structural item covers.

After this plan: the replay harness cannot hide a regression, privileges are decided from the plan the way Go decides them, a view body is built by one code path, and the statement paths that exist twice (in-process session and cluster node) share one definition per statement kind. Observe it through the replay (`cargo test -p difftest-result-tests --test integration_diff integrationtest_replay_matches_recorded_tidb_output`), the cluster replay (`INTEGRATION_TOPIC=... cargo test -p tidb-server --lib corpus_unistore -- --ignored`) and the Go oracle probes (`difftool.sh` in the session scratchpad).

## Context for a reader new to the repository

`rust/crates/tidb-session` holds `Session`, the statement dispatcher (`dispatch.rs`), privilege collection (`table_privilege.rs`, `identity.rs`) and the in-process DDL entry points. `rust/crates/tidb-executor` holds the volcano executors, the in-memory `Catalog` (`driver/catalog.rs`), the DML drivers and the in-process DDL (`ddl/`). `rust/crates/tidb-exec` holds the cluster tier: `cluster_ddl.rs` plans DDL against the durable meta key-value store, and `table_info_build.rs` builds `TableInfo` the way Go's `ddl` package does. `rust/crates/tidb-planner` is the port of Go's logical planner (`plan_builder*.rs`). `rust/crates/tidb-server/src/cluster_session_node` is the production session: it wraps `tidb_session::Session` and routes DDL and writes to the cluster tier. A "visit info" is Go's list of (database, table, privilege) entries that the plan builder appends while planning and that `CheckPrivilege` verifies after planning. The "replay" runs the recorded TiDB integration tests (`tests/integrationtest/t/**` with results in `r/**`) through Rust and compares.

## The structural mismatches, with their symptoms and risks

First, the replay harness skipped statements both engines rejected without comparing the error code, and skipped statements Rust refused but Go answered (`OutOfDomain`) without any budget. Both hid real divergences (about 150 wrong codes; a jump from 5 to 61 out-of-domain statements went unreported). Risk of fixing: low; it only makes red what was already wrong.

Second, privileges are collected from the syntax before planning (`table_privilege.rs`), while Go collects visit info during planning and verifies afterwards. Symptoms already patched by emulation: view definer security (1356), a CTE named as an UPDATE target (1046 instead of 1288). Risk: every statement kind needs its visit info appended in Go's order, and the planner builds only SELECT plans, so write statements need their own hook.

Third, a view body is processed by two things: `view.rs` canonicalises it against the catalog, and the planner expands it later. Symptoms: the replaced-view name had to be hidden by catalog surgery before the planner got a renaming-view field. Risk: SHOW CREATE VIEW text and stored bodies must not change.

Fourth, the in-process executor DDL (`tidb-executor/src/ddl`, about 23,000 lines) and the cluster DDL (`tidb-exec`, about 18,000 lines) each implement statement semantics, and the cluster node calls into both. Symptoms: nearly every DDL error-code fix had to be made twice. Risk: highest of all; it is the largest surface and the in-process tier is what most unit tests run on.

Fifth, execution details are incomplete: commit and write details are blocked on the native committer, DML source reads bypass the coprocessor, EXPLAIN ANALYZE runtime info is missing, and Go's grouped-range ordered scans (`PropMatchedNeedMergeSort`) are unported.

Sixth, the planner keeps three logical trees (builder, join enumeration, statistics) that are not bridged. Risk: very large; it is recorded here and scheduled last because no other item depends on it.

## Plan of work

Milestone 0 makes regressions visible. The replay compares error codes by default, keeps an exact list of recordings known to be stale (each confirmed against the oracle), and asserts an `OutOfDomain` budget that may only go down. Do this before touching any structure so that every later change is judged by a stricter instrument.

Milestone 1 scouts and then moves privilege decisions to plan time for reads. First read how Go appends visit info in `buildDataSource`, `BuildDataSourceFromView`, `buildInsert`, `buildUpdate` and `buildDelete`, and list every Rust site that builds a plan. Then give `PlanBuilder` a visit-info list, append in Go's order for SELECT, subqueries and views (definer clearing included), verify after planning, and delete the definer emulation in `identity.rs` and the CTE ordering patch in `planner_bridge.rs`. Writes follow in the same milestone only if the driver can append in Go's order; otherwise the milestone ends with writes documented and scheduled.

Milestone 2 makes a view body one code path. Compare `view.rs` canonicalisation with what the planner already produces, move the remaining logic into the planner, and delete the catalog-surgery fallbacks. SHOW CREATE VIEW output is the acceptance test and must stay byte-identical.

Milestone 3 scouts the DDL duplication before committing to a design. Inventory which statement kinds the cluster node already routes through `cluster_ddl.rs` only, which it validates in-process first, and which only run in-process. The decision to be recorded afterwards is whether the in-process tier becomes a thin front over the cluster definitions or the shared definitions move into one crate; choose the smaller reversible step, and migrate one statement kind at a time with a gate per kind.

Milestone 4 completes execution details: DML reads through the coprocessor, EXPLAIN ANALYZE runtime info, grouped-range ordered scans. Commit details wait on the native committer.

Milestone 5 is the planner's three trees. It starts with a prototype that bridges one pair of trees for one rule, and only continues if the prototype shows the bridge is tractable.

## Validation and acceptance

Every milestone ends with the same gate: the main-crate nextest run, the in-process replay, and the extra-crate nextest run, followed by the locked `tidb-server` build. A milestone is accepted when the replay reports no divergence outside the stale list, the `OutOfDomain` budget has not grown, and the new behavior is pinned by a test that fails before the change. Report the compared-statement count too: a drop in it means statements moved into a skipped class.

## Idempotence and recovery

Each milestone lands as its own commit through the pre-commit hook, so any one can be reverted alone. A structural change that makes the replay worse is reverted only if the worsening cannot be explained as emulation code that the change makes redundant.

## Progress

- [x] (2026-10-17) Milestone 0: the replay compares error codes by default, `KNOWN_STALE_RECORDINGS` is exact, and the `OutOfDomain` budget is 5. Making the code comparison default exposed one more real divergence (an ENUM default from an expression, 1265), fixed in the same change.
- [x] (2026-10-17) Milestone 1a, reads: a query's privileges come from the plan builder's visit info (`plan_builder/visit_info.rs`): SELECT per resolved table, the locking-read visit, and Go's view handling (a DEFINER view's body verified against the definer by host-pattern match and then cleared, an INVOKER view's body appended). The session collects them through `driver::query_visit_info`; the syntactic read collector and the definer emulation are deleted. Go's preprocessor rule that a missing table is 1142 for an account with no privilege on it is ported (`deny_missing_tables_without_privileges`).
- [x] (2026-10-17) Milestone 1b, writes: INSERT, UPDATE and DELETE read their sources through `driver::dml_visit_info` (the source query planned like a SELECT, subqueries in VALUES / ON DUPLICATE / SET planned on their own, Go's `popVisitInfo` for a non-qualified DELETE), and CREATE VIEW's body through `query_visit_info`. The write privileges on the targets still come from `resolve_write`, which already plans the FROM. The CTE-ordering patch in `update_privilege_tables` stays: it is the plan-side resolution `resolve_write` depends on, not a syntactic emulation.
- [ ] Milestone 2: one view-body code path.
- [ ] Milestone 3: DDL duplication (scout first, then a recorded design decision).
- [ ] Milestone 4: execution details.
- [ ] Milestone 5: planner trees (prototype first).

## Surprises and discoveries

The first replay with the harness change reported 56 fewer compared statements than the run before it. The cause was a new view-definer check that did not match the definer's host pattern (`root`@`localhost` against the stored `root`@`%`), so views created by that definer became unreadable and their statements fell into `OutOfDomain`. Only the compared-count drop and the skip-class totals showed it, which is why the budget assertion exists.

The visit-info pass plans a statement a second time, which is not free of effects. Three had to be neutralised: planning raises warnings (truncated afterwards), constant folding evaluates builtins (`RELEASE_ALL_LOCKS()` ran twice; folding is suspended on the thread for the pass, null-flag derivation included), and an uncorrelated subquery is run at plan time (the pass answers with a row of NULLs instead, because the catalog it plans against is not the statement's execution image, whose virtual tables are materialised per statement). A pass that fails for any reason contributes no visits and the statement's own planning raises the error, which also keeps Go's observable `get_lock(..), no_such_fn()` folding order. Double planning is the temporary price of Milestone 1a; the plan's end state plans once and checks after planning.

## Decision log

Structure first, symptoms later, by direction of the user on 2026-10-17. Harness visibility comes before every refactor so that regressions show. The stale-recording list is explicit and exact rather than a tolerance, so a recording that becomes correct again has to be removed by hand.

## Outcomes and retrospective

Milestone 0 is done. The rest is not started.

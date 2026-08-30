# Align the TPC-DS workload between Go TiDB and Rust TiDB

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root. This plan must be maintained according to that file and the root `AGENTS.md`.

## Purpose / Big Picture

The user wants the Rust TiDB implementation on `pingcap/tidb:hparser-integration` to behave like Go TiDB for the TPC-DS workload: the chosen plans must follow the Go implementation, query results on a deterministic minimum dataset must be identical, and Rust single-client performance must not regress. The observable end state is a reproducible local run that starts `tiup playground nightly`, loads one shared fixture, runs the same supported TPC-DS statements against the Go and Rust SQL endpoints with every relevant concurrency setting equal to one, and emits a query-by-query plan, result, error, and latency comparison. Any Rust correction is covered by a regression test and pushed to `hparser-integration` without overwriting concurrent work.

The Go source of truth in this checkout is `pkg/planner/core/casetest/tpcds/tpcds_test.go::TestTPCDSQ64`, backed by `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_in.json` and `tpcds_suite_out.json`. It covers Q64 only. The broader workload comes from `pingcap/tidb-bench` at commit `e9f058ae9bee089afdbf9b3397ed9948bf7e560b`; its `tpcds/genquery.sh` generates 42 TiDB-supported statements from the 99 TPC-DS templates.

## Progress

- [x] (2026-08-27 09:20Z) Located and read the Go Q64 test, its 13-table schema, input query, expected plan, and benchmark.
- [x] (2026-08-27 09:20Z) Pinned the external `tidb-bench` source to `e9f058ae9bee089afdbf9b3397ed9948bf7e560b` and inventoried its 42 supported query IDs.
- [x] (2026-08-27 09:20Z) Preserved the earlier Q64 TiKV-only evidence: equal result SHA-256, aligned major join-family counts, and a Rust p50 improvement against its preceding remote baseline.
- [x] (2026-08-27 10:00Z) Generated the 42 SQL statements and extracted a deterministic 68 MiB SF1-derived fixture (25 tables; each fact table capped at 50,000 rows; manifest recorded under the local evidence workspace).
- [x] (2026-08-27 10:00Z) Started one `tiup playground nightly` and the Rust `b7e5a7e7` release server against the same PD/TiKV, enabled 25/25 TiFlash replicas, and captured both source (MPP requested) and control (TiKV-only) matrices at one concurrency.
- [x] (2026-08-27 10:00Z) Classified the observed differences: source-mode plan/store-tier mismatch is the unsupported Rust TiFlash/MPP execution tier; control-mode differences are Rust physical-plan/cost choices; all 42 result hashes match; Q6 was an EXPLAIN-only DISTINCT trace failure.
- [x] (2026-08-27 10:00Z) Implemented and tested the Q6 trace fallback in `tidb-executor` with a scalar-DISTINCT regression test. This is a focused Rust behavior unit, not a claim of complete Go package transcreation.
- [x] (2026-08-27 10:00Z) Re-ran the full 42-query comparison after the correction and recorded the final receipt; no plan or result errors remain.
- [x] (2026-08-28 01:10Z) Corrected grouped TopN direct-field aliases and injected CASE NULL rendering, covered both with focused Rust tests, and re-ran the full 42-query matrix; TiKV-only exact plan matches increased from 6/42 to 9/42 and result hashes remain 42/42.
- [x] (2026-08-28 02:00Z) Matched Go's root StreamAgg choice for a single integer SUM above a joined source (TPC-DS Q48), including the joined-child explain trace and a result/plan regression test; the release matrix now reports 10/42 TiKV-only plan matches and 42/42 result matches.
- [x] (2026-08-28 02:10Z) Matched Go's global aggregate TopN explain text for TPC-DS Q96 (`Column` instead of `count(1)`), covered by a plan-trace regression test, and re-ran the 42-query matrix; TiKV-only exact plan matches increased to 11/42 and result hashes remain 42/42.
- [x] (2026-08-28 03:40Z) Preserved the established executor path for legal scalar/correlated subquery shapes that the shared planner cannot lower yet: the gate now uses the original AST shape, declined receipts fall back without 1105, multi-table subquery leaves remain full-width, and ordinary fallback joins use hash lowering. Added a multi-table correlated aggregate execution regression test; the Q6 plan and result now execute successfully on the rebuilt release server.
- [x] (2026-08-28 04:00Z) Rebased the focused fix onto remote `hparser-integration` tip `0dedc2fe0d8c6b5c0c19f40ee36f18dd3edb9451`. A follow-up full matrix was intentionally stopped at Q62 to honor the request to pause; the earlier complete 42-query receipt remains the authoritative matrix snapshot.
- [x] (2026-08-30 09:00Z) Re-ran the 42-query one-concurrency smoke matrix on the newer remote tip `6472010efa` after the shared planner alignment advanced. This is recorded separately in `rust/testport/receipts/tpcds_latest_647_20260830.md`: 6 Rust plan errors, 13 Rust result errors, one result-hash mismatch, and no completion claim. The earlier 42/42 receipt remains scoped to its older revision.
- [ ] Complete the Ready verification profile, including `make lint`, before claiming the entire goal complete.

## Surprises & Discoveries

- Observation: the repository's authoritative Go TPC-DS casetest contains only Q64, not all 99 benchmark queries.
  Evidence: `tpcds_suite_in.json` has one suite named `TestTPCDSQ64` and one case; `.agents/skills/tidb-test-guidelines/references/planner-case-map.md` also describes only Q64.

- Observation: `tidb-bench/tpcds/genquery.sh` deliberately excludes 57 templates and generates 42 statements: 3, 6, 7, 9, 10, 13, 15, 19, 25, 26, 28, 29, 34, 35, 41, 42, 43, 45, 46, 48, 50, 52, 55, 61, 62, 65, 66, 68, 69, 71, 72, 73, 76, 79, 84, 85, 88, 90, 91, 93, 96, and 99.
  Evidence: the pinned `genquery.sh` exclusion list and a shell enumeration over 1 through 99.

- Observation: the Rust tree has MPP metadata types and cost functions but still explicitly refuses several MPP task attachment paths and has no TiFlash execution tier.
  Evidence: `rust/crates/tidb-planner/src/task.rs` returns named errors for MPP selection, projection, limit, TopN, StreamAgg, and HashAgg attachment; `rust/crates/tidb-executor/src/access_cost.rs` documents that no TiFlash/MPP path is enumerated.

- Observation: the earlier Q64 source-setting plan cannot yet be identical because Go produces TiFlash/MPP nodes while Rust produces none.
  Evidence: Go produced 156 plan rows, including 147 MPP rows; Rust produced 76 rows and zero MPP rows. Under a TiKV-only one-concurrency control, Go produced 84 rows and Rust 77, with the same two MergeJoin, one IndexHashJoin, and one HashJoin choices.

- Observation: with TiFlash replicas available, all 42 benchmark statements execute successfully and produce byte-identical result hashes on Go and Rust, but Rust still emits no MPP operators. The source-mode normalized plan hash matches 0/42; the TiKV-only control-mode hash now matches 11/42 after plan-trace corrections for Q3, Q43, Q52, Q48, and Q96.
  Evidence: `/private/tmp/tpcds-matrix-5e37abd.json`; Go source Q3 begins `TableReader -> ExchangeSender (mpp[tiflash])`, while Rust source Q3 remains a TiKV `IndexJoin` tree.

- Observation: the Rust endpoint is substantially slower on this minimum fixture even with every relevant concurrency variable set to one. This is an outstanding performance requirement, not a waiver: the latest source-mode Rust/Go p50 median is 4.42x and control-mode is 1.38x. The focused Q6 and Q48 corrections did not establish a broad regression relative to their pre-fix runs (Q6 correction p50 geometric ratio 1.03x source, 1.04x control; Q48 control 1.02x), although Q25 remains a 1.53x outlier.

## Decision Log

- Decision: Treat the Go Q64 casetest as the exact plan oracle for Q64, and use a live nightly Go server as the behavioral oracle for the other 41 `tidb-bench` statements.
  Rationale: no Go unit-test expected plans exist for the other statements in this checkout, so inventing expected plans would violate the no-speculation rule. The same nightly binary and shared fixture still provide direct observable parity evidence.
  Date/Author: 2026-08-27 / Codex.

- Decision: Build a full workload matrix before choosing the next implementation package.
  Rationale: starting with MPP metadata because Q64 mentions TiFlash would be a broad speculative port. A query-by-query failure matrix identifies the smallest complete behavior boundary that improves the real workload and supplies a regression test.
  Date/Author: 2026-08-27 / Codex.

- Decision: Keep MPP plan parity as a required end-state item, not as fabricated explain output and not as a waived limitation.
  Rationale: the user asked for plan alignment, and the Go source test enforces MPP. Printing MPP-looking rows without planning and executing the corresponding task would preserve a different end state and be incorrect.
  Date/Author: 2026-08-27 / Codex.

- Decision: Treat direct-field aliases and NULL arms in injected grouped projections as
  plan-trace formatting bugs only when the underlying executor shape is already the
  same, and cover each correction with a plan-producing regression test.
  Rationale: Q3/Q52 and Q43 have identical TiKV operator trees and results; changing
  planner choices would add risk without moving the physical execution toward Go.
  Date/Author: 2026-08-28 / Codex.

- Decision: Use one client and set all exposed scan, lookup, join, aggregation, projection, window, stream aggregation, and optimizer concurrency variables to one for comparable measurements.
  Rationale: this directly implements the user's one-concurrency constraint and removes default parallelism as a source of latency variance.
  Date/Author: 2026-08-27 / Codex.

## Outcomes & Retrospective

The full minimum-fixture matrix covers all 42 generated statements in both requested source and TiKV-only control modes. The complete snapshot records result correctness for this fixture (42/42 hashes equal in each mode), Q6 with no EXPLAIN error, and Q3, Q43, Q48, Q52, and Q96 focused regression coverage. The follow-up on remote tip `0dedc2fe0d8c6b5c0c19f40ee36f18dd3edb9451` adds a multi-table correlated aggregate execution regression and keeps Q6 executable after the shared-planner alignment changes; its intentionally interrupted matrix is not a replacement for the complete snapshot. Plan parity remains incomplete (0/42 source, 11/42 control) because the Rust tree still lacks TiFlash/MPP execution; the latest absolute Rust latency is also higher (4.42x/1.38x median p50 versus Go). The exact Q64 Go unit-test MPP oracle is likewise still unmet. The plan therefore remains active: the next implementation unit must be an explicitly scoped MPP package boundary or another measured cost/executor correction, followed by the Ready profile.

The newer remote tip `6472010efa` has a separate smoke receipt. It is intentionally
not folded into the older acceptance snapshot: the shared planner changes now
expose subquery, physical-column, and retained-lookup errors in 13 statements,
plus one result mismatch. Those failures must be fixed and revalidated against
the Go source packages before any completion claim.

## Context and Orientation

`pkg/planner/core/casetest/tpcds/main_test.go` creates the 13 Q64 tables and marks each as having a TiFlash replica in mock planner metadata. `tpcds_test.go` turns on `tidb_enforce_mpp`, sets both broadcast thresholds to zero, and compares `EXPLAIN FORMAT='plan_tree'` with the expected `tpcds_suite_out.json` rows. The benchmark in the same file loads captured statistics and measures Q64 planning.

`rust/crates/tidb-executor/src/driver.rs` and its `driver/` modules lower parsed statements into runnable Rust executors and build the Rust explain trace. `rust/crates/tidb-executor/src/access_cost.rs` prices table and index access. `rust/crates/tidb-planner/src/task.rs`, `physical_property.rs`, and the physical operator modules model Go physical properties and tasks, including partially transcreated MPP metadata. `rust/testport/receipts/tpcds_q64.md` records the current focused Q64 evidence.

The external workload checkout is `/private/tmp/tidb-bench-tpcds`; it is an evidence workspace and must not be committed into TiDB. Generated query and data files are ignored by that repository. The implementation workspace is `/private/tmp/tidb-tpcds-replay`, a detached clean worktree created from the current remote `hparser-integration` tip so the user's large dirty primary worktree remains untouched.

A minimum fixture means the smallest deterministic rows needed to make the supported statements exercise meaningful branches. Empty tables are not sufficient because an empty-result equality can hide expression, aggregation, join, and ordering defects. The fixture may be derived from a prefix or selected relational slice of SF1, but its generation rule, row counts, and checksum must be recorded so the same rows can be recreated.

## Plan of Work

First generate the 42 supported SQL files from the pinned `tidb-bench` tools. Parse their referenced tables, columns, literal filters, dates, and join keys. Generate SF1 source data only as an input corpus, then select a small relationally consistent slice that gives non-empty Go results for as many queries as possible. Store the reproducible extraction script and manifest under `rust/testport/tpcds/`; do not commit generated bulk `.dat` files.

Next start `tiup playground nightly` with one PD, one TiKV, one TiDB, and no monitoring, then start the release Rust server from the clean worktree against the same PD. Load the schema and fixture through Go so both servers see identical data. For each SQL file, run `EXPLAIN FORMAT='plan_tree'` and the statement itself on fresh sessions with concurrency one. Normalize plan IDs and nondeterministic runtime details only; preserve operator type, task/store, access object, join keys, predicates, aggregation, order, and row result bytes. Run five warmups followed by twenty sequential timed executions for statements that succeed on both servers.

The loader's `--set-tiflash-replica` option should be used on the Go endpoint
before the Rust process joins the cluster; wait for every row in
`INFORMATION_SCHEMA.TIFLASH_REPLICA` to report `AVAILABLE=1` before collecting
the source (MPP-requested) mode.

Write the matrix as machine-readable JSON plus a Markdown receipt. The matrix must distinguish a SQL error from a plan mismatch, result mismatch, and performance regression. Use the current remote Rust binary as the pre-change performance baseline and reject a change when the median or p95 regression persists across repeated alternating-order runs beyond the documented noise threshold.

Then select the highest-impact failure class that maps to a coherent existing Rust crate and the corresponding Go source package. Inventory the entire Go package before making any transcreation claim. Add a regression test that fails before the code correction and passes afterward. Prefer existing Rust test modules nearest to the behavior. Implement the production change without unrelated formatting or renaming.

After the focused test passes, rebuild the Rust server, repeat the complete matrix, update this plan and the receipt, run the Ready validation profile, fetch the latest remote branch, replay the focused commits in a temporary clean worktree, and push without force.

## Concrete Steps

Run all TiDB commands from `/private/tmp/tidb-tpcds-replay` unless stated otherwise.

Generate the supported statements from the pinned workload checkout:

    cd /private/tmp/tidb-bench-tpcds/tpcds
    ./genquery.sh
    find queries -maxdepth 1 -name 'query_*.sql' -type f | sort -V

Start the shared nightly environment with explicit non-default ports:

    tiup playground nightly --tag codex-tpcds-full-20260827 --pd 1 --kv 1 --db 1 --without-monitor --perf --port-offset 13000

Build and start Rust after the scoped change. If macOS system OpenSSL discovery fails, enable the vendored OpenSSL feature only in the temporary worktree and restore the manifest before staging:

    LC_ALL=POSIX LANG=POSIX CARGO_NET_OFFLINE=true rustup run nightly-2026-08-22 cargo build --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-server --release
    rust/target/release/tidb-server --store tikv --path 127.0.0.1:15379 --cluster-session --load-privileges --host 127.0.0.1 --port 18000 --status 18080

Run focused Rust checks while iterating:

    LC_ALL=POSIX LANG=POSIX CARGO_NET_OFFLINE=true rustup run nightly-2026-08-22 cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p <changed-crate> <FocusedTest> --lib

Before a completion claim, run the Ready gates selected by `AGENTS.md` and `.agents/skills/tidb-verify-profile`, including:

    make lint

Do not run `make bazel_prepare` unless the change adds, removes, moves, or renames a Go file, changes a Go import block, adds a top-level Go `TestXxx`, changes Bazel files or test targets, or changes `go.mod`/`go.sum`.

## Validation and Acceptance

The workload inventory milestone is accepted when exactly 42 generated SQL files are present and a manifest maps every ID to its referenced tables and expected Go execution status.

The fixture milestone is accepted when a fresh nightly database can be loaded from the committed generator/extractor, table row counts and fixture checksum match the receipt, and the Go result is non-empty for every query where a small relational slice can reasonably satisfy the query's literals. Queries that are inherently empty under the minimal fixture remain valid comparison cases but are explicitly labelled weak-result coverage.

The parity milestone is accepted only when every one of the 42 statements has recorded Go and Rust plan/result status, all successful Rust results equal Go after documented deterministic normalization, and every remaining plan mismatch is enumerated. Q64 additionally must match the Go unit-test plan oracle, including MPP/TiFlash task structure; a TiKV-only match alone is insufficient.

The performance milestone is accepted when each mutually successful statement has one-client warmup and sequential measurements, Rust has no repeatable regression against the pre-change remote Rust baseline, and the receipt reports absolute Go/Rust latency without claiming equality when they differ.

The full goal is accepted only after targeted regression tests, a release build, the complete runtime matrix, and the Ready profile all pass. Missing Go tooling, missing TiFlash execution, or untested statements mean the goal remains active.

## Idempotence and Recovery

Query generation deletes only `/private/tmp/tidb-bench-tpcds/tpcds/queries`, which is ignored and reproducible. Playground data uses the explicit tag `codex-tpcds-full-20260827`; stop the owning TiUP process normally before deleting any tagged data. Never run a recursive deletion against a repository root or a home directory.

All implementation work stays in the detached `/private/tmp/tidb-tpcds-replay` worktree until it is committed. If the remote branch advances, create another clean worktree at the new tip and cherry-pick the focused commits; never force-push. The primary worktree has hundreds of unrelated user changes and must not be reset, cleaned, or reformatted.

If a cargo build requires vendored OpenSSL, restore `rust/third_party/tikv-client-rs/Cargo.toml` before staging and verify `git diff -- rust/third_party/tikv-client-rs/Cargo.toml` is empty.

## Artifacts and Notes

Current Q64 normalized result checksum on the earlier deterministic 13-table fixture:

    7acb112028cde67ae46294bd9171fc65dcf3674f5a32b429c6adf221056465f5

Earlier TiKV-only twenty-run medians after five warmups:

    Go nightly:       18.999 ms
    Rust before fix: 142.292 ms
    Rust after fix:  131.615 ms

These numbers are evidence for Q64 only and must not be extrapolated to the 42-query workload.

## Interfaces and Dependencies

The workload harness must use the MySQL protocol against the two SQL endpoints and must not depend on private in-process Rust APIs, because end-to-end parity includes session variables, planning, execution, and result encoding. It may use the installed `mysql` client and POSIX shell tools; if a small parser or report generator is needed, use the repository's bundled workspace Python runtime and keep the script under `rust/testport/tpcds/`.

The implementation boundary will be chosen only after the baseline matrix. Likely dependencies are `tidb-parser`/`tidb-ast` for syntax, `tidb-planner` for logical/physical properties and MPP tasks, `tidb-executor` for lowering and runtime semantics, and `tidb-session` for variables and protocol behavior. Any cross-crate change must state which Go package each behavior comes from and must avoid claiming a complete transcreation until the repository-wide atomic-package rule is met.

Revision note (2026-08-27): created the plan after the Q64-only iteration to restore the full 42-query workload and exact MPP plan requirement as the active acceptance scope.

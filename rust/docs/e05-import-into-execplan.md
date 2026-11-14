# Port IMPORT INTO from its Go owners

This ExecPlan follows root `PLANS.md`. Keep its progress and evidence current.

## Purpose / Big Picture

E05 tracks the refusal of IMPORT INTO in `tidb-session/src/dispatch.rs`. The requested outcome is Go-compatible local CSV, SQL and Parquet import, including the planner, importer and distributed task lifecycle. The current Go distributed owner is `pkg/dxf/importinto`, not the audit's historical `pkg/disttask/importinto` path. A validation-only increment does not close E05 or complete either Go package.

## Progress

- [x] Read root guidance; confirmed the initial worktree is clean.
- [x] Trace Go `ImportIntoExec.Next`, `buildImportInto`, importer options, source discovery and physical encoding dependencies.
- [x] Port the planner assignment visitor and all 24 cases of `TestImportIntoCollAssignmentChecker`; wire validation before option checks. Baseline session regression failed with the old unsupported error; the visitor, session regression and two containment tests now pass.
- [ ] Local source preparation is not ported. The prerequisite audit below prevents claiming the requested CSV/SQL/Parquet execution.
- [x] Confirm missing physical-import prerequisites outside the requested owners; keep executable imports refused.
- [x] Captured-input-variable regression passes; E05 registers say partial (13 unresolved: 2 open, 11 partial).
- [x] Full suites completed: planner 913 passed; expression 1186 passed; session 4301 passed, one timing failure, five skipped. The failing timing test passed when isolated on both original and changed source; the full run remains not fully green.
- [x] Commit `a518c0476b9` contains the planner visitor, registry visibility change and captured Go tests. The second increment contains session wiring, its regression and this partial-status audit. No push attempted.

## Surprises & Discoveries

`pkg/executor/importer/import.go:InitDataFiles` checks `filepath.IsAbs(e.Path)` on the original path. A `file://` URI is classified as local but fails this check. It must not be silently normalized into an accepted absolute path.

Rust's `tidb-lightning-mydump` contains the CSV reader. No Rust `TableKVEncoder`, `BaseKVEncoder`, Lightning local ingest backend, SQL dump parser or Parquet reader was found. `tidb-extsort`'s SST writer is an external-sort file implementation, not the missing TiKV physical import backend. Go local imports submit standalone DXF tasks and ingest physical KVs; routing through the existing transactional LOAD DATA implementation would change semantics.

## Decision Log

- Decision: Use the user's four Cargo environment variables and `-j6`, superseding the repository's usual 12 workers. Never delete build artifacts.
  Rationale: Explicit task constraints and shared build directories.
  Date/Author: 2026-11-14, Codex.
- Decision: Keep executable IMPORT INTO refused until its physical backend and job lifecycle exist. Port independently testable preparation in Go ownership locations and record the remaining gap explicitly.
  Rationale: A transactional INSERT replacement would violate the source-of-truth requirement.
  Date/Author: 2026-11-14, Codex.

## Context and Orientation

Go `pkg/planner/core/planbuilder.go` validates assignment syntax before building the import plan. Go `pkg/executor/import_into.go` builds an importer controller, discovers files, prechecks the target, submits a DXF task and returns job information. Go `pkg/executor/importer` owns file interpretation, KV encoding and ingest engines. Rust currently has only option validation in `tidb-executor/src/import_into_options.rs` before a refusal. The existing DXF framework alone does not implement an import task.

## Plan of Work

First move the assignment checks into a Rust planner module using the AST visitor and function registry. Port Go's own matrix, retaining postorder error precedence and variable collection. Wire it before the existing option checks. Next implement source preparation only where the current Go tests and available dependencies establish exact behavior. Before enabling successful writes, verify the physical encoder, ingest backend and DXF task lifecycle, including cleanup and job results. If these are absent outside the named scope, leave E05 partial with a precise prerequisite list.

## Concrete Steps

Run commands from `rust/` with the prefix:

    CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0

All Cargo commands also set `CARGO_TARGET_DIR=/Users/qiliu/.cargo/targets/tidb-7c34bc11` to reuse the shared artifacts. Use `cargo check -j6 -p <crate> --tests`, then `cargo nextest run --build-jobs 6 -j6 -p <crate> -E 'test(<name>)'`. At the end run `cargo nextest run --build-jobs 6 -j6 -p <crate>` once for every changed behavior crate. Start lengthy commands in the background and inspect their logs while continuing independent work. Do not change Go sources or other worktrees. Commit only `rust/` changes on the existing branch.

## Validation and Acceptance

Go test names become snake_case Rust tests. A regression must fail against the original implementation and pass with the change. Assignment errors must include Go's exact zero-based assignment index; accepted expressions must collect the same lowercase user-variable names. Local-source errors must preserve Go's validation order and redact storage credentials. Successful import acceptance additionally requires real row data, job lifecycle and cleanup evidence; passing preparation tests is not sufficient.

## Idempotence and Recovery

Tests may use their own temporary files and restore only their own changes. No shared artifacts are deleted. Preserve unrelated work if it appears. Any failure reproduced on the original tip is recorded separately from this change.

## Artifacts and Notes

Initial `git status --short` was empty. Initial dispatch validates options and always returns `IMPORT INTO is not supported yet`.

## Interfaces and Dependencies

The planner visitor uses `tidb_ast::Visitor`/`Visitable` and the existing `tidb-expr` function registry. Import execution depends on Go's Lightning KV encoding and physical ingest contracts, which must not be replaced by a new API or guessed behavior.

## Outcomes & Retrospective

The planner visitor is a tested preparation increment, not a completed importer or Go package. Physical import is blocked on missing prerequisite owners outside this task's named scope:

- `pkg/lightning/backend/kv`: `NewBaseKVEncoder`, required at `pkg/executor/importer/kv_encode.go:76`.
- `pkg/ingestor/ingestctrl`: `NewBackend`, required at `pkg/executor/importer/table_import.go:227`; Rust has only `tidb-ingestor-errdef`, not the engine or ingestion lifecycle.
- `pkg/lightning/mydump` SQL parser and `pkg/dumpformat/parquetfile` reader: Rust's mydump crate supplies CSV only. `pkg/executor/importer/import.go:newLoadDataParser` selects all three formats.

Within E05 the remaining work is the rest of `buildImportInto`, encoder-context validation, the importer/controller, source discovery, prechecks, task and job persistence, physical ingest, cleanup, cancellation and result rows. The new visitor currently runs at dispatch before option validation; it is not yet the full planner's build or privilege-check ordering. No imported row or live TiKV evidence is claimed.

The uncommitted Go testkit probe `zzprobe/e05/TestImportIntoInputVars` was run with `GOMAXPROCS=6 go test --tags=intest -p 6 ./zzprobe/e05 -run '^TestImportIntoInputVars$' -count=1 -v`. It compiles against this worktree and drains the returned record set. Six captured cases are stored in `rust/crates/tidb-planner/testdata/import_into_input_vars_go.txt`. The additional `file:///tmp/file.csv` case confirms error 8158, “file location should be absolute path when import from server disk”. Earlier attempts that did not drain the record set did not establish executor behavior; only the final probe result is used.

Validation so far: `cargo check -j6 -p tidb-planner -p tidb-session --tests` passes. `cargo nextest run -j6 -p tidb-planner -p tidb-session -E 'test(test_import_into_coll_assignment_checker) | test(import_into_refusal)'` passes all four tests. Before the change, `cargo nextest run -j6 -p tidb-session -E 'test(test_import_into_coll_assignment_checker)'` failed with actual “IMPORT INTO is not supported yet” versus expected “FUNCTION noexist is not supported in IMPORT INTO column assignment, index 0”. The captured-input-variable test also passes (`cargo nextest run -j6 -p tidb-planner -E 'test(test_import_into_input_vars)'`). Full suites run with `cargo nextest run --build-jobs 6 -j6 -p tidb-planner -p tidb-session --no-fail-fast`. Nextest uses `-j6` for test workers, so the full run explicitly supplies `--build-jobs 6` as well. Earlier targeted nextest builds supplied `-j6` alone and inherited the repository build-job setting. Full-suite outcomes: `cargo nextest run --build-jobs 6 -j6 -p tidb-planner -p tidb-session --no-fail-fast` ran 5215 tests: 5214 passed, one failed, five skipped (2765.968 seconds). All 913 planner tests passed. The session failure is `modify_column_runs_as_a_persisted_job_like_go` at `unistore_cop.rs:25378`: `floor((unix_timestamp() - unix_timestamp(a)) / 2)` answered 1 rather than 0 after converting a NULL to TIMESTAMP NOT NULL. It does not execute IMPORT INTO. The case passed in isolation on both original source (30.275 seconds) and changed source (28.964 seconds). The full-suite timing failure was not reproduced on the baseline and is not claimed to be a confirmed pre-existing failure. No unrelated code or timing assertion has been changed. `cargo nextest run --build-jobs 6 -j6 -p tidb-expr --no-fail-fast` passed all 1186 tests (22.308 seconds).

A compound Git invocation failed to create the worktree index lock, but the ordinary standalone command succeeded: `git commit -m 'Port Go IMPORT INTO column assignment checks' -- rust/crates/tidb-expr/src/builtin_registry.rs rust/crates/tidb-planner/src/lib.rs rust/crates/tidb-planner/src/import_into.rs rust/crates/tidb-planner/testdata/import_into_input_vars_go.txt`. Commit `a518c0476b9` contains that planner increment. The branch remains `codex/e05`; no alternate index/worktree or push was used. Session wiring and audit documentation form the second increment after the completed suites and the timing comparison.

## Final verification commands


From `rust/`, every build/test command used these four variables (and reused the shared target):

    export CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never
    export CARGO_PROFILE_DEV_DEBUG=0
    export CARGO_PROFILE_TEST_DEBUG=0
    export CARGO_INCREMENTAL=0
    export CARGO_TARGET_DIR=/Users/qiliu/.cargo/targets/tidb-7c34bc11

The successful checks and full-suite runs were:

    cargo check -j6 -p tidb-planner -p tidb-session --tests
    cargo nextest run -j6 -p tidb-planner -p tidb-session -E 'test(test_import_into_coll_assignment_checker) | test(import_into_refusal)'
    cargo nextest run -j6 -p tidb-planner -E 'test(test_import_into_input_vars)'
    cargo nextest run --build-jobs 6 -j6 -p tidb-planner -p tidb-session --no-fail-fast
    cargo nextest run --build-jobs 6 -j6 -p tidb-expr --no-fail-fast
    cargo nextest run --build-jobs 6 -j6 -p tidb-session -E 'test(modify_column_runs_as_a_persisted_job_like_go)'

The fourth command completed with the single timing failure described above, not a green result. The last command was run once against the original production source and once against the restored changes. The original-source run used `/private/tmp/e05-check-baseline.py`: it restored the four modified pre-existing Rust files from `84261b5174f`, leaving the new planner module undeclared, ran the filter, then restored the four files byte-for-byte from saved copies. No branch or worktree was switched.

From the repository root, the final Go capture and whitespace check were:

    GOMAXPROCS=6 go test --tags=intest -p 6 ./zzprobe/e05 -run '^TestImportIntoInputVars$' -count=1 -v
    git diff --check
    git diff --cached --check

The Go probe is uncommitted. Its output is the six-case Rust test fixture; the seventh probe case establishes Go's file-URI refusal. No `pkg/` source changed, no target/cache artifacts were deleted, no corpus milestone or live TiKV import ran, and no successful IMPORT INTO execution or whole owning-package completion is claimed. Missing prerequisite implementations, rather than a remaining test-only adjustment, prevent closing E05.

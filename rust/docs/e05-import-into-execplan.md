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

## Resumed scope (2026-11-14)

The user expanded scope to include the missing physical backend, ingestor,
SQL/Parquet readers and task/job lifecycle. These are repository work, not
external blockers. The earlier outcome's out-of-scope classification is
superseded. Current Go has no logical IMPORT INTO mode option; follow the
physical path. First wire the planner handoff and pin target restrictions and
SELECT width to Go, then implement controller/source handling, encoding and
ingest, the executor and DXF lifecycle. Never remove target artifacts.

The initial resumed status contains untracked `zzprobe/` only; preserve it and
never commit it. A new Go probe `TestImportIntoPlannerHandoff` confirms temporary
and cached targets are rejected before executor option validation, and SELECT
width includes visible generated columns. Its parser rejects arbitrary option
expressions (`thread=1+1`, `thread=missing_col`); do not claim these are valid SQL.

### Planner handoff increment

`common_plans::ImportInto` now retains resolved target metadata, rewritten
options, assignments, generated expressions, the job schema, and the SELECT
child. `buildImportInto` checks temporary/cached targets and SELECT width and
collects the five target privileges plus FILE for server sources. The executor
bridge optimizes the SELECT child before handing the plan to session dispatch.
Execution still refuses writes after option validation. Remaining planner work
includes URL parse errors, SEM v1 and nextgen S3 external-ID handling and a
complete snapshot/privilege regression inventory; this increment is not a claim
that all of `buildImportInto` or E05 is complete.

The original-source regression `test_import_into_build_plan` failed with MySQL
1105 (unsupported) instead of Go 1136. Go probes
`TestImportIntoPlannerHandoff` and `TestImportIntoPlanFields` ran successfully
with `GOMAXPROCS=6 go test --tags=intest -p 6 ./zzprobe/e05 -run
'^TestImportIntoPlannerHandoff$' -count=1 -v` and the corresponding PlanFields
filter. Captures live under `tidb-planner/testdata/import_into_{handoff,plan}_go.txt`.
The current `cargo check -j6 -p tidb-planner -p tidb-session --tests` passes with
the four required environment variables and shared target. Targeted nextest
uses `--build-jobs 6 -j6 -p tidb-planner -p tidb-session -E
'test(test_import_into_) | test(import_into_refusal)'`.

The installed nightly's `rust-objcopy` cannot find `libLLVM.dylib` by its bundled
rpath; it exists in the same toolchain's top-level lib directory. Commands now
set `DYLD_FALLBACK_LIBRARY_PATH=/Users/qiliu/.rustup/toolchains/nightly-2026-08-22-aarch64-apple-darwin/lib`.
This avoids the strip warning without changing compilation profiles or artifacts.
An initial check launched from the repository root was interrupted before
continuing; all subsequent checks execute from `rust/` to use its Cargo config.

Targeted handoff verification completed: all eight tests passed in 42.243s.
Full crate suites are deferred to the end of the resumed task, as the user
explicitly requested. The row remains partial and counts are unchanged.

### Importer interpretation increment

Ported the interpretation/options portion of importer Plan, defaults and
adjustment, CSV config, field mapping and load-column validation, file-type
selection, and the CSV arm of HandleSkipNRows. Plan session/metadata capture,
serialization, SEM/node limits, source discovery, other reader types, encoding
and execution remain. No successful import is claimed.

Go probes TestImportControllerMapping and TestImportCSVInterpretation ran with
`GOMAXPROCS=6 go test --tags=intest -p 6 ./zzprobe/e05 -run '^<name>$' -count=1 -v`.
Captures preserve nine mappings and six CSV interpretation/error cases. Eight
targeted Rust tests passed: `cargo nextest run --build-jobs 6 -j6 -p tidb-executor
-E 'test(importer::)'`; `cargo check -j6 -p tidb-executor --tests` passed. Both use
the required environment and shared target above. New test modules depend on
new controller APIs and cannot compile without the implementation. Full suites
remain deferred to the end.

### SQL reader and local discovery increment

The SQL ChunkParser follows parser.go/parser.rl's tokens and four states,
including read-block boundaries, exact row lengths and offsets, both SQL escape
modes, column tracking, binary literals and malformed-input errors. The shared
Parser contract adds CSV row metadata and common ReadChunks/ReadUntil;
HandleSkipNRows now distinguishes CSV physical lines from SQL rows. A native
filepath.Match port and local-discovery helper preserve Go's path validation,
nonrecursive directory walk, symlink sizes and ignored per-file pattern errors.
The controller does not yet call the helper. Source metadata/types are added;
Parquet-specific and extended-column metadata remain. No executable import is
claimed; reader I/O is currently supplied as decompressed byte buffers.

Go captures: TestSQLParserCapture (828 cases from the nine original Go parser
tests, with their tiny/default block sizes and both escape modes),
TestParserContract (12 CSV/SQL metadata, skip, resume and chunk cases),
TestImportLocalFiles (16 cases), and TestImportSourceTypes (19 cases). Each ran
with `GOMAXPROCS=6 go test --tags=intest -p 6 ./zzprobe/e05 -run '^<name>$'
-count=1 -v`. Only capture output is committed; the probes remain untracked.
Go's query component is retained in the local storage base when InitDataStore
reserializes the URI; a direct what?.csv path therefore fails while the same
file is discovered by *.csv. Malformed later glob chunks can pass the empty-name
validation, so per-file match errors must be ignored, as Go does.

`cargo check -j6 -p tidb-lightning-mydump -p tidb-executor --tests` passed.
`cargo nextest run --build-jobs 6 -j6 -p tidb-lightning-mydump -p tidb-executor
-E 'test(parser::) | test(import_parser_contract) |
test(supported_suffix_for_server_disk) | test(import_source_types)'
--no-fail-fast` passed all 12 tests. Commands use the required environment above.
Intermediate comparisons exposed EOF-token and vertical-tab differences; the
final capture comparison includes those regressions. One intermediate build
saw the new local helper before its metadata dependency was rebuilt; the final
check and test run rebuild both from the completed source. Full crate suites
and successful import execution are still outstanding.

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

- `pkg/lightning/backend/kv`: `NewBaseKVEncoder`, required at `pkg/executor/importer/kv_encode.go:76`. Ported as `tidb-lightning-backend-kv` (with `tidb-lightning-backend-encode` and `tidb-go-rand` for the AUTO_RANDOM / SHARD_ROW_ID_BITS shards). `testdata/encode_go.jsonl` comes from an uncommitted Go master probe (`GOTOOLCHAIN=go1.27.3 go test ./zzprobe/e05kv -run TestEncodeCapture`): 25 tables, 59 rows, compared on KV bytes, row ids, error text and allocator bases. The capture exposed two table-layer differences that also affect ordinary writes, both now fixed: the row value omitted Go `canSkip`'s NULL-without-default columns, and `AddRecord` set index entries before the record. `kv2sql.go` comes with conflict resolution.
- `pkg/objstore` storage layer (needed to read IMPORT INTO sources): ported in `tidb-objstore` (`storeapi`, `objectio`, `compressedio`, `compress.go`, `local.go`, `memstore.go`, `noop.go`, `hdfs.go`, `storage.go`, and the S3/GCS/Azure storages over `opendal`). Go's local, memstore and compress tests are ported; `s3_storage_round_trip` passes against MinIO (`bitnamilegacy/minio` in Colima, since MinIO's own images and downloads are archived).
- `pkg/lightning/mydump` for IMPORT INTO: the CSV and SQL parsers now read a storage reader in Go's `blockParser` blocks (they held the whole file before), the CSV parser reads headers (`ReadColumns`, `shouldParseHeader`) and reports `ReadUntilTerminator`'s position; `OpenReader`/`NewReaderOpener`, `ToStorageCompressType`, `SourceFileMeta`/`FileInfo`/`MDTableMeta`, `ParallelProcess`, `EstimateRealSizeForFile`/`SampleFileCompressRatio` and `region.go` (`MakeTableRegions`, `MakeSourceFileRegion`, `SplitLargeCSV`, `AllocateEngineIDs`, `CalculateBatchSize`) are ported with Go's region tests. Lightning's directory loader (`MDLoader`, routing rules, schema/view import) is not, as IMPORT INTO lists its own files. Parquet row counts fail until `pkg/dumpformat/parquetfile` is ported.
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

### Source URL increment (2026-11-15)

Go `buildImportInto` begins, for a file source, with `url.Parse(ld.Path)`: an
error answers `ErrLoadDataInvalidURI` with `ImportIntoDataSource` and the
error text; `objstore.IsLocal` decides `importFromServer`; SEM v1 refuses a
server-disk source; and a next-generation kernel under SEM runs
`checkNextGenS3PathWithSem` for an S3-like source and rewrites the path with
the keyspace as `external-id`. The Rust builder classified the scheme with a
hand-written split (`import_into::import_from_server`) and had none of the
other checks.

- [x] `plan_builder/import_into.rs::check_import_source_path` and
  `check_next_gen_s3_path_with_sem` follow Go, over `tidb-go-url` (Go 1.25
  `net/url`) and `tidb-objstore` (`IsLocal`, `IsS3Like`,
  `NormalizeQueryParameterKey`, the `s3like` query keys), all added for E07's
  destination URL in the same change. `import_from_server` is gone.
- [x] The checks run before option rewriting and the assignment checks, in
  Go's order.
- [ ] The importer's local discovery (`importer/local.rs`) still reserializes
  the URI by hand; it moves to the parsed URL with the controller's
  `InitDataStore`.

Deliberate difference: the URL quoted in Go's parse error is redacted with
`tidb_ast::redact_url` before it reaches the client (root `AGENTS.md` rule 7).

Evidence: `plan_builder::import_into::tests::test_process_next_gen_s3_path`
ports Go `TestProcessNextGenS3Path`. Session regression
`tests_core::dml::import_into_source_url_is_checked_like_go` covers the 8158
parse error (also with credentials in the URL) and SEM v1's 8132 refusal for
four server-disk spellings; with the two planner files restored to the
original source it fails (recorded below). The next-generation stamping runs
only in a next-generation kernel build and was not executed here.

    cargo nextest run -p tidb-planner -p tidb-session -E 'test(import_into) | test(test_process_next_gen_s3_path)'

Baseline: with `tidb-planner/src/import_into.rs` and
`plan_builder/import_into.rs` restored from the original source, the session
regression failed at its first assertion (code 1105 instead of 8158); with the
change restored, all ten targeted tests passed.

Changed-crate suites: see the E07 plan's destination URL increment (one shared
run, 6377/6386 passed; the 9 failures reproduce on the unchanged tip).

### Go master baseline (2026-11-15)

Go master's `buildImportInto` adds Starter handling: a Starter deployment
requires an explicit non-empty external ID (`checkStarterS3Path`, answering
ErrLoadDataInvalidURI), `checkNextGenS3PathWithSem` skips the external-ID
check there, and only non-Starter deployments stamp the keyspace. Ported
with master's `TestProcessNextGenS3Path` Starter cases (`checkStarterS3Path`
reads no mode, so its cases run in every build). Master still quotes
`url.Parse`'s error in ErrLoadDataInvalidURI; that URL goes through master's
`RedactURL`, which turns an unparseable URL into `(invalid URL)`.

## Closing E05: the stacked PR plan (2026-11-16)

The user asked for full Go parity before the row is deleted, with the final
acceptance on a TiUP playground (PD + TiKV) plus MinIO for S3. MinIO's own
images and binaries are archived (HTTP 410); `bitnamilegacy/minio` runs in
Colima as container `tidb-e05-minio` on `127.0.0.1:19000`
(`minioadmin`/`minioadmin`, bucket `e05`).

Opened so far (each stacked on the previous, from `dbsid/tidb`):

- #39 kvproto refresh, #35 storage URLs/redaction, #40 Lightning KV encoder
  (`tidb-lightning-backend-kv`), #41 `pkg/objstore` storage layer, #44
  streaming mydump parsers and `region.go`.

Remaining, in order:

1. Done: `pkg/dumpformat/parquetfile` as `tidb-dumpformat-parquetfile` over the
   `parquet` crate, pinned by an 18-record Go capture (`zzprobe/e05pq`);
   mydump's Parquet row count reads it.
2. Partly done (the PR after #45): `pkg/lightning/backend` as
   `tidb-lightning-backend` (`Backend`/`EngineWriter`, `EngineManager`,
   opened/closed engines with Go's 3 import retries, `MakeUUID`), and
   `ingestctrl`'s `be::Backend` impl with a local writer, `GetTS`, and the
   imported KV count. With it, `importer`'s `kv_encode.go`,
   `chunk_process.go`, `engine_process.go` and the engine half of
   `table_import.go`, pinned by an 8-case Go capture of
   `NewFileChunkProcessor(...).Process` (`zzprobe/e05imp`): KV pairs per
   writer, group checksums, and the encode error text. Still open from this
   step: `checksum.go`, `tikv_mode.go`, and the scheduler pause hooks.
   The original scope of this step: `pkg/lightning/backend` (`backend.go` engine manager, `MakeUUID`) and the
   `ingestctrl` pieces IMPORT INTO needs beyond ADD INDEX's: local writer
   with flush status, `CloseEngine`, `checksum.go` (remote checksum),
   `tikv_mode.go`, and the import-mode / PD scheduler pause hooks. Region
   split/scatter stays under D02 unless Go-visible.
3. Partly done (the PR after the chunk-processing PR): the rest of
   `import.go` (`NewImportPlanFromInput` over the statement's validated
   options with the session snapshot, `initParameters`, the plan's Go JSON
   for task metadata, `InitDataStore`, `InitDataFiles` over `objstore` with
   compression and Parquet size estimates, `CheckDataSourceAccess`,
   `GetSortStore`), `PopulateChunks` and subtask sizing, `job.go`
   (`mysql.tidb_import_jobs`), `precheck.go`, and `table_import.go`'s
   `NewTableImporter`, `prepareSortDir`, `adjustDiskQuota`, `VerifyChecksum`,
   `RemoteChecksumTableBySQL` and `RebaseAllocatorBases` behind session and
   allocator traits. Pinned by a 15-statement Go capture (`zzprobe/e05plan`)
   and the ported `job_test.go` / `precheck_test.go`. Still open from this
   step: `sampler.go` and `CalResourceParams` (next-gen only),
   `query_source.go`, `ImportSelectedRows`, and the disk quota loop.
   The original scope: `pkg/executor/importer`: the rest of `import.go` (plan capture, options,
   `InitDataStore`, `InitDataFiles`, `GetParser`, `CheckRequirements`),
   `kv_encode.go`, `chunk_process.go`, `engine_process.go`,
   `table_import.go`, `job.go` (`mysql.tidb_import_jobs`), `precheck.go`,
   `sampler.go`, `query_source.go`.
4. Partly done (the PR after #47): `pkg/dxf/importinto` as
   `tidb-dxf-importinto` (`proto.go`, `job.go`, `planner.go`, `scheduler.go`,
   `task_executor.go` for local sort, `RuntimeInfo`), the server's
   `ImportStore` over TiKV (import client, keyspace, allocator rebase, PD
   store count, KV emptiness check), and `pkg/executor/import_into.go`'s
   file path: `ImportIntoExec` (standalone and DETACHED),
   `ImportIntoActionExec` (CANCEL IMPORT JOB), SHOW IMPORT JOB(S) and SHOW
   IMPORT GROUP(S). Pinned by the SHOW/CANCEL capture `zzprobe/e05show` as a
   unistore session test, and by a live run (below). Still open from this
   step: `encode_and_sort_operator.go`, `subtask_executor.go`'s global sort
   half, `clean_up.go`, conflict collection/resolution, IMPORT FROM SELECT,
   TiKV import mode and the PD scheduler pause.
5. Done (the PR after #48): IMPORT INTO ... FROM SELECT on the classic
   kernel (`importFromSelect`, `sendQueryChunks`, `ImportSelectedRows`,
   `FlushTableStats`), `validateStatementInTxn`, TiKV import mode
   (`tikv_mode.go`, the scheduler's `OnTick` / `switchTiKV2NormalMode`, the
   Raft-KV2 `switchModeBySplitKeys`), PD's scheduler pause on the engine's
   range (`pdutil.PauseSchedulersByKeyRange`, also for ADD INDEX's ingest),
   and the task's etcd registration (`br/pkg/utils/register.go`). A
   next-gen FROM SELECT returns Go's "outside materialized view
   maintenance" error: Go only runs it (`ImportStepQuery`) during
   materialized-view maintenance, and materialized views are not ported.
6. Done (the PR after #49): `pkg/ingestor/simplesst` as
   `tidb-ingestor-simplesst` (sorted KV and stat file codecs, `Writer`,
   `OneFileWriter`, `KVReader`/`StatsReader` with the concurrent hotspot
   reader, `MergeKVIter`, `MergePropIter`, `GetReadRangeFromProps`,
   `GetAllFileNames`), `engineapi`'s `Range`/`ConflictInfo`/`OnDuplicateKey`
   as `tidb-ingestor-engineapi`, and Go `math/rand`'s `Intn` for the
   partition prefixes. Pinned by a 24-case Go capture (`zzprobe/e05sst`):
   every file's bytes, the writer summaries, and what the readers return,
   for both writers, the 4 duplicate actions, and 0, 1 and 60 rows.
7. Done (the PR after the simplesst PR): `pkg/ingestor/globalsort` as
   `tidb-ingestor-globalsort` (`SortedKVMeta`, `DivideMergeSortDataFiles`,
   `CalRangeSize`, `RangeSplitter`, `MergeOverlappingFiles`, `readAllData`,
   the external `Engine` and `MemoryIngestData`, `CleanUpFiles`),
   `engineapi`'s `Engine`/`IngestData`, and `ingestctrl`'s
   `IngestExternalEngine` with `verifyImportedStatistics`. Pinned by a Go
   capture (`zzprobe/e05gs`) of the pure functions and of 3 overlapping
   writers' files through every split, merge and engine load, for each
   duplicate action. Go sorts a loaded batch with an unstable parallel sort,
   so which duplicate an error names and the order of a key's recorded
   duplicates vary between Go runs; the test compares those loosely.
8. The DXF global-sort steps (encode and sort, merge sort, write and
   ingest), `clean_up.go`, and next-gen async prepare.
9. Conflict collection and resolution, the global-sort live acceptance;
   delete the E05 row.

### Live acceptance of FROM SELECT and the TiKV hooks (2026-11-16)

Same playgrounds as below. `/tmp/e05-data/select.sh` (not committed)
imports from a 3000-row indexed table: all columns, a column list, an
expression with `thread=2`, an aggregate, an empty SELECT, a non-empty
target (8173), duplicate keys (checksum mismatch, and the same 8223 index
inconsistency Go leaves), DETACHED (8166), and an explicit transaction
("cannot run IMPORT INTO in explicit transaction", also through EXECUTE),
then checks the data, `ADMIN CHECK TABLE` and the AUTO_INCREMENT rebase.
Go and Rust match except which duplicate row survives in the duplicate-key
case: its source is `SELECT ... UNION ALL SELECT ...`, and this port's
UNION ALL returns its children in either order (Go's returned them in
order in 6 of 6 runs), so the first row differs. With an ordered source
both keep the first row.

`/tmp/e05-data/modes.sh` (not committed) imports 400000 rows from S3 as a
DXF task and watches the cluster: the etcd key
`/tidb/brie/import/import-into/<task>` exists with a lease while the task
runs and is gone after; PD's audit log shows 2 `SetRegionLabelRule` and 2
`PatchRegionLabelRules` (one pause per engine); TiKV logs `switch mode`
Import then Normal. Go and Rust give the same output.

### Live acceptance of the file path (2026-11-16)

A TiUP nightly playground (1 PD, 1 TiKV) per server: Go master
`eca4674a6f` (go1.27.3) as the playground's tidb-server, and this tree's
debug `tidb-server --store tikv` on its own playground. The same script
(`/tmp/e05-data/accept.sh`, not committed) imports 2000 rows into indexed
tables from local CSV with a header, gzip CSV, SQL, Parquet, a CSV glob, S3
(MinIO) CSV, an S3 glob, S3 Parquet with a column list and `SET`, and S3
gzip CSV `WITH DETACHED`; then checks count/sum/min/max and `ADMIN CHECK
TABLE`, the non-empty-target precheck (8173), `SHOW IMPORT JOBS`, the
AUTO_INCREMENT rebase, and CANCEL of a finished job (8171). With table IDs
masked the two outputs are identical. A header row imported without
`skip_rows` fails the job with Go's client error and stored job error, and
CANCEL of a DETACHED job leaves it `cancelled|cancelled by user`.

Found by the live run, each with a regression test:
- `ImportStore::keyspace` built `'x'` + 3 bytes from API V1's
  `NULL_KEYSPACE_ID`, so every local checksum had 4 extra bytes per KV.
- `ErrChecksumMismatch` and the chunk encode error became 1105 MySQL errors,
  which `common.IsRetryableError` retries (Go's `*mysql.MySQLError` rule), so
  the subtask looped forever. Go returns `*errors.Error` / plain errors here,
  which it does not retry.
- The emptiness precheck ran SQL against a table in Import mode (8258); Go
  `CheckImportIntoTableIsEmpty` scans the record prefix at a snapshot.
- IMPORT INTO from files was routed as a write ("a write statement
  unexpectedly produced rows"); Go gives it the `importIntoSchemaNames`
  result schema.
- The client error and the stored job error kept every RFC prefix; Go keeps
  the error chain, so DXF stores `[outer RFC]` + `GetErrStackMsg` and the
  scheduler fails the job with `task.Error.Error()`.

Environment notes: the Rust server reads the macOS system HTTP proxy
(reqwest), Go's `net/http` does not; the Rust run needs
`NO_PROXY=127.0.0.1` to reach PD's HTTP API and MinIO. The Go playground has
`tidb_redact_log=ON` by default, which redacts the encode error's value.

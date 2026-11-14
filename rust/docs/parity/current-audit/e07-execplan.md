# Execute Go BRIE statements

This living ExecPlan follows the repository root `PLANS.md`.

## Purpose / Big Picture


E07 tracks BACKUP/RESTORE statements that parse but never execute. The intended outcome is Go-compatible physical local backup/restore, job inspection and cancellation. `local://` and `file://` select archive storage; they do not turn BR into a SQL dump. Successful backup/restore requires the TiKV backup/import services, PD safe points, Go metadata, and restore DDL.

## Progress


- [x] Read root guidance and trace `pkg/executor/brie.go`, its tests, task runners and existing Rust dispatch.
- [x] Port queue/progress, inspection/cancellation and the SQL builder fields with source-pinned tests; physical job registration/execution remains absent.
- [x] Port local metadata reading and SQL SHOW BACKUP METADATA; original Go fixtures and both SQL time zones pass.
- [ ] Physical backup/restore remains unimplemented: RunBackup/RunRestore, backup.Client, restore/snap_client, PD/GC/checkpoints, TiKV Backup/ImportSST, tidbGlue and restore DDL. E07 stays partial.
- [x] Run targeted tests and changed crate suites; replay unrelated failures and reconcile E07 in both registers.
- [x] Commit the implementation as `bfcfe4b0feb`; record the remaining gap in a separate documentation commit. No push.

## Surprises & Discoveries


Rust `tidb-br` currently owns range/rewrite helpers, not `task.RunBackup`, `task.RunRestore`, `backup.Client`, `restore/snap_client`, or `metautil`. Generated backup and SST-import protobufs already exist. Go `pkg/executor/brie_test.go` exercises queue cleanup and builder options, not a successful local roundtrip; `br/pkg/task/show/cmd_test.go` contains real metadata fixtures and SQL assertions. Go's physical runner requires TiKV even with local archive storage.

The baseline SQL regression failed with `Unsupported("this statement kind (ADMIN Brie) is not supported yet")` (`/tmp/e07-before.log`). The port of `TestBRIEBuilderOptions` caught the Rust parser's canonical `ENCRYPTION_KEY_FILE` name. The CANCEL test caught a detached `StmtContext` warning; the executor now appends through the live session. Go's `TimeStamp.String` uses `15:03:04`, so midnight formats as `00:12:00`, not `00:00:00`; a Go probe pins that unusual layout. The privilege test initially attached sessions to different stores; sharing the root session's catalog matches Go's testkit setup.

## Decision Log


- Decision: Preserve the physical BR format and keep incomplete execution explicit in the register. A Rust-only SQL dump cannot satisfy Go's restore contract. Date: 2026-11-14.
- Decision: Use the user's explicit `-j6` and all four Cargo environment settings, overriding the general 12-worker preference. Never delete shared build artifacts. Date: 2026-11-14.

## Context and Orientation


`rust/crates/tidb-executor/src/admin.rs::AdminExec::admin_next` owns admin dispatch. `show/exec.rs` currently answers empty BR listings. `tidb-br` is the BR owner; `tidb-proto::backup` contains Go's wire messages. `pkg/executor/brie.go` owns the process-global single-worker queue and 30-minute job retention (cleanup throttled to 10 minutes). `br/pkg/task/show/cmd.go` reads metadata with `metautil.MetaReader`. Tests must follow Go test names in snake_case and cite their source.

## Plan of Work


First add the queue/executor control path under `tidb-executor/src/brie.rs`, using Go's context cancellation and the existing error types. Add BR task configuration and metadata ownership under `tidb-br`, then connect SQL metadata output through the executor. Read Go runner dependencies before implementing physical transfer. Keep E07 partial if any required execution surface remains, with precise remaining owners.

## Concrete Steps


From `rust/`, prefix every Cargo invocation with:

    CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0

Run `cargo check -j6 -p tidb-br -p tidb-executor --tests`, then `cargo nextest run -j6 -p <crate> -E 'test(<name>)'` for source tests. At the end run `cargo nextest run -j6 -p <changed-crate>` once for each changed crate. Build/test output goes to `/tmp/e07-*.log`; poll long processes while continuing source work. Run `git diff --check`, stage only `rust/`, and commit without pushing or switching branches.

## Validation and Acceptance


Go's `TestFetchShowBRIE` pins task retention and SHOW's fetch-before-clean ordering. `TestBRIEBuilderOptions` pins option routing. `br/pkg/task/show`'s `TestFull`, `TestV2AndSmallTables`, `TestV2Encrypted`, and `TestShowViaSQL` pin real Go archives and time zones. A physical local backup claim additionally requires a successful backup/drop/restore and row comparison through the TiKV-backed SQL endpoint, with the Go archive format. Passing metadata tests alone is not that proof.

## Idempotence and Recovery


Tests use isolated temporary archive directories and never modify Go fixtures. Preserve all unrelated work; do not reset or clean the checkout. Do not commit throwaway Go probes. Each coherent commit records its tested scope.

## Artifacts and Notes


Initial `git status --short` was empty. E07 initially reads `open`; register totals are 13 unresolved, 3 open, 10 partial. No execution success has been established.

## Interfaces and Dependencies


Use existing `tidb-gocontext` for cancellation, `tidb-failpoint` with Go paths, `tidb-proto` for BR messages, `tidb-ast::redact_url` for storage exposure, and Go's metadata layout. Avoid new serialized formats or successful stub runners.

## Outcomes & Retrospective


Partial implementation, not a whole-package or physical backup/restore completion claim. E07 is retained as partial in both audit files (13 unresolved: 2 open, 11 partial). The SQL metadata regression now passes, as do Go archive fixtures, builder options, retention, cancellation and query redaction tests. Combined `cargo check --tests` passed for all three changed crates. Targeted run: 20/21 passed, then the corrected shared-store privilege fixture passed its focused retest (1/1). The full changed-crate run finished in 2,651.707 seconds: 5,359 tests run, 5,357 passed, two failed, seven skipped. All 23 added E07 tests passed. The failures are `cluster_diagnostics::tests::fetches_server_info_and_merges_logs_over_grpc` (empty system information under sandbox-denied `sysctl -a`) and `tests_explain::an_index_lookup_charges_its_tasks_like_go` (2.47 KB versus 2.42 KB). An unchanged-tip replay reproduced the diagnostics failure and passed the memory-accounting test. All temporarily replaced tracked source bytes were restored and their SHA-256 hashes checked. The focused memory-accounting retest with the port restored also passed (1/1). This was a non-reproducing full-run failure, not a proven BRIE regression or a claimed fix; the full suite is not reported as green.

No production Go source was modified. The Go metadata probe ran under `zzprobe/e07/` and was removed after its output was captured; no probe source is committed. No target directory was deleted. No push or branch switch was performed. No physical TiKV backup/restore, integration-corpus replay, server build or deployment validation was run.

## Milestones


The implementation commit adds local metadata reading, SQL configuration fields, SQL inspection, queue behavior, permissions and result classification together with their dependency lockfile. It is accepted by Go's original full/V2/encrypted fixtures, key/default tests, metadata walker tests, captured edge cases, SQL time-zone output, builder options, retention and job controls. It does not claim physical transfer. A separate documentation commit updates the register and this plan with the verified outcome and remaining scope.

## Verification record


All Cargo commands run from `rust/` with `CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`; nextest additionally uses `CARGO_BUILD_JOBS=6` so both compilation and tests use six workers (`nextest -j6` controls test threads).

    cargo check -j6 -p tidb-br -p tidb-executor -p tidb-session --tests
    cargo nextest run -j6 -p tidb-br -p tidb-executor -E 'test(brie::) | test(brie_source::) | test(task::) | test(metautil::) | test(local_storage::)' --no-fail-fast
    cargo nextest run -j6 -p tidb-executor -E 'test(test_visit_info_brie)'
    cargo nextest run -j6 -p tidb-br -p tidb-executor -p tidb-session --no-fail-fast
    cargo nextest run -j6 -p tidb-executor -p tidb-session --lib -E 'test(fetches_server_info_and_merges_logs_over_grpc) | test(an_index_lookup_charges_its_tasks_like_go)' --no-fail-fast
    cargo nextest run -j6 -p tidb-session --lib -E 'test(an_index_lookup_charges_its_tasks_like_go)'

From the root, `TZ=UTC go test --tags=intest -p 6 ./zzprobe/e07 -run TestMetadataEdges -count=1 -v` passed; its output is `rust/crates/tidb-br/testdata/show-metadata-go.txt`. The probe and the tested show code contain no failpoint calls; no failpoint source instrumentation was needed. Earlier check attempts caught ordinary compile errors; two Cargo attempts from the repository root failed because the workspace is in `rust/`. An attempt to pass both nextest `-j6` and `--test-threads 6` was rejected as duplicate options and was corrected. These command setup failures are not passing validation.

Revision 2026-11-14: implemented and tested the metadata/control/configuration portion; identified and retained the physical execution gap. Full suite, unchanged-tip replay and passing current-source memory-accounting retest recorded. Implementation committed as `bfcfe4b0feb`; physical execution remains open.

The implementation is committed on the original `codex/e07` branch. The following documentation commit records this verification and preserves the E07 partial row. Source restoration after the baseline replay was verified by SHA-256, and `git diff --check` plus register-count consistency checks passed.

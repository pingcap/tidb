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


## Physical runner continuation (2026-11-14)


- [x] Re-read root guidance; verified a clean worktree at continuation start.
- [x] Trace RunBackup, RunRestore, BRIE store guards, Go unistore, and metadata writer ownership.
- [x] Port local atomic writes and Go V1/V2 MetaWriter; exact plaintext bytes match zzprobe/e07writer, AES modes pass TestEncryptAndDecrypt, and encrypted V1/V2 DDL readback matches captured Go output. Restore metadata loading now includes files, partitions, checksums, and statistics.
- [ ] Port backup ranges/client and restore metadata/checksum/schema paths, then task orchestration and BRIE invocation.
- [ ] Resolve source contract for unistore: Go buildBRIE rejects non-tikv stores; Go unistore has no Backup or ImportSST service. Clarification requested while independent porting proceeds.
- [ ] Run targeted tests per coherent piece, full changed-crate suites at the end, commit pieces, and reconcile both audit entries.

The user explicitly requests physical database/table backup-drop-restore over local/file archives, with no Go source changes. A generic SQL dump or a fabricated successful queue job is outside this contract. Each component remains a prerequisite until the actual physical round-trip passes. Preserve the Go store restriction pending the requested clarification; work on shared prerequisites meanwhile.

The first milestone adds MetaWriter and atomic local writes under tidb-br, validated by original Go tests and an uncommitted probe with deterministic output. The second adds source-backed backup and restore clients; the third connects task runners and BRIE and validates physical data and schema recovery. A local unistore extension requires an explicit contract decision because its implementation has no counterpart in Go unistore. All Cargo commands retain the four specified environment values and six build/test workers. Never delete targets or modify another worktree.


Continuation evidence: `cargo check -j6 -p tidb-br --tests` passes. Targeted nextest metadata run: 10/10 passed; edge-capture run: 2/2 passed. `TZ=UTC go test --tags=intest -p 6 ./zzprobe/e07writer -run TestMetaWriter -count=1 -v` and `-run TestMetadataEdges` pass. Fixtures retain only E07 output lines, with no temp paths or credentials. The first edge probe had a mistyped 34-byte AES key and failed as expected; corrected to the Go test's 32-byte key before capture. All probes remain uncommitted. A mistaken initial Cargo check from the repository root was interrupted; subsequent commands run from rust/ with its configured rustflags and the required environment. The toolchain emits a nonfatal rust-objcopy/libLLVM warning; tests execute successfully.

Go executable constraint probe: `TZ=UTC go test --tags=intest -p 6 ./zzprobe/e07writer -run TestUnistoreBRIE -count=1 -v` passes and confirms both statements are rejected by Go on unistore before physical execution. No physical round-trip is claimed. Source at `pkg/store/mockstore/unistore/tikv/server.go` embeds only the TiKV KV service, not Backup or ImportSST. The pending user question concerns authorizing an extension beyond this source contract.


## Continuation stopping point: Git metadata write denied


The physical-runner continuation is blocked at its first required coherent commit by an environment write denial outside this worktree. Both `git add rust/` attempts failed to create `/Users/qiliu/projects/tidb/.git/worktrees/cx-e07/index.lock` with `Operation not permitted`. The paired `git commit -m 'Port Go BR metadata writing and restore file loading'` also failed. `git rev-parse --git-dir --git-common-dir` resolves the expected metadata directory; directory ownership/modes show qiliu and 0755, and no existing lock was reported. The active approval policy cannot request elevated filesystem access. No alternate index, permission bypass, branch switch or push was attempted. All source changes remain unstaged and uncommitted for recovery once the environment permits the required Git write.

Final changed-crate suite: `cargo nextest run -j6 -p tidb-br` passed 68 tests, with 2 skipped, in 0.713 seconds (test execution). All Cargo commands use the required four environment settings plus CARGO_BUILD_JOBS=6. Only tidb-br code changed. Go source under pkg/ was unchanged; uncommitted zzprobe/e07writer holds the three source probes. Physical BACKUP/RESTORE, real cluster execution, server build, corpus replay, compatibility checks, statistics-file transfer, and the task/backup/restore clients remain unverified or unimplemented. No whole-Go-package completion is claimed. The metadata prerequisite is reviewable; the requested physical task runner is not delivered.

Both E07 rows retain partial status. The JSON had stale duplicate top-level open/partial values (3/10) despite counts and rows being 2/11; these duplicate values now agree with the actual 13 rows and the Markdown header. See e07-physical-runner-report.md for the exact command record and recovery requirements.


## Destination URL increment (2026-11-15)


Go `buildBRIE` reads the destination through `objstore.ParseRawURL`, then,
for `s3` and `gs`/`gcs`, `objstore.ExtractQueryParameters` into the task
config's embedded `BackendOptions`; `cfg.Storage` (and the job's storage and
`restoreQuery` text) is the parsed URL's `String`. The Rust builder split the
scheme off by hand, kept the raw text as `cfg.Storage`, and `BrieTaskInfo::new`
stripped S3/GCS queries with its own string slicing.

- [x] Port Go 1.25 `net/url` (the toolchain `go.mod` pins) as `tidb-go-url`:
  `Parse`, `ParseRequestURI`, escaping, `Values`/`ParseQuery`/`Encode`, `String`,
  `EscapedPath`/`EscapedFragment`, `Redacted`, `RequestURI`, `Hostname`/`Port`,
  and the `netip.ParseAddr` error paths `parseHost` reports. Components are
  bytes, as Go strings are. Not ported: `ResolveReference`, `JoinPath`, the
  binary marshalers.
- [x] Port `pkg/objstore/parse.go` as `tidb-objstore` (`ParseRawURL`,
  `ParseBackend`, `ParseBackendFromURL`, `ExtractQueryParameters`,
  `NormalizeQueryParameterKey`, `FormatBackendURL`, `IsLocal*`, `IsS3Like`,
  `BackendOptions`, GCS/Azblob options and their `apply`), and the S3 options of
  `pkg/objstore/s3like` as `tidb-objstore-s3like` (`S3BackendOptions.Apply`,
  `SetForcePathStyle`, the query-key and provider constants).
- [x] `build_brie` follows Go's order: parse, scheme switch (option extraction
  or SEM v1 refusal), store check, `cfg.Storage = storageURL.String()`;
  `task::common::Config` carries `backend_options` with Go's default
  `ForcePathStyle: true`.
- [ ] Remote storage construction from the parsed backend, BR CLI flag parsing
  and the physical runners remain as listed above.

Deliberate difference: Go's `invalid destination URL: parse "<url>": ...`
echoes the raw destination; here the quoted URL passes through
`tidb_ast::redact_url` first (root `AGENTS.md` rule 7). `ExtractQueryParameters`
walks the query in byte order; Go walks a map, so two keys that normalize to
the same tag have an arbitrary winner there.

Evidence. `tidb-go-url/testdata/net_url_go.jsonl` was printed by an uncommitted
probe that copied Go 1.25.12's `url_test.go` tables with go/ast and ran them, plus
the inputs of `TestParseErrors`, `TestRejectControlCharacters`,
`TestInvalidUserPassword`, `TestURLHostnameAndPort`, netip error paths and BR /
IMPORT INTO storage URLs (`GOTOOLCHAIN=go1.25.12 go run .`): 324 records, all
matched. `tidb-objstore` ports every test of Go's `parse_test.go` (the fake keys
of `TestParseRawURL` are replaced by other fake ones). New BRIE regression
`brie::tests::build_brie_parses_the_destination_like_go` covers the annotated
parse error, credential redaction, S3/GCS option extraction and the stored
storage/query; `brie_source::test_show_backup_query_redact` now takes its job
information from `build_brie` rather than a hand-built `BrieTaskInfo`. The new
regression reads `Config::backend_options`, which did not exist before, so it
cannot be compiled against the original source; there `build_brie` accepted
`1invalid:` (no URL parse) and kept the raw S3 query as `cfg.Storage`.

    cargo test -p tidb-go-url -p tidb-objstore -p tidb-objstore-s3like
    cargo nextest run -p tidb-br -p tidb-executor -E 'test(brie) | test(task::) | test(test_show_backup_query_redact) | test(test_cancel) | test(test_security_enhanced_local_backup_restore) | test(test_visit_info_brie) | test(test_show_via_sql)'

No physical backup or restore is claimed; E07 stays partial.

Changed-crate suites (shared with E05's source URL increment):
`cargo nextest run --locked -p tidb-go-url -p tidb-objstore -p tidb-objstore-s3like -p tidb-br -p tidb-planner -p tidb-executor -p tidb-session --no-fail-fast`
ran 6386 tests: 6377 passed, 9 failed, 9 skipped. The same 9 fail on the
unchanged tip (a detached `ngaut/hparser-integration` worktree, same filter):
six HTTP tests against local test servers (a 502 from this machine's network
path), `load_stats_*` (two; the `tests/integrationtest/s/` fixture is absent),
and the live cluster-config / Prometheus reads. `cargo build --locked -p tidb-server` passes.


## Go master baseline (2026-11-15)


The user set Go master (pingcap/tidb `eca4674a6f`, Go 1.27.3, kvproto
`9192d38c8b67`) as the source of truth for E05/E07; earlier increments read
this branch's in-tree `pkg/`. The destination URL increment was re-ported:

- `tidb-go-url` follows Go 1.27 `net/url`: strict colons for http/https
  hosts (other schemes keep Go's last-colon port), and `String` escaping a
  `//` path without authority. The capture was redone with
  `GOTOOLCHAIN=go1.27.3` (336 records).
- kvproto refreshed to master's pin first (separate PR), for
  `backup.S3.sse_kms_encryption_context`.
- `ast.RedactURL` is master's: `net/url` parse, userinfo and fragment
  removed, a per-scheme allowlist of query options (unknown keys dropped,
  secrets and endpoints masked), an invalid URL shown as `(invalid URL)`, a
  split SSE-KMS JSON context dropping the whole query. Every caller of
  `tidb_ast::redact_url` gets it; master's `TestRedactURL`,
  `TestBRIESecureText` and `TestImportIntoSecureText` tables replace the old
  ones.
- `objstore`: `ParseRawURL` and `IsLocalPath` errors quote no URL;
  `ExtractQueryParameters` fails on an undecodable query; missing-bucket
  errors redact the URL; `FormatBackendURL` keeps `oss`/`ks3`;
  `BackendOptions.Redacted`; `S3BackendOptions.SseKmsContext` and
  `encodeSSEKMSContext` (Go `encoding/json` decode and `Marshal` rules,
  pinned by a 129-record capture of `S3BackendOptions.Apply`).
- `buildBRIE` uses `task.ApplyTiDBRuntimeConfig` and annotates extraction
  errors; `brieQueue.clearTask` keeps unfinished tasks
  (`TestClearTaskKeepsUnfinishedBRIETasks`).

Go master's own `ParseRawURL` no longer echoes the URL, so the earlier
deliberate redaction difference for BRIE is gone. The job's shown storage is
still `RedactURL(cfg.Storage)` rather than Go's raw `cfg.Storage` (AGENTS.md
rule 7: Go shows an Azure query's credentials in SHOW BACKUPS).


## Physical BACKUP/RESTORE (2026-11-16)


Scope agreed with the user: everything SQL can reach. `BRIEExec.Next`
runs `task.RunBackup(ctx, glue, "Backup", cfg)` and
`task.RunRestore(ctx, glue, "Restore", cfg)` only, so the port covers
snapshot full/database/table backup and restore with every option the SQL
builder sets: LAST_BACKUP (incremental), BACKUP_TS / SNAPSHOT, compression,
encryption, rate limit, concurrency, checksum, checkpoints, ONLINE,
WAIT_TIFLASH_READY, WITH_SYS_TABLE and LOAD_STATS. Not reachable from SQL,
recorded as a new gap row when E07 closes: log backup and PiTR
(`br/pkg/stream`, `restore/log_client`), EBS snapshots, raw/txn KV mode,
the repo layout, and the `br` CLI. Go's refusal on a non-TiKV store stays.
Live roundtrips run on the TiUP playgrounds over `local://` and S3 (MinIO);
GCS and Azure are covered by Go's tables only.

Crate layout. `tidb-br` sits below `tidb-domain-core`, so it keeps the BR
packages that need no session or domain: `errors`, `version`, `conn/util`,
`gc`, `pdutil`, `utils`, `checkpoint`'s storage parts. The runners that need
a session reach it through Go's `glue.Glue` / `glue.Session` interfaces,
defined in `tidb-br::glue` and implemented by the executor (Go `tidbGlue` in
`pkg/executor/brie.go`).

PRs, each stacked on the previous one (the first on #56):

1. Done: BR connection: `errors`, `version`, `conn` / `conn/util`, `gc`,
   `pdutil`, `config` (`kv.go`), `glue`, `utils` (retry/backoff, error
   handling, `StoreManager`), over a synchronous `pd.Client` trait
   (`pdclient`); `coreos/go-semver` as `tidb-go-semver`, Go
   `strconv.Unquote` in `tidb-go-strconv`, and the PD client's keyspace
   GC barriers (vendored-client patch 120). Pinned by a Go capture
   (`zzprobe/e07conn`, `tidb-br/testdata/conn_go.jsonl`) and Go's tests.
2. Done: backup client: `backup` (`client.go`, `schema.go`, `store.go`,
   `limit.go`: ranges and schemas, the per-store push-down loop with lock
   resolution and store watching), `checksum`, the checkpoint runner and
   its backup flavour, statistics files, DDL jobs for incremental backup,
   `utils/storewatch` and the schema helpers; the catalog at a TS is read
   through `tidb-meta` over a glue snapshot (`metareader`). Pinned by Go's
   unit tests that need no cluster; schema building and DDL jobs are
   checked live in PR 3.
3. Done: `task.RunBackup` (`BackupConfig` with Go's flag defaults,
   `Adjust`, `Hash` pinned to Go's JSON and SHA-256), BRIE backup execution
   (Go `BRIEExec.Next`, `tidbGlue`, `mapBRIEErr`, `cancelBRIEOnKill`), and
   the server's BRIE node over its TiKV store (snapshot reads, coprocessor
   client, statistics by snapshot, PD label rules, internal sessions).
   `BackupSchemaVersion` is Go master's 2. Live on the playgrounds: eleven
   cases (`/tmp/e07/live.sh`) of Rust backups restored by Go with matching
   KV counts, bytes and row hashes, over `local://` and MinIO S3, plus
   Go's error texts. Rust-by-Rust restore comes with PR 5.
4. Done: restore split and import: `restore/split` (`pdClient` with
   TiKV's `SplitRegion`, batch and one-region scatter, `RegionSplitter`,
   `PaginateScanRegion`), `internal/import_client`, `import_mode_switcher`,
   `restorer` (`MultiTablesRestorer`), the snapshot `checkpoint/restore`
   items, and `snap_client`'s `import.go` (`TiDBFull` download and ingest),
   `tikv_sender.go`, `placement_rule_manager.go` and the import half of
   `client.go`. The PD client reads regions and stores from PD directly and
   scatters one region (vendored-client patch 130). Pinned by Go's split,
   restorer and `snap_client` tests over a port of Go's
   `MockPDClientForSplit`, plus Go's `TestSortAndValidateFileRanges` cases
   captured from Go master. Live (`tidb-br/tests/restore_live.rs`): Rust
   backups over `local://` and MinIO S3, and a Go backup on the Go cluster,
   imported into unused table IDs with every KV equal to the source.
5. Done: the schema half of `snap_client`: `client.go`'s loading,
   `AllocTableIDs` (`internal/prealloc_table_id`), `InitCheckpoint`,
   policies, databases and tables through `internal/prealloc_db`, DDL job
   replay, merge-option label rules, `ResetTS` and the checksum;
   `pipeline_items.go` (checksum, logical statistics load with
   `metautil.RestoreStats`, TiFlash wait) and the logical half of
   `systable_restore.go`; the table-backed snapshot checkpoint
   (`checkpoint/storage.go`, the `TableMetaManager` of `manager.go`) with
   `CheckpointMetadataForSnapshotRestore` pinned to Go's JSON; and the
   restore methods of the glue traits, erroring until the server implements
   them. Pinned by Go's `prealloc_table_id`, `prealloc_db`, client,
   pipeline and system table tests over an in-memory glue cluster. The
   `br` CLI's physical stats and system table loads, the external-storage
   checkpoint and `txn-total-size-limit` are not ported.
6. `task.RunRestore`, the restore registry, task registration and the
   log-backup / changefeed checks; BRIE restore execution and the server's
   restore glue (DDL, restricted SQL, statistics, label rules, TiFlash).
   Live: backup, drop and restore with row and checksum comparison; Go
   backups restored by Rust.
7. Delete the E07 row; record the unreachable parts as a gap row.

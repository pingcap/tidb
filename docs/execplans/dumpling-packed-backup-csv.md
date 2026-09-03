# Export CSE packed backups through one shared scan service

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

After this change, Dumpling can export selected tables from one Cloud Storage Engine (CSE) packed backup without connecting to TiDB. Dumpling starts exactly one `cse-ctl dumper` for the whole export. The helper loads the packed manifest once, listens on a private Unix socket, and serves concurrent half-open KV range scans over HTTP/2. Dumpling passes its `--threads` value as `cse-ctl --scan-concurrency` so one user setting bounds both CSV writers and packed shard scans.

Dumpling owns TiDB semantics. It scans TiDB metadata ranges through the same raw KV command, decodes full `model.DBInfo` and `model.TableInfo` values in Go, derives physical table record ranges, and decodes row values with TiDB codecs. Existing Dumpling CSV flags, filters, file naming, splitting, compression, output storage, and writer metrics remain observable output behavior.

Each `POST /scan` carries hexadecimal inner range keys as JSON and returns the established binary KV frames. Stream EOF is not sufficient for success: Dumpling must require the HTTP trailer `x-cse-scan-status: complete`, surface the percent-encoded error trailer on failure, and reject a missing completion trailer. The supplied MinIO fixture proves the result. Exporting `test.warehouse` while the configured TiDB port is unusable must produce 300 data rows plus a header and the stable schema/data hashes recorded below.

## Progress

- [x] (2026-07-16) Re-read repository policy, `PLANS.md`, the Dumpling writer interfaces, TiDB metadata encoding, and CSE packed-reader invariants.
- [x] (2026-07-16) Replaced the CSE long-lived opcode protocol with `--metadata-url`, `--start-key-hex`, and `--end-key-hex`, raw frames on stdout, and clean EOF termination.
- [x] (2026-07-16) Replaced CSE schema/table APIs with `PackedBackupReader::scan_range`, retaining keyspace-prefix handling, shard coverage checks, `backup_ts` visibility, checksums, and per-shard file lifetime.
- [x] (2026-07-16) Replaced the Dumpling process pool with one-shot range processes and moved database/table metadata parsing into TiDB.
- [x] (2026-07-16) Updated focused tests so real TiDB DDL/DML produces the metadata and row KVs consumed by the packed adapters.
- [x] (2026-07-16) Passed CSE target tests, TiDB WIP target tests, and the real-fixture export with baseline hashes.
- [x] (2026-07-16) Passed final CSE format/clippy gates, TiDB Ready validation, final fixture export, and the diff/test-quality audit.
- [x] (2026-07-17) Added opt-in legacy encryption: Dumpling propagates `--cse-legacy-encryption`, and CSE resolves the existing `CSE_MASTER_KEY_*` KMS configuration for legacy shard properties.
- [x] (2026-07-18) Added SST range-hint pruning and statistics so a range scan reports candidate, retained, and conservatively retained files.
- [x] (2026-07-18) Removed the draft slow logging, periodic sampling, procfs collection, JSON telemetry, key/range logging, and generic output-storage wrapper.
- [x] (2026-07-18) Added the first packed-only timing implementation for diagnosing slow exports.
- [x] (2026-07-18) Passed focused CSE tests and failpoint-safe Dumpling WIP tests after the scope rewrite.
- [x] (2026-07-18) Passed CSE format, clippy, and release build; passed TiDB Bazel preparation, optimized build, and Ready lint.
- [x] (2026-07-18) Exported the real MinIO fixture with the release CSE binary, verified stable hashes and row shape, and audited the packed-only logs for volume and sensitive content.
- [x] (2026-09-01) Read the current CSE HTTP/2 Unix-socket protocol, lifecycle, trailer contract, concurrency argument, and integration test.
- [x] (2026-09-01) Replaced all one-shot stdout process code with one shared `cse-ctl` process and HTTP/2 range scans.
- [x] (2026-09-01) Extended the existing packed protocol test to cover Unix-socket HTTP/2, new command arguments, completion/failure trailers, missing trailers, and the combined metrics gatherer.
- [x] (2026-09-01) Passed WIP tests, the race test, Bazel preparation, strict-deps/nogo build, Ready `make lint`, and the final diff audit.
- [x] (2026-09-01) Removed the custom packed timing state and CSE performance-log forwarding. Added Prometheus timing for the Dumpling-owned CSE startup, row decode, and whole-export phases, while reusing existing Dumpling row, writer, and error metrics.
- [x] (2026-09-01) Kept scan, shard, KV/byte, SST, and object-I/O metrics exclusively in CSE to avoid duplicating metric families already exported through the combined `/metrics` handler.
- [x] (2026-09-01) Regenerated Bazel metadata and passed the focused failpoint-safe tests, protocol race test, Bazel build with nogo, Ready lint, and final static audit.
- [x] (2026-09-02) Built current Dumpling and release `cse-ctl`, authenticated to the local-k8s MinIO fixture, exported `test.warehouse`, and reproduced the 301-line CSV plus stable schema and data hashes.

## Surprises & Discoveries

- Observation: The bucket named in the original investigation had a typo. The live fixture and CSE playground use `juncen-native-br`.
  Evidence: The working metadata URL is `http://minioadmin:minioadmin@localhost:9000/juncen-native-br/backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta`.

- Observation: CSE's `schema::TableInfo` models only fields CSE consumes, so using it to select and reserialize TiDB metadata can lose fields required by `SHOW CREATE TABLE`.
  Evidence: `components/schema/src/schema.rs` in the CSE workspace has a smaller field set than TiDB's `pkg/meta/model.TableInfo`.

- Observation: TiDB metadata can be recovered with ordinary raw range scans. Hash entries share a prefix made from `m`, `codec.EncodeBytes(hashKey)`, and the `structure.HashData` flag; the remaining key suffix is one encoded hash field.
  Evidence: TiDB `pkg/structure/type.go` implements `hashDataKeyPrefix` and `decodeHashDataKey` with that layout.

- Observation: A clean child-process EOF is sufficient to terminate a one-shot scan. An extra zero-length record would add a second end-of-stream mechanism and reserve an otherwise representable frame.
  Evidence: `readPackedRow` accepts EOF only before a new key-length field and reports partial headers or bodies as corruption.

- Observation: The one-shot implementation preserves the prior fixture output byte for byte.
  Evidence: The WIP run produced CSV SHA256 `ba114d3290558252db863c0ee51177721da09a2296d96eaf2cd2e91abb5f7f79` and schema SHA256 `04c67a09bed9d3b993cdf48605a023953b2d57d1fe22ab1f7461d32e9ff42e8c`.

- Observation: A child range duration alone cannot identify a storage bottleneck because stdout backpressure can extend the child lifetime.
  Evidence: CSE reports object/snapshot/iteration and stdout time separately. TiDB reports pipe-read and packed row-decode time, while the existing Dumpling writer metrics continue to cover generic destination writes.

- Observation: The current CSE service deliberately separates transport EOF from scan success.
  Evidence: `cmd/cse-ctl/src/dumper.rs` sends `x-cse-scan-status: complete` only after `PackedScan::next_batch` finishes; scan errors send `failed` plus `x-cse-scan-error`, and a disconnected stream can have no trailer.

- Observation: CSE scan metrics are process-wide and remain meaningful only when the same process serves all ranges.
  Evidence: `GET /metrics` reads the process Prometheus registry, and the shared `PackedScanPool` owns the aggregate concurrency limit across requests.

- Observation: CSE already exposes packed-reader scan, shard, emitted KV/byte, SST, object-read, snapshot, and iteration measurements.
  Evidence: `components/native_br/src/metrics.rs` defines the `native_br_packed_reader_*` families. Dumpling therefore needs only timings for work it owns outside CSE.

- Observation: The current CSE branch had one API-signature drift that prevented a fresh release `cse-ctl` build.
  Evidence: `MasterKeyConfig::decrypt` requires an `is_legacy` argument, while the dumper's legacy-only master-key loader still called it without one. Passing `true` matches the documented process-level legacy configuration semantics.

## Decision Log

- Decision: Keep packed file lookup, decompression, checksum verification, MVCC merging, and shard coverage in CSE.
  Rationale: CSE already owns those storage formats and `SnapAccess`; reimplementing them in Go would duplicate correctness-sensitive code.
  Date/Author: 2026-07-16, Codex.

- Decision: Make `cse-ctl dumper` a one-shot inner-key range scanner.
  Rationale: One URL plus one inclusive-start/exclusive-end range directly expresses the storage operation and removes bidirectional process state.
  Date/Author: 2026-07-16, Codex.

- Decision: Encode range arguments as hexadecimal and stream repeated `u32_le key_len`, `u32_le value_len`, key, value frames until clean EOF.
  Rationale: CLI arguments remain shell-safe, row bytes have no text expansion, and the process exit status and stderr provide the error channel.
  Date/Author: 2026-07-16, Codex.

- Decision: Range arguments and emitted keys omit the API V2 keyspace prefix.
  Rationale: Dumpling works with TiDB logical keys, while the packed manifest is authoritative for the keyspace ID. CSE adds and removes the prefix at its storage boundary.
  Date/Author: 2026-07-16, Codex.

- Decision: Parse database and table metadata in TiDB by scanning the `DBs` hash and each database's `DB:<id>` hash.
  Rationale: TiDB's current `model` and metadata codecs preserve all schema fields and define public-object and table-field semantics.
  Date/Author: 2026-07-16, Codex.

- Decision: Start one process per physical table record range and advance partition ranges sequentially inside one `TableDataIR`.
  Rationale: Existing Dumpling writers bound simultaneous table tasks by `--threads`; this also bounds child processes without a persistent pool.
  Date/Author: 2026-07-16, Codex.

- Decision: Reuse existing `TaskTableData`, `Writer.WriteTableData`, and `WriteInsertInCsv` behavior.
  Rationale: These source-neutral interfaces already own headers, quoting, nulls, dialects, splitting, compression, naming, and metrics.
  Date/Author: 2026-07-16, Codex.

- Decision: Make legacy decryption explicit and reuse the existing `CSE_MASTER_KEY_*` environment contract.
  Rationale: Opt-in behavior preserves unencrypted and CMEK reads, while keeping plaintext master-key material out of process arguments.
  Date/Author: 2026-07-17, Codex.

- Decision: Keep Dumpling-owned packed timings in the existing Dumpling metrics registry.
  Rationale: A single `dumpling_packed_phase_duration_seconds{phase,result}` histogram covers CSE startup, TiDB row decoding, and the complete packed export without adding state to generic writer or object-storage code.
  Date/Author: 2026-09-01, Codex.

- Decision: Keep SST selection statistics inside the packed reader.
  Rationale: Candidate counts are indexed once from the packed manifest and compared with the range-filtered change set, avoiding an extra unfiltered clone or any generic kvengine hook.
  Date/Author: 2026-07-18, Codex.

- Decision: Treat CSE stderr only as bounded process diagnostics.
  Rationale: Prometheus is the performance interface. Dumpling continuously drains stderr, retains at most 64 KiB for exit errors, and does not parse or forward a second telemetry format.
  Date/Author: 2026-09-01, Codex.

- Decision: Replace the one-process-per-range design rather than retain it as a compatibility path.
  Rationale: The current CSE command no longer accepts range flags or writes rows to stdout. One shared process also keeps the manifest, encryption-key cache, scan pool, and Prometheus state alive across requests.
  Date/Author: 2026-09-01, Codex.

- Decision: Use Go's `net/http`, `net.Dialer`, `encoding/json`, and temporary-directory lifecycle, with `golang.org/x/net/http2.Transport` only for the HTTP/2 wire implementation.
  Rationale: The standard library owns request, response, Unix dialing, JSON, URL decoding, process, and filesystem behavior; the one non-standard package supplies HTTP/2 over a custom Unix connection.
  Date/Author: 2026-09-01, Codex.

- Decision: Include CSE Prometheus metric families in Dumpling `/metrics` only while the packed process owner is live.
  Rationale: A dynamic `prometheus.Gatherer` parses CSE exposition and `prometheus.Gatherers` merges it with Dumpling's default gatherer, so `promhttp.HandlerFor` retains one consistency-checked encoding path without manually joining HTTP responses.
  Date/Author: 2026-09-01, Codex.

- Decision: Do not duplicate CSE packed-reader metrics in Dumpling.
  Rationale: CSE owns scans, shards, emitted KVs and bytes, SSTs, object reads, snapshots, and iteration. Dumpling reuses its existing finished-row, writer-duration, and error metrics and adds only phase timings for work outside that boundary.
  Date/Author: 2026-09-01, Codex.

## Outcomes & Retrospective

Dumpling now matches CSE's shared service protocol while retaining its TiDB-owned schema, physical-range, row-decoding, and CSV-writing semantics. Exactly one process owns manifest initialization, encryption caches, scan concurrency, metrics, and its private socket. All scan responses require an explicit success trailer, and the process exporter is reachable through Dumpling during the export.

The product-code diff stays small because the old per-range process lifecycle, custom timing state, and stderr performance parser were removed instead of retained as compatibility code. Dumpling and CSE expose one Prometheus response through the standard `promhttp` path, and the two sides measure disjoint ownership boundaries. Focused and race tests pass, the generated Bazel dependency graph compiles under nogo, and the Ready lint gate passes. A real local-k8s MinIO export also reproduces the established row count and hashes.

## Context and Orientation

There are two workspaces. TiDB is `/DATA/disk3/juncen/developer/tidb_worktrees/exp-export-packed`. CSE is `/DATA/disk3/juncen/developer/tikv-worktree/exp-export-packed`.

In CSE, `components/native_br/src/packed_reader.rs` adapts a decoded `kvenginepb::PackedBackup` to `kvengine::SnapAccess`. The manifest supplies the API V2 keyspace ID, fixed `backup_ts`, shard snapshots, and `(file_id, file_type)` object references. A half-open range `[start, end)` includes `start` and excludes `end`. An inner key is the TiDB logical key without the API V2 keyspace prefix. `cmd/cse-ctl/src/dumper.rs` loads the manifest once, binds a Unix socket, and serves HTTP/2 `POST /scan` requests until it receives SIGINT or SIGTERM.

In TiDB, `dumpling/export/packed_protocol.go` owns the single child process, HTTP/2 client, scan response validation, and raw framing. `dumpling/export/packed.go` implements metadata scanning, physical table ranges, row decoding, `TableMeta`, `TableDataIR`, and `SQLRowIter`. `dumpling/export/dump.go` selects packed mode without opening SQL, PD, consistency, or GC connections. `dumpling/export/writer.go` and `dumpling/export/writer_util.go` consume the same interfaces for SQL and packed sources.

TiDB metadata is stored through `pkg/structure`. A hash data prefix is `m + codec.EncodeBytes(hashKey) + codec.EncodeUint(uint64(structure.HashData))`. The database hash key is `DBs`. Each database's table hash key is `meta.DBkey(database.ID)`. A scan uses `[prefix, prefix.PrefixNext())`; `codec.DecodeBytes` decodes the field suffix. `meta.IsDBkey` and `meta.IsTableKey` select database and table fields. JSON values decode directly into `model.DBInfo` and `model.TableInfo`, and only `model.StatePublic` objects are exported.

Physical table data uses `tablecodec.GenTableRecordPrefix(tableID)` through `PrefixNext()`. Partition definitions supply physical IDs; a nonpartitioned table uses its table ID.

## Plan of Work

### Milestone 1: Narrow the CSE boundary

Remove schema JSON types, table ID encoding, stdin reads, magic bytes, opcodes, and stream terminators from `cmd/cse-ctl/src/dumper.rs`. Add required hexadecimal range arguments, reject empty or unordered ranges, scan once, and use process EOF as stream completion.

In `components/native_br/src/packed_reader.rs`, expose `scan_range(start_inner, end_inner, emit)`. Add the manifest keyspace prefix internally, scan intersecting shards at `backup_ts`, strip the prefix before emission, fail on coverage gaps, and scope each `SnapAccess` so its files are dropped before loading the next shard. Remove schema and table-key parsing dependencies. Target tests must prove malformed manifests fail and only one shard file set stays live.

### Milestone 2: Move semantics into Dumpling

Replace `cseDumperPool` with `cseDumperScan` in `dumpling/export/packed_protocol.go`. Start the exact one-shot CLI, read frames until clean EOF, wait for exit status, retain bounded stderr, reject partial framing, and kill/wait when iteration closes early.

In `dumpling/export/packed.go`, implement a range-scanner callback, TiDB hash metadata decoding, public schema selection, and physical record ranges. Each packed row iterator starts the next range process only after the prior process exits. Remove `packedPool` and `openPackedBackup` from `dumpling/export/dump.go`.

### Milestone 3: Verify behavior and repository gates

Extend existing broad tests. Use a real TiDB mock store transaction as the test range scanner, so production metadata range and JSON logic consumes keys written by TiDB business logic. Run CSE tests, format, clippy, TiDB failpoint-aware target tests, `make lint`, and the real MinIO export. Compare the exact row count and hashes.

### Milestone 4: Make slow exports diagnosable

Expose performance data as Prometheus metrics. CSE owns packed-reader scan, shard, KV/byte, SST, snapshot, iteration, and object-I/O measurements. Dumpling owns row decoding, destination writing, completed rows, errors, and total export lifecycle. Do not expose a second telemetry format through stderr; retain only bounded stderr diagnostics for process failures.

### Milestone 5: Adopt the shared HTTP/2 service protocol

In `dumpling/export/packed_protocol.go`, start one `cse-ctl dumper` with a private Unix socket and `--scan-concurrency <threads>`. Use one concurrency-safe HTTP/2 transport for all scans, post range JSON to `/scan`, continue decoding the existing frames from each response body, and require the completion trailer. Cancel and wait for the process exactly once when the packed export returns, and remove the private temporary directory through the same owner.

In `dumpling/export/packed.go`, construct that process owner once at the beginning of `Dumper.dumpPacked`, use it for schema and table ranges, and pass it into every `packedTableData`. Remove executable, metadata URL, legacy-encryption, and per-range child-process state from table iterators. Preserve the existing Dumpling writer concurrency and row decoding behavior.

### Milestone 6: Replace custom packed timing with Prometheus

Delete the packed timing and stderr performance-parser files. Register `dumpling_packed_phase_duration_seconds{phase,result}` with the existing Dumpling registry. Record `cse_start` around process readiness, `decode` around TiDB row decoding, and `export` around the complete packed export. Reuse existing Dumpling metrics for completed rows, destination writes, and terminal errors.

Continuously drain CSE stderr with standard-library readers, retaining at most 64 KiB as process-exit diagnostics. Leave packed-reader scan, shard, KV/byte, SST, snapshot, iteration, and object-I/O metrics in CSE, where they are already exported and automatically included in Dumpling `/metrics` by the combined gatherer.

## Concrete Steps

From `/DATA/disk3/juncen/developer/tidb_worktrees/exp-export-packed`, use the failpoint-safe test runner because `dumpling/export` contains failpoint instrumentation. The protocol test starts a real HTTP/2 server on a Unix socket, verifies the range JSON and binary frames, and covers complete, failed, and missing trailers plus the combined metrics gatherer.

    make bazel_prepare
    ./tools/check/failpoint-go-test.sh dumpling/export -run '^(TestMetricsRegistration|TestPackedProtocolRows|TestPackedRowsUseTiDBStorageEncoding|TestConfigValidation)$' -count=1
    ./tools/check/failpoint-go-test.sh dumpling/export -run '^TestPackedProtocolRows$' -count=1 -race
    bazel build --remote_cache=https://cache.hawkingrei.com/bazelcache --noremote_upload_local_results //dumpling/export:export
    make lint

For a real export, configure the same `DFS_*` environment variables accepted by CSE, then use the relative metadata path rather than an object-store URL:

    dumpling --packed-backup '<bucket-or-container>/<meta-object-key>' --cse-ctl-path '<path-to-cse-ctl>' --threads 8 --filetype csv --output '<output-dir>'

While that command is running, `http://<status-addr>/metrics` must include the CSE packed-reader Prometheus metric families alongside Dumpling metrics. Before startup and after shutdown it contains only Dumpling metrics.

## Validation and Acceptance

Acceptance requires all of the following:

One Dumpling packed export starts exactly one `cse-ctl dumper`. Its argv contains the relative metadata path, a private Unix socket, `--scan-concurrency` equal to Dumpling `--threads`, and optional `--legacy-encryption`. No range keys are process arguments and stdout is not a data channel.

Every metadata, table, and partition scan sends `POST /scan` over the shared HTTP/2 transport. The JSON fields are `start_key_hex` and `end_key_hex`. The body contains repeated `(u32_le key_len, u32_le value_len, key, value)` frames. Clean body EOF is accepted only with `x-cse-scan-status: complete`; `failed` decodes `x-cse-scan-error`, and a missing or unknown status fails the export.

CSE adds the manifest keyspace prefix before storage access, scans `WRITE_CF` at the packed backup timestamp, and strips the prefix from emitted keys. Existing checksums, decompression, blob/file access, shard coverage, scan concurrency, and snapshot lifetimes remain delegated to CSE. Dumpling continues to own TiDB metadata and row decoding.

The private process and socket directory have one owner. Returning from `dumpPacked`, including cancellation and writer errors, closes HTTP connections, cancels and waits for the process, and removes the temporary directory. CSE metrics are gathered only while that owner is live.

`dumpling_packed_phase_duration_seconds` reports only `cse_start`, `decode`, and `export`, each labeled with `success` or `error`. Existing Dumpling metrics continue to report completed rows, writer duration, and errors. CSE alone reports packed-reader scan, shard, KV/byte, SST, snapshot, iteration, and object-I/O measurements.

## Idempotence and Recovery

All tests, generators, formatters, and lint commands are safe to rerun. Each execution uses `t.TempDir` or `os.MkdirTemp`; cleanup owns only its exact private path.

The failpoint test script always disables failpoints during cleanup. If a manual test is interrupted, run `make failpoint-disable` before continuing. `make bazel_prepare` is repeatable and must remain the source of Bazel metadata changes.

## Artifacts and Notes

The pre-fix regression run failed to compile because the test required the new socket/concurrency argv and HTTP scan types while production still exposed the old per-range process API. After implementation, the focused test and its race variant pass. The protocol test uses a real `http2.Server` over a Unix listener and observes the completion trailer after consuming the body. The metrics test verifies an emitted packed phase histogram sample. The Bazel target also builds with strict dependencies and nogo after Gazelle adds `@org_golang_x_net//http2`, and `make lint` passes.

The 2026-09-02 local-k8s MinIO export produced `test.warehouse.000000000.csv` with 301 lines, including its header. Its SHA256 is `ba114d3290558252db863c0ee51177721da09a2296d96eaf2cd2e91abb5f7f79`; `test.warehouse-schema.sql` has SHA256 `04c67a09bed9d3b993cdf48605a023953b2d57d1fe22ab1f7461d32e9ff42e8c`. These match the prior baseline exactly.

The CSE release build and formatting check pass after adapting the legacy decrypt call. The repository-wide CSE `make clippy` remains blocked in unrelated existing test code because `tests/cloud_engine/native_backup/dumper.rs` initializes the removed `PackEnv.offline` field. Stricter package-only clippy also reports pre-existing warnings elsewhere in `cse-ctl` and its dependencies, so no clean clippy claim is made for the CSE worktree.

## Interfaces and Dependencies

The process command is:

    cse-ctl dumper \
      --metadata-url <bucket-or-container>/<meta-object-key> \
      --unix-socket <private-socket-path> \
      --scan-concurrency <dumpling-threads> \
      [--legacy-encryption]

DFS backend, bucket/container, prefix, endpoint, credentials, and region come from CSE's `DFS_*` environment contract. Legacy mode continues to use `CSE_MASTER_KEY_*`. Dumpling inherits the parent environment rather than parsing or copying those settings.

In TiDB, `cseDumper` owns the command, temporary directory, HTTP/2 transport, stderr diagnostics, wait state, and idempotent close. `cseDumper.scan` creates one `cseDumperScan` per HTTP response. `packedRangeScanner` adapts it to metadata loading, and `packedTableData` shares the same owner across all writers. These types remain behind the existing `TableMeta`, `TableDataIR`, and `SQLRowIter` interfaces.

Revision note: Created on 2026-07-16 for the initial packed CSV implementation. Updated on 2026-09-01 to replace the retired one-shot stdout protocol with the shared HTTP/2 Unix-socket service, expose CSE metrics through Dumpling, replace custom packed timing with Prometheus, and refresh validation instructions. Updated on 2026-09-02 with the local-k8s MinIO export evidence.

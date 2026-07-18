# Export CSE packed backups through one-shot raw KV scans

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

After this change, Dumpling can export selected tables from one Cloud Storage Engine (CSE) packed backup without connecting to TiDB. The CSE helper has a deliberately narrow interface: one `cse-ctl dumper` invocation accepts the packed-backup metadata URL and one half-open KV range, writes visible raw key/value pairs to stdout, and exits. It has no stdin protocol, opcodes, schema response, table IDs, or TiDB schema parsing.

Dumpling owns TiDB semantics. It scans TiDB metadata ranges through the same raw KV command, decodes full `model.DBInfo` and `model.TableInfo` values in Go, derives physical table record ranges, and decodes row values with TiDB codecs. Existing Dumpling CSV flags, filters, file naming, splitting, compression, output storage, and writer metrics remain observable output behavior.

The supplied MinIO fixture proves the result. Exporting `test.warehouse` while the configured TiDB port is unusable must produce 300 data rows plus a header and the stable schema/data hashes recorded below.

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
- [x] (2026-07-18) Added bounded observability for packed-export stages, range processes, shard/object reads, packed row decoding, and packed output-storage calls. Normal per-range and per-shard records stay at debug level; only slow operations and failures are promoted.
- [x] (2026-07-18) Removed the draft packed instrumentation from the generic Dumpling writer files. Packed output timing is installed only by `dumpPacked` through a storage wrapper defined in `packed_observability.go`.
- [x] (2026-07-18) Moved SST range-hint statistics out of generic kvengine metadata code. The packed reader now derives them locally from the existing unfiltered and filtered change sets, without adding generic kvengine observation hooks, counters, or logs.
- [x] (2026-07-18) Re-ran the focused CSE and Dumpling tests, CSE format, and CSE clippy after the observability changes.
- [x] (2026-07-18) Rebuilt CSE with the release profile and Dumpling with the optimized Go build, exported the MinIO fixture, audited log volume and sensitive fields, and rechecked the stable output hashes.
- [x] (2026-07-18) Passed the TiDB Ready lint gate and completed the final scope, diff, and test-quality audit.
- [x] (2026-07-18) Moved most performance-only implementation out of packed export, protocol, CSE dumper, and packed-reader main-path files. Normal structs keep at most one compact observation handle rather than collections of counters and timers.
- [x] (2026-07-18) Replaced the oversized terminal and per-range log records with bounded, topic-specific records, then repeated Ready validation and the release-profile fixture export.

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
  Evidence: CSE writes framed rows through a bounded stdout pipe while Dumpling decodes rows, formats CSV, and writes the destination concurrently. The observability breakdown therefore records CSE object/snapshot/iteration/stdout time, packed row-decode and table-pipeline time, and packed output-storage create/write/close time.

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

- Decision: Keep routine range, shard, and table completion records at debug level; emit info only for packed-export progress, terminal summaries, and operations exceeding fixed slow thresholds, and warn on failures.
  Rationale: One process is launched for every metadata or physical-table range, so unconditional info logs at each inner boundary would scale with schema and partition count. Slow and failure records retain the detailed fields needed for diagnosis without flooding normal exports.
  Date/Author: 2026-07-18, Codex.

- Decision: Keep Dumpling-side instrumentation inside the packed-export implementation.
  Rationale: Packed row decoding is timed by `packedRowIter`, and `dumpPacked` supplies a packed-only storage wrapper to existing writers. Generic writer source files retain no packed fields, phases, branches, or callbacks.
  Date/Author: 2026-07-18, Codex.

- Decision: Keep SST selection statistics inside the packed reader.
  Rationale: Comparing the existing full and range-filtered change sets provides the packed-export diagnostics without adding statistics, hooks, or logs to generic kvengine metadata code. The separate range-hint API remains the functional pruning boundary requested for packed scans.
  Date/Author: 2026-07-18, Codex.

- Decision: Isolate performance observation from the normal packed-export control and data path.
  Rationale: The scope is limited to `dumpPacked`, its CSE dumper protocol, and the packed reader. Generic Dumpling writers, object-storage implementations, kvengine metadata, snapshots, and DFS paths receive no observability hooks or logs. Packed main-path files expose only small observer handles; counter sets, timing aggregation, machine telemetry, slow-operation sampling, and detailed log construction belong in dedicated observability files. Terminal logs are split by topic so no ordinary log record carries the full metric set.
  Date/Author: 2026-07-18, Codex.

## Outcomes & Retrospective

The implementation now matches the one-shot raw KV design in both workspaces. CSE no longer imports TiDB schema types or table-key codecs from its packed reader. Dumpling starts no persistent CSE processes, reconstructs schema from raw metadata, and launches table range scans only while writers consume rows.

Focused tests, repository lint gates, and the real fixture pass. The test changes were kept inside two broad existing tests: the framing table covers clean EOF and several corrupt-stream boundaries, while the storage-encoding feature test uses real TiDB DDL/DML to produce database metadata, table metadata, integer/common/partition handles, defaults, nulls, binary values, decimal, time, enum, and set values. They are not tautological fixture round trips or isolated happy-path assertions. The CSE range-validation table is retained because ordered non-empty ranges are part of the new public CLI contract; the multi-shard reader test derives range coverage and peak live-file behavior in addition to checking emitted fixtures.

## Context and Orientation

There are two workspaces. TiDB is `/root/workspace/tidb/exp-export-packed`. CSE is `/root/workspace/cloud-storage-engine/exp-export-packed-csv`.

In CSE, `components/native_br/src/packed_reader.rs` adapts a decoded `kvenginepb::PackedBackup` to `kvengine::SnapAccess`. The manifest supplies the API V2 keyspace ID, fixed `backup_ts`, shard snapshots, and `(file_id, file_type)` object references. A half-open range `[start, end)` includes `start` and excludes `end`. An inner key is the TiDB logical key without the API V2 keyspace prefix. `cmd/cse-ctl/src/dumper.rs` parses the object-store URL and CLI range, loads the manifest, invokes the reader once, writes raw frames, flushes, and exits.

In TiDB, `dumpling/export/packed_protocol.go` owns one child process and validates the raw framing. `dumpling/export/packed.go` implements metadata scanning, physical table ranges, row decoding, `TableMeta`, `TableDataIR`, and `SQLRowIter`. `dumpling/export/dump.go` selects packed mode without opening SQL, PD, consistency, or GC connections. `dumpling/export/writer.go` and `dumpling/export/writer_util.go` consume the same interfaces for SQL and packed sources.

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

### Milestone 4: Make slow exports diagnosable without noisy logs

In Dumpling, report the overall stage and bounded periodic progress, and attach process startup, first-row, row/byte, and duration fields to range completion. Aggregate packed row-decode and table-pipeline time inside packed types, and observe destination create/write/close calls through a storage wrapper installed only by `dumpPacked`. Emit routine per-range and per-table records only at debug level, with one info record when an operation crosses its slow threshold. Do not add packed observability fields or branches to the generic writer implementation.

In CSE, emit one terminal range summary containing manifest, encryption-mode, SST-pruning, object-read, snapshot, iteration, stdout, and flush statistics. Keep successful shard detail at debug level, cap slow-object info records to three per process, and preserve elapsed time on failing stages. Run a release-profile fixture export and inspect the actual log distribution before accepting the thresholds and fields.

## Concrete Steps

From `/root/workspace/cloud-storage-engine/exp-export-packed-csv`, run:

    cargo test -p native_br packed_reader::tests -- --nocapture
    cargo test -p cse-ctl dumper::tests -- --nocapture
    cargo test -p kvengine test_read_iterator_all_versions -- --nocapture
    make release
    target/release/cse-ctl dumper --help
    make format
    make clippy

From `/root/workspace/tidb/exp-export-packed`, inspect the Bazel gate. Run `make bazel_prepare` only when the current local diff matches a trigger in `AGENTS.md`; the observability-only diff changes existing Go files and existing test content, so it does not add a new trigger. The package contains failpoints, so use the cleanup-safe test script:

    ./tools/check/failpoint-go-test.sh dumpling/export -run '^(TestPackedProtocolRows|TestPackedRowsUseTiDBStorageEncoding|TestConfigValidation|TestDumpExit|TestDumpTableMeta|TestPrepareDumpingDatabases)$' -count=1
    ./tools/check/failpoint-go-test.sh dumpling/export -run '^(TestWriteDatabaseMeta|TestWriteTableMeta|TestWriteTableDataWithFileSize|TestWriteInsertInCsv|TestWriteInsertInCsvWithDialect)$' -count=1
    go build -trimpath -ldflags '-s -w' -o /tmp/dumpling-packed-observability ./dumpling/cmd/dumpling
    make lint

Run the real fixture with an unusable TiDB port:

    rm -rf /tmp/dumpling-packed-warehouse-range
    /tmp/dumpling-packed-observability --host 127.0.0.1 --port 1 --packed-backup 'http://minioadmin:minioadmin@localhost:9000/juncen-native-br/backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta' --cse-ctl-path /root/workspace/cloud-storage-engine/exp-export-packed-csv/target/release/cse-ctl --filter 'test.warehouse' --filetype csv --output /tmp/dumpling-packed-warehouse-range --threads 2 --loglevel debug

Inspect the result:

    wc -l /tmp/dumpling-packed-warehouse-range/test.warehouse.000000000.csv
    sha256sum /tmp/dumpling-packed-warehouse-range/test.warehouse.000000000.csv /tmp/dumpling-packed-warehouse-range/test.warehouse-schema.sql

Expected output is 301 CSV lines and the hashes in `Artifacts and Notes`.

## Validation and Acceptance

Acceptance requires all of the following:

`cse-ctl dumper --help` exposes one metadata URL, two range keys, and optional `--legacy-encryption`. The command does not read stdin, emit magic bytes or schema JSON, accept table IDs, or wait for a second request. stdout contains only repeated raw KV frames and ends at process EOF. Invalid hex, empty ranges, reversed ranges, partial output framing, uncovered backup ranges, missing objects, an invalid legacy master-key configuration, and a nonzero child exit produce errors.

CSE adds the manifest keyspace prefix before storage access, scans `WRITE_CF` with `Some(backup_ts)`, and strips the prefix from emitted keys. Existing checksums, compressed blocks, blob/file access, shard coverage, and snapshot file lifetimes remain delegated to `SnapAccess` and `PackedContentDfs`.

Dumpling loads full public database/table schema through raw metadata ranges, exports partition ranges sequentially, and has no persistent child process pool. Existing SQL-source tests remain green. Packed mode completes with `--host 127.0.0.1 --port 1`, proving no TiDB connection is used.

The fixture output contains `test-schema-create.sql`, `test.warehouse-schema.sql`, and `test.warehouse.000000000.csv`. The CSV has one header and 300 rows, 300 unique IDs, and nine fields per record. Its schema and CSV hashes match below. The release-profile run must also show one start, metadata, scheduling, and terminal export record; routine range/shard detail must remain debug-only; and no full metadata URL, access key, secret key, session token, or master-key material may appear in the logs.

## Idempotence and Recovery

All source reads and fixture object-store reads are immutable. The target tests, generators, formatters, linter, and export are safe to rerun. Remove only the dedicated `/tmp/dumpling-packed-warehouse-range` directory before repeating the export. Never alter the MinIO fixture.

The failpoint test script always disables failpoints during cleanup. If a manual test is interrupted, run `make failpoint-disable` before continuing. If `make bazel_prepare` generates changes, inspect them and retain only metadata required by current source/dependency changes. Do not hand-edit generated Cargo lock data; let Cargo refresh it after dependency edits.

## Artifacts and Notes

The fixture metadata URL is:

    http://minioadmin:minioadmin@localhost:9000/juncen-native-br/backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta

The WIP end-to-end run completed successfully with `tables=1`, `tasks=3`, and no TiDB connection. Output evidence:

    301 test.warehouse.000000000.csv
    ba114d3290558252db863c0ee51177721da09a2296d96eaf2cd2e91abb5f7f79  test.warehouse.000000000.csv
    04c67a09bed9d3b993cdf48605a023953b2d57d1fe22ab1f7461d32e9ff42e8c  test.warehouse-schema.sql
    data_rows=300 unique_ids=300 bad_field_counts=0

The packed metadata identifies cluster `7628844331459966093`, keyspace ID `2`, keyspace name `beacon`, and 17 shards. The fixture references 922 SSTs and exercises LZ4 blocks and CRC32 checksums.

The 2026-07-17 legacy-encryption extension passed the packed-reader and CLI
unit tests, `make format`, CSE `make clippy`, the failpoint-safe focused
Dumpling tests, and TiDB `make lint`. The unencrypted fixture was exported
again both without the opt-in flag and with the flag plus the test KMS vendor;
both runs retained the same 301 lines and hashes. The packed-reader test builds
an actually encrypted L0 file and reads it through the injected legacy master
key.

The final 2026-07-18 scope-audit run used a CSE binary built by `make release`
and reporting `build_profile=release`, plus an optimized, stripped Dumpling
binary. It completed five one-shot scans with no failed or canceled scans,
retained 10 of 410 SST candidates, read 10 content objects, and reported
matching CSE/protocol row and byte totals. Peak CSE child RSS was 100,614,144
bytes. After splitting detailed records by topic, the debug log contained 61
records: 47 debug, 14 info, and no warning or error records; its longest record
was 764 bytes. Routine range and table completion appeared only at debug level,
no internal machine-protocol line escaped into the parent log, and the
sensitive-value audit found no full metadata URL, URL credentials, AWS
credential/session-token value, or legacy master-key configuration value. The
CSV and schema hashes remained unchanged, with 300 data rows, 300 unique IDs,
and nine fields per row. Generic Dumpling
writer files had zero diff, and the observability follow-up had zero diff in
generic kvengine metadata files. The earlier packed-scan range-hint commit still
contains the functional `meta.rs` API change used for SST pruning.

## Interfaces and Dependencies

In CSE, `native_br::packed_reader::PackedBackupReader` has the public operation:

    pub async fn scan_range<F>(
        &self,
        start_inner: &[u8],
        end_inner: &[u8],
        emit: F,
    ) -> native_br::Result<PackedScanStats>
    where
        F: FnMut(&[u8], &[u8]) -> std::io::Result<()>;

The process command is:

    cse-ctl dumper \
      --metadata-url <packed-backup-url> \
      --start-key-hex <inclusive-inner-key-hex> \
      --end-key-hex <exclusive-inner-key-hex> \
      [--legacy-encryption]

Legacy mode resolves the encrypted process master key through
`CSE_MASTER_KEY_ID`, `CSE_MASTER_KEY_CIPHER_TEXT`, `CSE_MASTER_KEY_VENDOR`,
`CSE_MASTER_KEY_ENDPOINT`, and `AWS_REGION`. Dumpling exposes the matching
`--cse-legacy-encryption` switch and passes it to every range process.

stdout repeats `u32_le key_len`, `u32_le value_len`, `key`, and `value`. A clean EOF ends the stream. stderr plus a nonzero exit reports failure.

In TiDB, `packedRangeScanner` accepts a context, start/end logical keys, and an emit callback. `loadPackedDatabases` uses it for metadata hashes. `packedTableData` stores executable, URL, table schema, and physical ranges. `packedRowIter` owns at most one `cseDumperScan` and advances ranges sequentially. These types remain behind the existing `TableMeta`, `TableDataIR`, and `SQLRowIter` interfaces.

Revision note: Created on 2026-07-16 for the initial packed CSV implementation. Rewritten later that day to record the one-shot raw KV command, TiDB-owned schema parsing, removal of the long-lived protocol, and current validation evidence. Updated on 2026-07-18 with the range-pruning and low-noise observability milestone plus release-profile acceptance steps.

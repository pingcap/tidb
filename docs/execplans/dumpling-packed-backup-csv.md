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

## Outcomes & Retrospective

The implementation now matches the one-shot raw KV design in both workspaces. CSE no longer imports TiDB schema types or table-key codecs from its packed reader. Dumpling starts no persistent CSE processes, reconstructs schema from raw metadata, and launches table range scans only while writers consume rows.

Focused tests, repository lint gates, and the real fixture pass. The test changes were kept inside two broad existing tests: the framing table covers clean EOF and several corrupt-stream boundaries, while the storage-encoding feature test uses real TiDB DDL/DML to produce database metadata, table metadata, integer/common/partition handles, defaults, nulls, binary values, decimal, time, enum, and set values. They are not tautological fixture round trips or isolated happy-path assertions. The CSE range-validation table is retained because ordered non-empty ranges are part of the new public CLI contract; the multi-shard reader test derives range coverage and peak live-file behavior in addition to checking emitted fixtures.

## Context and Orientation

There are two workspaces. TiDB is `/root/workspace/tidb/exp-export-packed`. CSE is `/workspace/cloud-storage-engine/exp-export-packed-csv`.

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

## Concrete Steps

From `/workspace/cloud-storage-engine/exp-export-packed-csv`, run:

    cargo test -p native_br packed_reader::tests -- --nocapture
    cargo test -p cse-ctl dumper::tests -- --nocapture
    cargo test -p kvengine test_read_iterator_all_versions -- --nocapture
    cargo build -p cse-ctl
    target/debug/cse-ctl dumper --help
    make format
    make clippy

From `/root/workspace/tidb/exp-export-packed`, inspect the Bazel gate. `dumpling/export/BUILD.bazel` changes, so `make bazel_prepare` is required. The package contains failpoints, so use the cleanup-safe test script:

    make bazel_prepare
    ./tools/check/failpoint-go-test.sh dumpling/export -run '^(TestPackedProtocolRows|TestPackedRowsUseTiDBStorageEncoding|TestConfigValidation|TestDumpExit|TestDumpTableMeta|TestPrepareDumpingDatabases)$' -count=1
    ./tools/check/failpoint-go-test.sh dumpling/export -run '^(TestWriteDatabaseMeta|TestWriteTableMeta|TestWriteTableDataWithFileSize|TestWriteInsertInCsv|TestWriteInsertInCsvWithDialect)$' -count=1
    make lint

Run the real fixture with an unusable TiDB port:

    rm -rf /tmp/dumpling-packed-warehouse-range
    go run ./dumpling/cmd/dumpling --host 127.0.0.1 --port 1 --packed-backup 'http://minioadmin:minioadmin@localhost:9000/juncen-native-br/backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta' --cse-ctl-path /workspace/cloud-storage-engine/exp-export-packed-csv/target/debug/cse-ctl --filter 'test.warehouse' --filetype csv --output /tmp/dumpling-packed-warehouse-range --threads 2

Inspect the result:

    wc -l /tmp/dumpling-packed-warehouse-range/test.warehouse.000000000.csv
    sha256sum /tmp/dumpling-packed-warehouse-range/test.warehouse.000000000.csv /tmp/dumpling-packed-warehouse-range/test.warehouse-schema.sql

Expected output is 301 CSV lines and the hashes in `Artifacts and Notes`.

## Validation and Acceptance

Acceptance requires all of the following:

`cse-ctl dumper --help` exposes one metadata URL and two range keys. The command does not read stdin, emit magic bytes or schema JSON, accept table IDs, or wait for a second request. stdout contains only repeated raw KV frames and ends at process EOF. Invalid hex, empty ranges, reversed ranges, partial output framing, uncovered backup ranges, missing objects, and a nonzero child exit produce errors.

CSE adds the manifest keyspace prefix before storage access, scans `WRITE_CF` with `Some(backup_ts)`, and strips the prefix from emitted keys. Existing checksums, compressed blocks, blob/file access, shard coverage, and snapshot file lifetimes remain delegated to `SnapAccess` and `PackedContentDfs`.

Dumpling loads full public database/table schema through raw metadata ranges, exports partition ranges sequentially, and has no persistent child process pool. Existing SQL-source tests remain green. Packed mode completes with `--host 127.0.0.1 --port 1`, proving no TiDB connection is used.

The fixture output contains `test-schema-create.sql`, `test.warehouse-schema.sql`, and `test.warehouse.000000000.csv`. The CSV has one header and 300 rows, 300 unique IDs, and nine fields per record. Its schema and CSV hashes match below.

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

## Interfaces and Dependencies

In CSE, `native_br::packed_reader::PackedBackupReader` has the public operation:

    pub async fn scan_range<F>(
        &self,
        start_inner: &[u8],
        end_inner: &[u8],
        emit: F,
    ) -> native_br::Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> std::io::Result<()>;

The process command is:

    cse-ctl dumper \
      --metadata-url <packed-backup-url> \
      --start-key-hex <inclusive-inner-key-hex> \
      --end-key-hex <exclusive-inner-key-hex>

stdout repeats `u32_le key_len`, `u32_le value_len`, `key`, and `value`. A clean EOF ends the stream. stderr plus a nonzero exit reports failure.

In TiDB, `packedRangeScanner` accepts a context, start/end logical keys, and an emit callback. `loadPackedDatabases` uses it for metadata hashes. `packedTableData` stores executable, URL, table schema, and physical ranges. `packedRowIter` owns at most one `cseDumperScan` and advances ranges sequentially. These types remain behind the existing `TableMeta`, `TableDataIR`, and `SQLRowIter` interfaces.

Revision note: Created on 2026-07-16 for the initial packed CSV implementation. Rewritten later that day to record the one-shot raw KV command, TiDB-owned schema parsing, removal of the long-lived protocol, and current validation evidence.

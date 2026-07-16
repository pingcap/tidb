# Export CSE packed backups as CSV with Dumpling

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

After this change, a user can point Dumpling at one Cloud Storage Engine (CSE) packed-backup metadata object and export selected tables as Dumpling CSV files without a running TiDB server. The existing Dumpling CSV flags, table filter, file naming, splitting, compression, object-store output, and writer metrics remain the observable output behavior. A local end-to-end run against the supplied MinIO backup must export `test.warehouse` with its real schema and rows.

A packed backup is a protobuf metadata object containing shard snapshots and references to immutable CSE content files. A shard snapshot contains L0 files and leveled SST files. The reader must merge their key/value versions, apply snapshot visibility at `backup_ts`, load TiDB schema JSON from metadata key ranges, and decode table record keys and row values using TiDB's existing codecs.

## Progress

- [x] (2026-07-16) Re-read repository policy, `PLANS.md`, the Dumpling task/writer/CSV path, and the CSE packed reader, schema loader, protobuf, and SST format entry points.
- [x] (2026-07-16) Confirmed the supplied fixture and its exact MinIO metadata key.
- [x] (2026-07-16) Added `native_br::packed_reader`, which maps packed content references into CSE's existing `SnapAccess` and streams visible row KVs at `backup_ts`.
- [x] (2026-07-16) Added the long-lived `cse-ctl dumper` subprocess protocol: schema JSON is sent once per request and scans stream raw key/value frames without base64 or whole-table buffering.
- [x] (2026-07-16) Added the Dumpling pool, schema/row adapter, packed-mode configuration and dispatch while preserving the SQL source path.
- [x] (2026-07-16) Added focused tests for protocol damage, malformed packed manifests, and TiDB-produced integer/common handles, missing defaults, null, binary, decimal, time, enum, and set values.
- [x] (2026-07-16) Exported `test.warehouse` from the supplied MinIO fixture with TiDB pointed at the unusable port `127.0.0.1:1`; verified 300 data rows, 300 unique IDs, nine fields per row, and stable schema/CSV hashes.
- [x] (2026-07-16) Completed the repository-required Ready validation, CSE format/clippy gates, and final diff/test-quality review.

## Surprises & Discoveries

- Observation: The bucket named in the original request has a typo. The live fixture and the CSE playground use `juncen-native-br`, while the request said `juncen-natvie-br`.
  Evidence: `components/packed_to_iceberg/src/playgrounds.rs` defaults `NATIVE_BR_REAL_PACKED_META_BUCKET` to `juncen-native-br`.

- Observation: The real fixture has 17 shards and 922 referenced SSTs, with LZ4 blocks and CRC32 checksums. It has no blob, columnar, transaction-chunk, standalone-schema, or encrypted-file references.
  Evidence: The metadata object `backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta` was inspected during the initial investigation.

- Observation: Dumpling's CSV writer already accepts source-neutral `TableMeta` and `TableDataIR` values. Only `TableDataIR.Start` and SQL retry handling currently assume that a database connection exists.
  Evidence: `dumpling/export/task.go`, `dumpling/export/writer.go`, and `dumpling/export/writer_util.go`.

- Observation: CSE's `schema::TableInfo` intentionally models only fields CSE consumes, so serializing that type back to JSON drops TiDB schema fields used by `SHOW CREATE TABLE`.
  Evidence: `components/schema/src/schema.rs` has a smaller field set than TiDB's `pkg/meta/model.TableInfo`.

- Observation: Reconstructing `SnapAccess` for every table request repeatedly downloads or prepares the same shard files and metadata, despite `cse-ctl dumper` being long-lived.
  Evidence: The first implementation called `SnapAccess::from_change_set` inside every `for_each_visible_kv`; the final reader stores one asynchronous `OnceCell<SnapAccess>` per shard and reuses it across requests.

- Observation: The supplied real fixture remains byte-for-byte stable after adding snapshot reuse and precomputed TiDB row decoder metadata.
  Evidence: The final run produced CSV SHA256 `ba114d3290558252db863c0ee51177721da09a2296d96eaf2cd2e91abb5f7f79` and table-schema SHA256 `04c67a09bed9d3b993cdf48605a023953b2d57d1fe22ab1f7461d32e9ff42e8c`, matching the pre-optimization run.

## Decision Log

- Decision: Keep packed format, object IO, MVCC merge, schema loading, and row decoding in a source package that does not import Dumpling.
  Rationale: This makes the Dumpling layer a type adapter and task producer, and lets the storage reader be tested independently.
  Date/Author: 2026-07-16, Codex.

- Decision: Reuse `TaskTableData`, `Writer.WriteTableData`, and `WriteInsertInCsv` unchanged for row serialization behavior.
  Rationale: These are the existing source-neutral interfaces immediately before output naming and CSV formatting.
  Date/Author: 2026-07-16, Codex.

- Decision: Add one `--packed-backup` input URL. The URL names the exact metadata object; its scheme and query options use TiDB object-store conventions. Packed mode requires CSV and does not establish SQL, consistency, GC, or PD connections.
  Rationale: One explicit source selector keeps current MySQL/TiDB behavior unchanged and supplies the bucket, metadata key, endpoint, and credentials through an established URL representation.
  Date/Author: 2026-07-16, Codex.

- Decision: Use the packed `backup_ts` as the fixed read timestamp and fail with contextual errors on unsupported or corrupt packed content.
  Rationale: The packed metadata describes a consistent snapshot at that timestamp. Silent fallback could export mixed or incomplete rows.
  Date/Author: 2026-07-16, Codex.

- Decision: Keep packed-format reading in CSE and communicate with Dumpling through a pool of long-lived `cse-ctl dumper` subprocesses.
  Rationale: CSE already owns and tests the SST, compression, checksum, file-reference, shard, and MVCC implementation. Reusing it avoids a second format implementation in Go while allowing table scans to run concurrently.
  Date/Author: 2026-07-16, Codex.

- Decision: Use a small bidirectional binary protocol. Requests are one-byte opcodes; scan requests add little-endian physical table IDs; responses contain one schema JSON document or streamed `(key length, value length, key, value)` records.
  Rationale: Raw row bytes avoid base64 expansion and per-row JSON parsing, and eight bytes of framing per row do not become the export bottleneck.
  Date/Author: 2026-07-16, Codex.

- Decision: Preserve the original TiDB database/table JSON values while using CSE schema types only to select public objects.
  Rationale: TiDB must decode its complete current `model` and reconstruct schema SQL without losing fields that CSE does not interpret.
  Date/Author: 2026-07-16, Codex.

- Decision: Cache each constructed CSE `SnapAccess` in its long-lived dumper process and precompute TiDB column decode metadata once per row iterator.
  Rationale: Table scans can reuse immutable snapshot state, while raw rows still stream without whole-table buffering. This removes repeated storage preparation and per-row map construction without expanding the subprocess protocol.
  Date/Author: 2026-07-16, Codex.

## Outcomes & Retrospective

The CSE reader, streaming subprocess protocol, Dumpling adapter, and focused regression tests are implemented. Dumpling starts and completes packed mode without a TiDB or PD connection, including when its configured TiDB port is unusable. The supplied `test.warehouse` fixture exports 300 rows plus one header row with the expected nine columns and stable schema/data hashes. Existing Dumpling writer code remains responsible for headers, quoting, nulls, dialects, file names, and size splitting; its focused regression tests pass through the same `TableMeta`, `TableDataIR`, and `SQLRowIter` boundary used here.

The test additions were reviewed against the repository's test-quality rules. The Go table test obtains KVs through real TiDB DDL/DML and checks several deletion-sensitive decoding branches in one feature table. Protocol and malformed-manifest tests exercise corrupt inputs, missing content references, uncovered ranges, and failure behavior. None is an addition-only test, a happy-path single-point test, or a tautological fixture round trip. The reused CSE iterator's existing `test_read_iterator_all_versions` test also passes in sync and async forms, covering a three-level LSM tree, multiple versions, and a latest-version tombstone.

## Context and Orientation

The TiDB workspace is `/root/workspace/tidb/exp-export-packed`. Dumpling's public command is `dumpling/cmd/dumpling/main.go`. Configuration and flags are in `dumpling/export/config.go`. `dumpling/export/dump.go` currently creates SQL connections, discovers database metadata, and sends tasks. `dumpling/export/task.go` defines `TaskTableData`; `dumpling/export/ir.go` defines `TableMeta`, `TableDataIR`, and `SQLRowIter`; `dumpling/export/writer.go` consumes tasks; `dumpling/export/writer_util.go` serializes rows to CSV; and `dumpling/export/sql_type.go` owns the CSV receiver types.

The CSE workspace is `/workspace/cloud-storage-engine/exp-export-packed-csv`. `components/kvenginepb/src/changeset.proto` defines `PackedBackup`, `ChangeSet`, `Snapshot`, and `FileRef`. `components/kvengine/src/read.rs` implements `SnapAccess`; `components/kvengine/src/table/sstable/` implements L0, leveled SST, blocks, checksums, compression, indexes, and iteration. `components/schema/src/load.rs` defines TiDB metadata key ranges and public-object selection. The new `components/native_br/src/packed_reader.rs` adapts a packed manifest to `SnapAccess`, while `cmd/cse-ctl/src/dumper.rs` owns object-store URL parsing and the subprocess protocol.

TiDB's existing `pkg/parser/model` structures can decode the stored database and table JSON. `pkg/tablecodec/tablecodec.go` and `pkg/util/rowcodec` decode row keys, handles, and row values. These existing codecs are authoritative for SQL values and avoid duplicating TiDB row semantics.

The supplied object store is MinIO at `http://localhost:9000`, with access key and secret key `minioadmin`. The bucket is `juncen-native-br`, and the exact metadata key is `backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta`. The packed metadata identifies cluster `7628844331459966093`, keyspace ID `2`, keyspace name `beacon`, and 17 shards. The smallest real validation table is `test.warehouse`.

## Plan of Work

The implementation keeps packed storage ownership in CSE. `PackedBackupReader` maps `(file_id, file_type)` to manifest object keys, builds existing CSE snapshot contexts, checks shard coverage, scans at the fixed backup timestamp, and returns row keys without the API V2 keyspace prefix. Schema scans preserve original JSON values after using CSE schema types to select public databases and tables.

`cse-ctl dumper` loads the immutable manifest once and remains alive for multiple schema/scan requests. Dumpling starts a pool sized by `--threads`; each table task acquires one process, streams rows, and returns it to the pool. Failed processes are replaced. The protocol never encodes row bodies as text and never buffers a complete table.

The Dumpling adapter decodes complete schema JSON into TiDB `model` types, reconstructs schema SQL, applies the existing table filter, expands partition physical IDs, and decodes integer/common handles and row values with `tablecodec`. It fills missing columns from TiDB defaults and exposes rows through existing CSV receiver types. Existing task, writer, naming, compression, header, dialect, and size-splitting code stays unchanged.

Focused tests use TiDB DDL/DML to produce real schema and stored KVs rather than round-tripping a synthetic codec fixture. The remaining work is to run the supplied backup through both binaries, repair any interoperability issue, then run CSE format/clippy and TiDB Ready checks.

## Concrete Steps

Run commands from `/root/workspace/tidb/exp-export-packed`.

Before adding Go source or a new top-level Go test, consult the Bazel prepare gate and then run:

    make bazel_prepare

During implementation, format touched Go files and run the smallest package tests selected by the WIP verification profile. If a touched package uses failpoints, enable them before the package test and disable them afterward according to the repository playbook.

Build CSE's CLI from `/workspace/cloud-storage-engine/exp-export-packed-csv`:

    cargo build -p cse-ctl

An end-to-end invocation from the TiDB workspace uses the equivalent of:

    go run ./dumpling/cmd/dumpling --packed-backup 'http://minioadmin:minioadmin@localhost:9000/juncen-native-br/backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta' --cse-ctl-path /workspace/cloud-storage-engine/exp-export-packed-csv/target/debug/cse-ctl --filter 'test.warehouse' --filetype csv --output /tmp/dumpling-packed-warehouse

The command must terminate successfully without a TiDB server and create the normal Dumpling schema/data files for `test.warehouse` under the output directory.

Before claiming completion, use the Ready verification profile. It includes generated Bazel metadata checks, targeted tests, `make lint` for code changes, the real fixture export, and a final `git diff --check` plus diff review. Record every exact command and result in this document.

## Validation and Acceptance

Acceptance requires all of the following observable behavior:

The existing SQL-source Dumpling tests continue to pass. Packed mode starts without connecting to `--host` or PD. The CLI rejects a malformed metadata URL, non-CSV output, missing content references, corrupt checksums, uncovered shard ranges, and unsupported packed data with actionable errors.

The source package proves table-format behavior across multiple blocks, files, levels, versions, tombstones, range boundaries, compression types, and checksum failures. Schema and row tests prove public-object filtering, partition physical IDs, integer and common handles, absent/default columns, null values, binary values, and representative TiDB scalar encodings. Tests must be deletion-sensitive and must not merely replay a fixture created by the same parser.

Against the supplied MinIO fixture, Dumpling discovers `test.warehouse`, writes its schema and CSV, and exports the same row count and representative values as an independent CSE reader or SQL-derived reference. CSV headers, quoting, null encoding, separators, line terminators, binary dialect, compression, file naming, and size splitting remain governed by existing Dumpling flags.

## Idempotence and Recovery

All reads are immutable object-store reads and all tests use temporary output directories. Re-running `make bazel_prepare`, generators, formatters, tests, or the export is safe. Remove only the dedicated temporary output directory before repeating an end-to-end run. Do not alter the MinIO fixture. If generation produces unrelated changes, inspect and retain only artifacts required by the added source files; never hand-edit generated output.

## Artifacts and Notes

Initial fixture facts: cluster ID `7628844331459966093`, keyspace ID `2`, keyspace name `beacon`, 17 shards, 25 L0 SSTs, 897 leveled SSTs, and 922 SST references in total. The sampled files use LZ4 compression and CRC32 checksums.

Final end-to-end output is under `/tmp/dumpling-packed-warehouse-ready`. Its three files are `test-schema-create.sql`, `test.warehouse-schema.sql`, and `test.warehouse.000000000.csv`. The CSV has 301 lines including its header, 300 unique `w_id` values, and exactly nine fields in every record. Hashes are:

    ba114d3290558252db863c0ee51177721da09a2296d96eaf2cd2e91abb5f7f79  test.warehouse.000000000.csv
    04c67a09bed9d3b993cdf48605a023953b2d57d1fe22ab1f7461d32e9ff42e8c  test.warehouse-schema.sql

Final validation commands, all successful, were:

    # In /workspace/cloud-storage-engine/exp-export-packed-csv
    make format
    cargo test -p native_br packed_reader::tests -- --nocapture
    cargo test -p cse-ctl dumper::tests -- --nocapture
    cargo test -p kvengine test_read_iterator_all_versions -- --nocapture
    cargo build -p cse-ctl
    make clippy

    # In /root/workspace/tidb/exp-export-packed
    make bazel_prepare
    ./tools/check/failpoint-go-test.sh dumpling/export -run '^(TestPackedProtocolRows|TestPackedRowsUseTiDBStorageEncoding|TestConfigValidation|TestDumpExit|TestDumpTableMeta|TestPrepareDumpingDatabases)$' -count=1
    ./tools/check/failpoint-go-test.sh dumpling/export -run '^(TestWriteDatabaseMeta|TestWriteTableMeta|TestWriteTableDataWithFileSize|TestWriteInsertInCsv|TestWriteInsertInCsvWithDialect)$' -count=1
    make lint
    go run ./dumpling/cmd/dumpling --host 127.0.0.1 --port 1 --packed-backup 'http://minioadmin:minioadmin@localhost:9000/juncen-native-br/backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta' --cse-ctl-path /workspace/cloud-storage-engine/exp-export-packed-csv/target/debug/cse-ctl --filter 'test.warehouse' --filetype csv --output /tmp/dumpling-packed-warehouse-ready --threads 2

## Interfaces and Dependencies

The CSE API is `native_br::packed_reader::PackedBackupReader`. It accepts a decoded `PackedBackup` plus a read-only `Dfs`, exposes original schema JSON through `load_schema`, and streams visible encoded rows through `scan_tables`.

The subprocess protocol begins with `CSEDUMP\0` plus version byte `1`. Opcode `0` closes, `1` requests one length-prefixed schema JSON document, and `2` requests a scan with a `u32` count and little-endian `i64` table IDs. Each row response is `u32 key_len`, `u32 value_len`, then raw bytes; zero lengths terminate a scan.

The Dumpling adapter implements the existing `TableMeta`, `TableDataIR`, and `SQLRowIter` interfaces and emits existing `TaskTableData` tasks. It does not expose CSE file, shard, block, or MVCC types to the writer.

Revision note: Created on 2026-07-16 after revalidating the earlier investigation against the current TiDB and CSE worktrees. Updated later that day to record the implemented CSE subprocess architecture, raw-schema preservation, snapshot reuse, final real-fixture evidence, and Ready validation.

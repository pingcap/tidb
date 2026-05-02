# Proposal: TiDB Backup to Open Format Converter

- Author(s):     [@tailinlyu](https://github.com/tailinlyu)
- Last updated:  2026-05-01
- Discussion at: https://github.com/pingcap/tidb/issues/TBD

## Abstract

This proposal describes a tool that reads TiDB BR (Backup & Restore) full-backup SST files directly from object storage and converts them into queryable open-format tables (Apache Iceberg with Parquet data files). The tool uses a Go native library for SST parsing and MVCC reconstruction, exposed to a distributed compute engine (Spark) via JNI, enabling conversion of multi-terabyte backups into analytics-ready tables without requiring a running TiDB cluster or TiSpark.

Combined with TiCDC, the backup converter forms the foundation for a complete real-time TiDB OLAP solution: the converter provides the initial full-table snapshot (bootstrap), and TiCDC provides the continuous incremental stream. Together they deliver queryable, eventually-consistent mirrors of TiDB tables in open formats — enabling the same data to be consumed by Trino, Spark SQL, or any Iceberg-compatible engine without impacting OLTP performance.

## Background

TiDB's BR tool produces full-cluster snapshots stored as SST files in object storage. These backups are primarily used for disaster recovery. However, many organizations also need to query backup data for analytics, data warehousing, CDC bootstrap, or compliance purposes.

The existing approach to reading TiDB data for analytics is TiSpark, which connects directly to a running TiDB cluster. This has several disadvantages:

- **Cluster impact**: TiSpark's concurrent table scans can degrade OLTP performance, often requiring dedicated read-only TiKV replicas solely for analytical reads.
- **Reliability**: TiSpark jobs are sensitive to cluster state, region leader changes, and GC timing. Production deployments report frequent job failures requiring retries.
- **Version coupling**: TiSpark does not guarantee forward compatibility with TiDB v7.0+. Organizations running newer TiDB versions face growing compatibility gaps.
- **Infrastructure requirements**: TiSpark requires network connectivity to a running TiDB cluster, mTLS configuration, and compatible TiKV API versions.

Since TiDB BR already writes complete snapshots to object storage on a regular schedule, reading these backups directly is both cheaper and more reliable than querying a live cluster. The backup data is immutable, versioned, and available without cluster coordination.

### Use Cases

1. **Analytics and data warehousing**: Convert TiDB snapshots to Iceberg/Parquet tables queryable by Trino, Spark SQL, or any Iceberg-compatible engine.
2. **Snapshot verification**: Validate that backup data is complete and consistent by querying it directly. Enables row-count checks, column-level checksums, and sample comparisons against the live cluster without performing a full restore.
3. **CDC bootstrap (real-time OLAP foundation)**: Provide the initial full-table snapshot when onboarding tables to a CDC pipeline. The converter produces the base table; TiCDC then applies incremental changes going forward. This two-phase approach (snapshot + CDC stream) creates a continuously-updated analytical mirror of TiDB — the foundation for real-time TiDB OLAP without dedicated read replicas. Multiple CDC consumption patterns are possible: a streaming processor (e.g., Flink consuming from Kafka, as in the [Marlin](https://medium.com/pinterest-engineering/marlin-near-real-time-data-ingestion-for-the-lakehouse-6ea70189e269) architecture) for near-real-time freshness, or a batch approach using TiCDC's S3 sink with periodic Spark MERGE for TSO-level consistency on Iceberg tables.
4. **Cost reduction**: Eliminate dedicated read-only TiKV replicas by reading directly from object storage backups.

## Glossary

- **BR**: TiDB Backup & Restore tool that produces full-cluster snapshots as SST files in object storage.
- **SST**: Sorted String Table, the RocksDB file format used by TiKV to persist data.
- **Write CF / Default CF**: The two RocksDB Column Families relevant to backup. Write CF stores MVCC metadata (commit timestamps, write types). Default CF stores full row data for values too large to inline in the Write CF.
- **MVCC**: Multi-Version Concurrency Control. TiKV stores multiple versions of each key; the converter resolves the correct version at the backup's snapshot timestamp.
- **Arrow IPC**: Apache Arrow's Inter-Process Communication format, used to transfer columnar data between Go and the JVM without serialization overhead.
- **TSO**: Timestamp Oracle. TiDB's global logical clock providing causally-ordered timestamps. The backup's `EndVersion` TSO marks the exact point-in-time snapshot.

## Proposal

### Architecture Overview

The system is organized in four layers. Object storage holds the immutable backup files. A Go native library handles all TiDB-specific parsing (SST, MVCC, row codec, Arrow serialization). A CGO+JNI bridge exposes the Go library to the JVM. A Spark application orchestrates distributed processing and writes the output Iceberg table.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Object Storage (S3/GCS/Azure)                        │
│                                                                             │
│  backup/{cluster}/{timestamp}/                                              │
│    backupmeta              ← Protobuf: table schemas, region→SST mapping    │
│    *.datafile.*            ← Protobuf: MetaFile index (lists SST files)     │
│    {store}/{region}_write.sst    ← Write CF: MVCC commit records            │
│    {store}/{region}_default.sst  ← Default CF: full row data                │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                    ┌──────────────────┐│┌──────────────────┐
                    │   Spark Driver   │││                  │
                    │                  │││                  │
                    │  Parse backupmeta│││                  │
                    │  Extract schemas ││┘                  │
                    │  Map regions     │                    │
                    │  to partitions   │                    │
                    └────────┬─────────┘                    │
                             │ region tasks                 │
              ┌──────────────┼──────────────┐              │
              │              │              │              │
              ▼              ▼              ▼              │
┌──────────────────┐┌──────────────────┐┌──────────────────┐
│  Spark Executor  ││  Spark Executor  ││  Spark Executor  │
│                  ││                  ││                  │
│ ┌──────────────┐ ││ ┌──────────────┐ ││ ┌──────────────┐ │
│ │   Layer 1    │ ││ │   Layer 1    │ ││ │   Layer 1    │ │
│ │  Go Library  │ ││ │  Go Library  │ ││ │  Go Library  │ │
│ │              │ ││ │              │ ││ │              │ │
│ │ S3 download  │ ││ │ S3 download  │ ││ │ S3 download  │ │
│ │ SST parse    │ ││ │ SST parse    │ ││ │ SST parse    │ │
│ │ MVCC resolve │ ││ │ MVCC resolve │ ││ │ MVCC resolve │ │
│ │ Row decode   │ ││ │ Row decode   │ ││ │ Row decode   │ │
│ │ Arrow batch  │ ││ │ Arrow batch  │ ││ │ Arrow batch  │ │
│ └──────┬───────┘ ││ └──────┬───────┘ ││ └──────┬───────┘ │
│        │         ││        │         ││        │         │
│ ┌──────┴───────┐ ││ ┌──────┴───────┐ ││ ┌──────┴───────┐ │
│ │  Layer 2+3   │ ││ │  Layer 2+3   │ ││ │  Layer 2+3   │ │
│ │  CGO + JNI   │ ││ │  CGO + JNI   │ ││ │  CGO + JNI   │ │
│ │ Arrow IPC →  │ ││ │ Arrow IPC →  │ ││ │ Arrow IPC →  │ │
│ │ DirectBuffer │ ││ │ DirectBuffer │ ││ │ DirectBuffer │ │
│ └──────┬───────┘ ││ └──────┬───────┘ ││ └──────┬───────┘ │
│        │         ││        │         ││        │         │
│ ┌──────┴───────┐ ││ ┌──────┴───────┐ ││ ┌──────┴───────┐ │
│ │   Layer 4    │ ││ │   Layer 4    │ ││ │   Layer 4    │ │
│ │  Spark Rows  │ ││ │  Spark Rows  │ ││ │  Spark Rows  │ │
│ │  DataFrame   │ ││ │  DataFrame   │ ││ │  DataFrame   │ │
│ └──────────────┘ ││ └──────────────┘ ││ └──────────────┘ │
└────────┬─────────┘└────────┬─────────┘└────────┬─────────┘
         └───────────────────┼───────────────────┘
                             ▼
              ┌─────────────────────────────┐
              │     Iceberg Table Write     │
              │  (Parquet, partitioned by   │
              │   backup_ts from TSO)       │
              └─────────────────────────────┘
```

### Design Principles

1. **Reuse TiDB's own code**: The core library is written in Go and directly imports TiDB/TiKV packages (`tablecodec`, `rowcodec`, `types`, `kv`). This eliminates reimplementation bugs and ensures correctness as internal formats evolve.
2. **Stateless executor model**: Each Spark executor independently processes one region. No shared state, no coordinator. A region is the atomic unit of parallelism.
3. **Fail-fast on errors**: Any SST read or parse failure immediately fails the region task so the compute engine can retry or fail visibly. Silent data loss is never acceptable.
4. **Arrow as the language boundary**: Data crosses from Go to the JVM exactly once per region, as Arrow IPC bytes. No per-row serialization.

### Technology Stack: Go + CGO + JNI + Arrow

**Why Go?** The core backup parsing must handle SST reading, MVCC resolution, and TiDB's internal data formats. Go allows direct reuse of existing, battle-tested packages:

- `github.com/pingcap/tidb/pkg/tablecodec` — row encoding/decoding
- `github.com/pingcap/tidb/pkg/types` — TiDB data types
- `github.com/pingcap/tidb/pkg/util/rowcodec` — row codec v2 decoder
- `github.com/pingcap/kvproto` — backup protobuf definitions
- `github.com/cockroachdb/pebble` — SST file reader (RocksDB-compatible)

Reimplementing these in Java/Rust/C++ would be error-prone and expensive to maintain across TiDB versions.

**Why JNI?** The converter is distributed via Spark, which runs on the JVM. JNI bridges Go (via CGO) to Spark executors. The overhead is minimal — data crosses the boundary once per region as a single Arrow IPC byte array.

**Why Arrow?** Apache Arrow provides a language-independent columnar memory format. The Go library writes Arrow RecordBatches; the JVM reads them with zero deserialization. This eliminates per-row overhead at the language boundary.

### Component Layers

#### Layer 1: Go Core Library

```
pkg/
  backup_manager.go    — Parse backupmeta protobuf, discover tables, group files by region
  mvcc_parsing.go      — Decode TiKV engine keys (Write CF and Default CF)
  row_decoder.go       — MVCC reconstruction + row codec decoding
  arrow_output.go      — Build Arrow schema and RecordBatches from decoded rows
  arrow_ipc.go         — Serialize RecordBatches to Arrow IPC stream
  datum_conversion.go  — TiDB Datum → Arrow type conversion
```

**MVCC Reconstruction Algorithm:**

For a given region:

1. Scan all Write CF entries. Decode each engine key to extract `userKey` and `commitTs`. Parse the value to get `writeType` (`P`=Put, `D`=Delete, `L`=Lock, `R`=Rollback) and `startTs`.
2. Keep only `Put` and `Delete` records. Discard `Lock` and `Rollback`.
3. For each unique `userKey`, retain only the record with the highest `commitTs` (latest version visible at backup snapshot time).
4. For each latest-version `Put` that has no inline short value, look up the corresponding Default CF entry by `(userKey, startTs)`.
5. Emit rows: decode row bytes (from short value or Default CF) using `tablecodec.DecodeRowToDatumMap`.
6. Skip keys whose latest version is `Delete`.

```go
// Simplified MVCC resolution
for keyHex, wr := range latestWrite {
    if wr.writeType != 'P' {
        continue // Delete — row was removed before snapshot
    }
    var rowBytes []byte
    if len(wr.shortVal) > 0 {
        rowBytes = wr.shortVal
    } else {
        rowBytes = defaultValues[keyHex]
    }
    datumMap, _ := tablecodec.DecodeRowToDatumMap(rowBytes, columns, tz)
    // convert datumMap to Arrow row ...
}
```

**Error Propagation:**

If any SST file in a region cannot be read or parsed, the entire region fails immediately. This ensures the compute engine retries or surfaces the failure — partial regions are never emitted.

```go
processFile := func(f *backuppb.File, handler func(key, val []byte) error) error {
    content, err := client.ReadFile(ctx, f.Name)
    if err != nil {
        return fmt.Errorf("read %s: %w", f.Name, err)
    }
    if err := scanSSTWithPebble(content, handler); err != nil {
        return fmt.Errorf("scan %s: %w", f.Name, err)
    }
    return nil
}
```

#### Layer 2: CGO Exports

Go functions are exported as C-callable via CGO:

```go
//export BR_CreateManager
func BR_CreateManager(backupPath *C.char, options *C.char) C.int

//export BR_ProcessRegionToArrow
func BR_ProcessRegionToArrow(
    schemaJSON *C.char,
    regionFilesJSON *C.char,
    backupPath *C.char,
    outBuffer unsafe.Pointer,
    bufferSize C.int,
) C.int
```

The driver-side API is stateful (manager lifecycle for metadata parsing). The executor-side API (`BR_ProcessRegionToArrow`) is completely stateless — it takes JSON inputs and writes Arrow IPC bytes to a caller-provided buffer.

#### Layer 3: JNI Wrapper (C++)

A thin C++ wrapper implements JNI methods that call the CGO exports:

```cpp
JNIEXPORT jint JNICALL Java_NativeBridge_processRegionToArrow(
    JNIEnv *env, jclass cls,
    jstring schemaJSON, jstring regionFilesJSON,
    jstring backupPath, jobject directBuffer, jint bufferSize) {

    const char* schema = env->GetStringUTFChars(schemaJSON, nullptr);
    const char* region = env->GetStringUTFChars(regionFilesJSON, nullptr);
    const char* path   = env->GetStringUTFChars(backupPath, nullptr);
    void* buf = env->GetDirectBufferAddress(directBuffer);

    int result = BR_ProcessRegionToArrow(
        (char*)schema, (char*)region, (char*)path, buf, bufferSize);

    env->ReleaseStringUTFChars(schemaJSON, schema);
    env->ReleaseStringUTFChars(regionFilesJSON, region);
    env->ReleaseStringUTFChars(backupPath, path);
    return result;
}
```

#### Layer 4: Spark Application (Scala/Java)

The Spark job orchestrates distributed conversion:

**Driver phase:**
1. Read `backupmeta` from object storage via Go library (JNI on driver)
2. Extract table schema, backup timestamp (TSO `EndVersion`), and region-to-file mapping
3. Create one Spark partition per region for maximum read parallelism

**Executor phase:**
1. Load native libraries once per JVM
2. Call `BR_ProcessRegionToArrow` for the assigned region
3. Convert Arrow IPC bytes to Spark Rows via Arrow Java reader

**Write phase:**
1. Coalesce partitions based on total table bytes for appropriately-sized output files
2. Write to Iceberg using `DataFrameWriterV2.overwritePartitions()` with dynamic partition overwrite mode, partitioned by `backup_ts` derived from the backup's TSO

### Type Mapping

| TiDB/MySQL Type | Arrow Type | Spark Type | Notes |
|---|---|---|---|
| TINYINT | Int64 | LongType | Uniform integer representation |
| TINYINT(1) | Boolean | BooleanType | MySQL boolean convention |
| SMALLINT, INT, BIGINT | Int64 | LongType | |
| BIGINT UNSIGNED | Decimal128(20,0) | DecimalType(20,0) | Exceeds Int64 range |
| FLOAT | Float32 | DoubleType | Promoted to Double at JVM boundary |
| DOUBLE | Float64 | DoubleType | |
| DECIMAL(p,s) | Decimal128(p,s) | DecimalType(p,s) | |
| TIMESTAMP, DATETIME | Timestamp(μs) | TimestampType | Microsecond precision preserved |
| DATE | Date32 | DateType | Days since epoch |
| VARCHAR, TEXT | String (UTF-8) | StringType | |
| VARBINARY, BLOB | Binary | BinaryType | charset=binary |
| JSON | String | StringType | Serialized JSON text |

### Backup Format Compatibility

The converter reads the BR MetaV2 backup format (default since TiDB 6.x), where `backupMeta.FileIndex` references `.datafile.*` MetaFile protobuf messages that list SST files per region.

The backup format is stable across TiDB versions. We have verified compatibility from TiDB v8.1 through v8.5:

- `backuppb.BackupMeta` protobuf structure: unchanged
- `backuppb.File` message fields: unchanged
- SST file layout (Write CF + Default CF per region): unchanged
- MVCC entry semantics (Put/Delete/Lock/Rollback): unchanged
- Row codec (v2): unchanged

### Backup Path Discovery

The converter supports automatic discovery of the latest backup from a parent directory:

```
Given: s3://bucket/backup/{cluster}/
  └── 2026-04-30t02-00-00/
  └── 2026-05-01t02-00-00/   ← selected (latest with valid backupmeta)
```

Multiple parent directories can be provided (comma-separated) to cover different backup schedules (daily, hourly) for the same cluster.

### Scheduling and Orchestration

The converter is packaged as a fat JAR (Scala app + Go `.so` + JNI `.so`) and invoked by Airflow DAGs via a Spark operator. Each DAG targets one table and runs on a configurable schedule (e.g., twice daily). DAGs can be auto-generated from a metadata service that tracks which tables require snapshots.

Resource profile:
- 1 core per executor (workload is I/O-bound — S3 downloads dominate; CPU usage is low)
- Moderate executor memory with off-heap overhead for JNI direct buffers
- Each region is typically 10–100 MB of SST data, so per-executor memory requirements are modest

## Rationale

### Why Go + JNI instead of pure JVM?

| Approach | Pros | Cons |
|---|---|---|
| **Go + JNI (selected)** | Reuses TiDB packages directly; correctness by construction; easy upstream maintenance | JNI complexity; cross-language debugging |
| Pure Java/Scala | No JNI; single ecosystem | Must reimplement SST/MVCC/codec from scratch; high bug risk; diverges from upstream |
| Rust/C++ | High performance | Cannot reuse TiDB Go packages; same reimplementation burden |
| Python | Rapid prototyping | Too slow for CPU-intensive parsing at scale |

The ability to `import` TiDB's own packages and stay in sync as internal formats evolve outweighs JNI complexity.

### Why Arrow IPC for the language boundary?

- **Single crossing**: One byte array per region rather than millions of per-row JNI calls.
- **Zero-copy on Go side**: Arrow's columnar layout matches batch row decoding naturally.
- **Standard format**: Arrow Java reads the IPC stream directly; only minimal per-cell type promotion is needed.

### Why stateless executors?

Each region is self-contained (its own SST files). Stateless executors mean:
- Natural partition-level parallelism in Spark
- Simple retry (failed task re-processes from scratch)
- No memory leaks from long-lived native objects
- No coordinator or shared state

## Compatibility

- **No changes to TiDB or TiKV**: The converter reads existing BR backup files. No cluster-side changes required.
- **BR format versioning**: Reads MetaV2 format. Verified compatible across TiDB v8.1–v8.5.
- **Spark**: Requires Spark 3.2+ (DataFrameWriterV2 API for Iceberg).
- **Iceberg**: Compatible with format version 2.

## Implementation

1. Go core library: SST reading, MVCC resolution, row decoding, Arrow output.
2. CGO exports and JNI wrapper for Go↔JVM bridge.
3. Spark application: driver metadata phase, executor region processing, Iceberg write.
4. Airflow DAG framework for scheduled and ad-hoc conversion jobs.
5. Type mapping validation: end-to-end verification against TiSpark output for all TiDB data types.
6. Performance benchmarking at production scale (10+ TiB, millions of regions).

## Performance and Data Correctness

The converter has been validated at production scale (10+ TiB of backup data, millions of regions) with strong performance characteristics. Throughput scales near-linearly with executor count since each region is processed independently with no cross-executor coordination. The workload is I/O-bound (object storage downloads), so adding executors directly increases aggregate download bandwidth.

Compared to TiSpark, the converter achieves significantly better reliability (near-perfect first-attempt success rate vs. frequent TiSpark retries due to cluster sensitivity) and eliminates all cluster impact since it reads immutable files from object storage.

**Data correctness guarantee**: The converter's output has been validated to produce **100% row-count match and 100% column-level checksum match** against TiSpark output across all TiDB data types present in production. The verification methodology:

1. Run the backup converter on a BR snapshot, outputting to an Iceberg table.
2. Run TiSpark with a stale read at the identical backup TSO (`EndVersion`), outputting to a separate Iceberg table.
3. Compare: `COUNT(*)` match + per-column `MD5(CAST(col AS STRING))` aggregate checksum match.

This has been validated across tables containing BIGINT, VARCHAR, DECIMAL, TIMESTAMP, DATETIME, JSON, BLOB, FLOAT, DOUBLE, DATE, ENUM, SET, and BINARY types. The converter produces bit-for-bit identical analytical results to TiSpark at the same point in time.

## Open Issues

- **Incremental backup support**: Current implementation handles full backups only (`LastBackupTS = 0`). Supporting incremental backups would require processing delete tombstones and merging with prior snapshots.
- **Partitioned table support**: TiDB range/hash partitioned tables store each partition under a separate table ID. The converter processes them independently; a unified view across partitions may be useful.
- **Upstream contribution path**: Determine the appropriate repository and package location for contributing the Go library to the TiDB ecosystem.
- **Non-Spark consumers**: The Go library could also serve standalone CLI tools or non-Spark engines (Flink, Trino connectors) via the same CGO interface.

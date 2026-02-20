# TiDB Design Documents

- Author(s): TBD
- Discussion PR: TBD
- Tracking Issue: TBD

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes in-database ONNX model serving for TiDB. The feature embeds an ONNX runtime inside TiDB server to execute inference as SQL expressions, supports MVCC-consistent model versioning with `AS OF TIMESTAMP` semantics, and enables hybrid predicate filtering with optional TiFlash pushdown. Model artifacts live in external storage (S3-compatible and local filesystem for dev/test) with integrity validation.

User spec: `/Users/brian.w/projects/tidb/docs/in-db-onnx-model-serving-user-spec.md`

## Motivation or Background

Users increasingly want to use ML inference directly in SQL without exporting data to external serving systems. This is especially valuable for feature filtering, classification, and scoring inside transactional or HTAP queries. TiDB already supports vector search and HTAP, so in-database model inference is a logical extension that reduces data movement, latency, and operational complexity.

Key goals:

- Provide SQL-native inference for ONNX (FP32 only initially).
- Preserve snapshot semantics: model versions resolve consistently with the data snapshot, including `AS OF TIMESTAMP`.
- Support hybrid condition filtering, allowing model functions in WHERE/HAVING/JOIN while remaining cost-based and deterministic.
- Allow pushdown to TiFlash when supported to minimize data transfer and improve analytic workloads.
- Keep the system secure, deterministic, and governable by default.

Note: Even without model inference pushdown, TiFlash still adds value by pushing down standard predicates, projections, and aggregations, reducing the rowset before TiDB runs model inference.

## Detailed Design

### SQL surface and privileges

DDL:

- `CREATE MODEL <name> (INPUT <cols...> OUTPUT <cols...>) USING ONNX LOCATION '<uri>' CHECKSUM '<sha256>' [COMMENT ...]`
- `ALTER MODEL <name> SET LOCATION '<uri>' CHECKSUM '<sha256>'` (creates a new version)
- `DROP MODEL <name>` (marks deleted; historical versions remain for time travel)
- `SHOW CREATE MODEL <name>`

Inference:

- `MODEL_PREDICT(model_name, <inputs...>)` returns a structured result. Accessors can be used to reference outputs (e.g. `MODEL_PREDICT(m, a, b).score`).
- `MODEL_PREDICT` is valid in `SELECT`, `WHERE`, `HAVING`, `JOIN ON`, and computed columns.

Privileges:

- `CREATE MODEL`, `ALTER MODEL`, `DROP MODEL` for DDL.
- `EXECUTE MODEL` for inference.
- Privileges are grantable at database or global scope.

NULL behavior:

- By default, NULL inputs are errors (strict semantics). A sysvar (`tidb_model_null_behavior`) can optionally allow NULL->NULL.

### Model metadata and snapshot semantics

Two system tables are introduced:

- `mysql.tidb_model` for logical model definitions (id, name, owner, status).
- `mysql.tidb_model_version` for immutable versions (version id, created at, artifact URI, checksum, input/output schema, runtime options, status).

Schema (v1):

```sql
CREATE TABLE mysql.tidb_model (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  db_name VARCHAR(64) NOT NULL,
  model_name VARCHAR(64) NOT NULL,
  owner VARCHAR(64) NOT NULL DEFAULT '',
  created_by VARCHAR(64) NOT NULL DEFAULT '',
  updated_by VARCHAR(64) NOT NULL DEFAULT '',
  status VARCHAR(16) NOT NULL DEFAULT 'public',
  comment TEXT,
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted_at TIMESTAMP NULL DEFAULT NULL,
  UNIQUE KEY uniq_model (db_name, model_name)
);

CREATE TABLE mysql.tidb_model_version (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  model_id BIGINT NOT NULL,
  version BIGINT NOT NULL,
  engine VARCHAR(16) NOT NULL,
  location TEXT NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  input_schema JSON NOT NULL,
  output_schema JSON NOT NULL,
  options JSON,
  created_by VARCHAR(64) NOT NULL DEFAULT '',
  status VARCHAR(16) NOT NULL DEFAULT 'public',
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TIMESTAMP NULL DEFAULT NULL,
  UNIQUE KEY uniq_model_version (model_id, version),
  KEY idx_model_id (model_id)
);
```

These tables are MVCC-versioned. When a query is executed under `AS OF TIMESTAMP`, the model version is resolved using the same timestamp. If no version is visible for that time, the query errors. Without `AS OF TIMESTAMP`, the model version is resolved at statement start and remains stable for the duration of the statement.

### Artifact storage and caching

Artifacts are stored externally and referenced by URI. Supported backends in v1:

- S3-compatible object storage.
- Local filesystem URI (dev/test only).

The artifact checksum is validated on load. TiDB servers maintain:

- A statement-local cache keyed by `(model_id, version_id)`.
- An optional process-level cache with LRU + TTL.

The runtime does not access filesystem or network during inference; only artifact fetching uses storage APIs.

### Runtime integration and determinism

The ONNX runtime is embedded in TiDB server and supports FP32 models only. Custom ops are disabled by default. A sysvar (`tidb_enable_model_custom_ops`) gates opt-in usage.

Determinism:

- Non-deterministic ops are rejected unless `tidb_model_allow_nondeterministic` is enabled.
- Fixed thread pool and deterministic execution settings are applied to the runtime.

Type mapping:

- SQL scalars map to 1-element tensors.
- Array/vector types map to 1-D tensors.
- Output tensors are unpacked into structured SQL types.

### Hybrid condition filtering and pushdown

Model predicates are first-class expressions. The optimizer treats them as expensive filters and orders them after cheaper predicates unless a cost model indicates otherwise. Hybrid condition filtering allows combining model predicates with standard predicates in WHERE/HAVING/JOIN while preserving correctness. Even without model inference pushdown, non-model predicates can still be pushed down to TiFlash to reduce the rowset before inference.

Pushdown strategy:

- In v1, TiDB evaluates model predicates only.
- TiFlash pushdown still applies to non-model filters/aggregations/projections to minimize data transfer and inference volume.
- Future extension: allow TiFlash model predicate pushdown when it advertises an ONNX runtime capability and the optimizer selects it.
- TiKV pushdown is out of scope for v1.

### Error handling and visibility

Errors in model load or inference are fatal to the statement (no best-effort semantics). Error codes include the model name, version, and cause (missing artifact, checksum mismatch, unsupported op, timeout).

Visibility:

- `EXPLAIN` and `EXPLAIN ANALYZE` show `MODEL_PREDICATE` and `MODEL_PROJECT` nodes with runtime stats.
- Slow query logs include per-model inference time and the resolved version.
- `INFORMATION_SCHEMA` views expose model versions and cache status.

### Governance and configuration

Key sysvars (default OFF):

- `tidb_enable_model_inference`
- `tidb_model_max_batch_size`
- `tidb_model_timeout`
- `tidb_model_cache_capacity`
- `tidb_model_cache_ttl`
- `tidb_model_allow_nondeterministic`
- `tidb_enable_model_custom_ops`

Resource control integrates with existing resource groups to account for inference CPU and memory. Runaway query management can terminate excessive inference workloads.

### Compatibility considerations

- Parser/DDL: new keywords and DDL statements.
- Planner/Executor: new expression nodes and cost model support.
- Statistics: optionally track model predicate selectivity (future work).
- TiKV: no changes for v1.
- TiFlash: optional runtime capability gate and pushdown support.
- BR/TiCDC/Dumpling: model metadata tables should be backed up and replicated. External artifacts are not automatically captured by BR; operators must ensure artifacts are stored durably.
- Upgrade: feature is behind flags; system tables are additive.
- Downgrade: older versions ignore model tables; recommended to disable inference before downgrade.

## Test Design

### Functional Tests

- DDL lifecycle: create/alter/drop model and `SHOW CREATE MODEL`.
- Privilege enforcement: execute with/without `EXECUTE MODEL`.
- `MODEL_PREDICT` type mapping and output accessors.
- Snapshot semantics: `AS OF TIMESTAMP` resolves correct model version.
- Artifact checksum validation and error paths.

### Scenario Tests

- Hybrid predicates in WHERE/HAVING/JOIN with standard filters.
- TiFlash pushdown enabled vs disabled.
- Model hot-swap: concurrent ALTER while queries run.
- Cache behavior and TTL expiry.

### Compatibility Tests

- Backup/restore of model metadata tables.
- TiCDC replication behavior for model metadata.
- Downgrade with inference disabled.

### Benchmark Tests

- Inference latency vs batch size.
- Throughput under concurrent queries.
- Impact of model predicates on mixed workloads.

## Impacts & Risks

Impacts:

- Lower latency and less data movement for ML inference use cases.
- Additional CPU/memory usage in TiDB nodes for inference.
- Optional performance benefits on TiFlash for analytic workloads.

Risks:

- Misconfigured or large models can cause resource pressure.
- External artifact availability may affect query reliability.
- Incorrect cost modeling could lead to suboptimal predicate ordering.
- Non-deterministic ops or custom ops could introduce inconsistent results if enabled.

## Investigation & Alternatives

Alternatives considered:

- External serving only (SQL calls HTTP/gRPC): simpler but higher latency and more operational complexity.
- Full TiKV/TiFlash pushdown from day one: higher performance potential but much larger scope and risk.
- Storing artifacts in system tables: simpler snapshot semantics but impractical for large models.

The chosen design balances correctness, performance, and risk by embedding inference in TiDB, supporting optional TiFlash pushdown, and using external artifact storage with MVCC metadata.

## Unresolved Questions

- Should we provide a built-in UDF for model feature pre-processing (e.g., normalization)?
- Should we support limited INT8 quantized models in a follow-up release?
- What is the minimal TiFlash capability surface for safe pushdown in v1?

# In-DB MLflow (PyFunc) Model Serving Design

- Author(s): TBD
- Discussion PR: TBD
- Tracking Issue: TBD

## Overview

This document extends the existing in-database model serving feature to support MLflow
PyFunc models in addition to ONNX. The design keeps the SQL surface, privileges, snapshot
semantics, and hybrid predicate behavior unchanged, while introducing a new model engine
(`mlflow`) backed by a local Python sidecar on each TiDB node.

Decisions (confirmed):

- Model flavor: MLflow **PyFunc only**.
- Execution: **local Python sidecar** per TiDB node (no external serving endpoint).
- Environment: **system Python environment** only (no per-model venv/container management).
- DDL syntax: `USING MLFLOW`.
- Model URI schemes: `file:` and `s3:` only.
- Inference: **batch** execution; **FP32 only** inputs/outputs.

## Goals

- Allow `MODEL_PREDICT` to run against MLflow PyFunc models with the same semantics as ONNX.
- Preserve MVCC snapshot semantics for model version resolution (`AS OF TIMESTAMP`).
- Maintain hybrid predicate filtering and cost-based ordering.
- Keep the security model and privilege checks unchanged.
- Avoid adding new sysvars for v1 MLflow support.

## Non-Goals

- Supporting non-PyFunc MLflow flavors directly (e.g., native sklearn/torch flavors in SQL).
- Automatic Python environment management (venv/conda/container creation).
- External MLflow Tracking Server integration (`runs:/`, `models:/` URIs).
- TiFlash model predicate pushdown for MLflow.
- Non-FP32 data types.

## Detailed Design

### SQL Surface and Metadata

The SQL surface remains unchanged, with the only addition being a new engine keyword:

- `CREATE MODEL <name> (INPUT <cols...> OUTPUT <cols...>) USING MLFLOW LOCATION '<uri>' CHECKSUM '<sha256>' [COMMENT ...]`
- `ALTER MODEL <name> SET LOCATION '<uri>' CHECKSUM '<sha256>'`

Model versions continue to live in `mysql.tidb_model_version` with `engine='mlflow'`.
No schema changes are required.

### Model Artifacts and Checksums

MLflow models are directory artifacts. The loader accepts `file:` and `s3:` URIs and
materializes the model directory into the local TiDB model cache. The checksum is computed
from a deterministic manifest of the directory contents (sorted relative paths + file
content hashes). Any mismatch is a fatal DDL error.

### MLflow Validation and Schema Matching

On `CREATE MODEL`, TiDB reads the `MLmodel` file and enforces:

- Presence of the `python_function` (PyFunc) flavor.
- Presence of a model signature with named inputs/outputs.
- Input/output names and counts must match the SQL model schema.
- Input/output types must be numeric and convertible to FP32; otherwise reject.

The validated input/output schema is stored in `tidb_model_version` as JSON (same as ONNX).

### Runtime Architecture

A new `ModelBackend` interface unifies inference across ONNX and MLflow:

- `InferBatch(ctx, modelInfo, inputs) -> outputs`
- `InspectIO(artifact) -> input/output metadata`

ONNX uses the existing in-process runtime and session cache. MLflow uses a local Python
sidecar and an in-process Go client.

### Python Sidecar

Each TiDB node lazily starts a local Python worker pool. The pool is managed in-process
with a small fixed size (default 2-4). Communication is over a local Unix domain socket
using a lightweight RPC protocol (binary FP32 payload + JSON header). The protocol
supports:

- `Load(model_path)` with LRU caching per worker.
- `Predict(model_path, input_names, batch_size, payload)` returning FP32 outputs.

The sidecar loads models via `mlflow.pyfunc.load_model` and executes `predict` using
NumPy/pandas for batch input. Outputs are required to be scalar or 1-D FP32 arrays per row.

### Inference Semantics

- `MODEL_PREDICT` remains the SQL entry point.
- Inputs are cast to FP32; non-castable inputs are errors.
- Output columns are FP32 and follow the declared output schema.
- Hybrid predicates and cost ordering continue to treat model predicates as expensive.
- Snapshot semantics and privilege checks are identical to ONNX.

### Configuration

No new sysvars are introduced. A minimal `model-mlflow` block in `tidb.toml` is added:

- `python` (default `python3`)
- `sidecar_workers` (default 2)
- `request_timeout` (default `2s`)
- `model_cache_entries` (default 64)

### TiFlash Interaction

MLflow inference is evaluated in TiDB only. TiFlash remains valuable for pushing down
standard predicates/aggregations to reduce the rowset before model inference.

## Error Handling

- Invalid MLflow model (missing PyFunc flavor or signature) -> DDL error.
- Schema mismatch or unsupported types -> DDL error.
- Sidecar startup or runtime errors -> statement error with model/version context.
- Timeouts -> statement error; query can be retried.

## Test Design

### Unit Tests

- Parser accepts `USING MLFLOW`.
- DDL validation of MLflow `MLmodel` file (PyFunc + signature).
- Loader checksum for model directory.
- Signature mismatch and unsupported type errors.

### Component Tests

- Sidecar protocol with a Go test double (no Python dependency).
- Batch inference handling and timeout propagation.

### Integration Tests

- `tests/integrationtest/t/model_mlflow.test` using a tiny public PyFunc model staged into
  `/tmp/tidb-models`.
- Test hybrid predicate filtering with `MODEL_PREDICT` and verify plan output.
- Skip test if `python3` or `mlflow` is missing on the test node.

## Risks and Mitigations

- **Environment drift:** System Python dependencies may differ across nodes. Mitigation:
  clear errors and operator documentation; keep v1 scope small.
- **Performance:** Python sidecar adds overhead. Mitigation: batch inference + worker pool.
- **Security:** PyFunc code can execute arbitrary Python. Mitigation: require explicit
  `tidb_enable_model_inference` and existing `MODEL_EXECUTE` privileges.

## Rollout

- Feature remains gated by `tidb_enable_model_inference`.
- No new sysvars.
- Add observability labels for `model_type=mlflow` in existing metrics.

## Alternatives Considered

1. Embedded Python in `tidb-server` (lower latency, higher risk).
2. External MLflow Serving endpoint (simpler integration, weaker snapshot semantics).
3. Full MLflow URI support (`runs:/`, `models:/`) via tracking server.

## Open Questions

- Should we require an explicit PyFunc signature, or allow schema inference in v1?
- Should we introduce resource group accounting for sidecar CPU time separately?
- Should we add per-model environment validation hooks?

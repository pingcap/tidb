# In-DB ONNX Model Serving Phase 6: Observability Design

## Overview

Phase 6 adds observability for model inference across three surfaces:

- `EXPLAIN ANALYZE` operator stats
- Slow query log fields
- Prometheus metrics (low-cardinality only)

The design uses statement-scoped aggregation to keep metrics low-cardinality while still providing per-model detail in logs and `EXPLAIN ANALYZE`.

## Goals

- Surface model inference time, batch size, and error counts in `EXPLAIN ANALYZE`.
- Record per-model inference and load stats in slow query logs.
- Add global Prometheus metrics for model load latency, batch sizes, and error counts (no per-model labels).

## Non-Goals

- No TiFlash model pushdown observability (still future work).
- No per-model Prometheus labels (avoid cardinality explosion).

## Data Model

Introduce a statement-scoped aggregation in `StmtCtx` keyed by:

- `model_id`
- `version_id`
- `role` (`predicate` or `projection`)

Per key, record:

- `calls`
- `errors`
- `total_infer_time`
- `total_batch_size`
- `max_batch_size`
- `total_load_time`
- `load_errors`

## Instrumentation Points

- `MODEL_PREDICT` evaluation (scalar + vectorized) records:
  - inference duration
  - batch size
  - error status
  - role (`predicate` vs `projection`)
- Model load/session creation records:
  - load duration
  - load errors

Aggregation happens in `StmtCtx`, so all visibility is statement-consistent and aligns with snapshot semantics.

## EXPLAIN ANALYZE

Model operators (`MODEL_PREDICATE`, `MODEL_PROJECT`) read aggregated stats and expose:

- total inference time
- call count
- average batch size
- max batch size
- error count

## Slow Query Log

Add a JSON field (e.g., `model_inference`) with an array of entries:

- `model_id`, `version_id`, `role`
- `calls`, `errors`
- `total_ms`, `avg_batch`, `max_batch`
- `load_ms`, `load_errors`

This provides per-model visibility without requiring per-model metrics labels.

## Metrics

Low-cardinality Prometheus metrics only:

- `tidb_model_inference_total{type,result}` (already exists)
- `tidb_model_inference_duration_seconds{type,result}` (already exists)
- `tidb_model_load_duration_seconds{result}` (new)
- `tidb_model_batch_size{type}` (new)

Errors are counted via `result=err`.

## Error Handling

Inference and load failures remain statement-fatal. Before returning an error, the evaluator records the partial stats into `StmtCtx` so slow logs and `EXPLAIN ANALYZE` (when available) can expose the failure context.

## Testing

- Unit tests for aggregation: `ok` vs `err`, batch size rollups, load latency.
- `EXPLAIN ANALYZE` tests verifying model operator stats.
- Slow log tests asserting `model_inference` JSON payload.
- Metrics tests verifying new histograms increment.

## Rollout

Observability is always-on when inference is enabled. No new sysvars are required for Phase 6.

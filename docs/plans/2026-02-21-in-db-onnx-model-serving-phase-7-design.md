# In-DB ONNX Model Serving Phase 7 Design

- Author(s): TBD
- Discussion PR: TBD
- Tracking Issue: TBD

## Overview

Phase 7 adds three production surfaces:

1. Optimizer cost model for hybrid predicates containing `MODEL_PREDICT`.
2. Resource group accounting that includes model inference CPU time.
3. INFORMATION_SCHEMA views for model versions and the process-level model cache.

This phase does not change SQL syntax or execution semantics.

## Goals

- Treat model predicates as expensive filters so the optimizer can order them using cost + selectivity.
- Include model inference time in existing RU accounting and runaway protection without new knobs.
- Expose model version metadata and cache state via INFORMATION_SCHEMA.

## Non-Goals

- TiFlash model predicate pushdown.
- Runtime sampling or adaptive selectivity estimation.
- New sysvars for Phase 7.

## Detailed Design

### Optimizer Cost Model for MODEL_PREDICT

**Detection**

- A predicate is marked "model predicate" if it contains `MODEL_PREDICT(...)` or output accessors derived from it.
- This can be detected by walking the expression tree and checking function names.

**Cost and Selectivity**

- Assign a fixed per-row CPU cost `model_predict_cpu_cost_per_row`.
- Assign a default selectivity `model_predict_default_selectivity` (for example 0.1).
- These are internal constants for v1 and not exposed as sysvars.

**Planner Integration**

- During predicate reordering in logical optimization, apply the cost model so cheap predicates are evaluated before model predicates unless selectivity suggests otherwise.
- Cost estimates flow into join ordering and access path selection via existing cost computation.
- If reordering is disabled for a statement, keep original predicate order.

**Explainability**

- No new EXPLAIN fields are required. Changes manifest in predicate order and plan cost.

### Resource Group Accounting

**Model Inference Time Source**

- `StmtCtx.ModelInferenceStats()` already aggregates total inference time.
- Sum total inference time across the statement and treat it as CPU time.

**Accounting**

- Add the model inference time into the statement CPU accounting used by RU calculation.
- This uses existing resource group behavior; there are no new rules or sysvars.
- If no inference occurs or stats are missing, the additional CPU charge is zero.

**Runaway**

- Existing runaway protection remains unchanged and now naturally includes inference CPU time via the updated accounting.

### INFORMATION_SCHEMA Views

#### INFORMATION_SCHEMA.TIDB_MODEL_VERSIONS

Expose model metadata based on `mysql.tidb_model` and `mysql.tidb_model_version`.

Proposed columns:

- `TABLE_SCHEMA` (db name)
- `MODEL_NAME`
- `MODEL_ID`
- `VERSION`
- `ENGINE`
- `LOCATION`
- `CHECKSUM`
- `INPUT_SCHEMA`
- `OUTPUT_SCHEMA`
- `OPTIONS`
- `STATUS`
- `CREATE_TIME`
- `DELETED_AT`

This view is read-only and mirrors system table contents at query time.

#### INFORMATION_SCHEMA.TIDB_MODEL_CACHE

Expose the process-level ONNX session cache on the local TiDB node.

Proposed columns:

- `MODEL_ID`
- `VERSION`
- `INPUT_NAMES` (comma-separated)
- `OUTPUT_NAMES` (comma-separated)
- `CACHED_AT`
- `TTL_SECONDS`
- `EXPIRES_AT` (NULL when TTL is disabled)

The cache key format already includes model id/version and IO names, so these fields can be derived without storing duplicates. This view reflects only the local node cache.

### Security and Privileges

- INFORMATION_SCHEMA access follows standard rules (read-only, non-sensitive metadata).
- No new privileges are required.

## Testing Plan

1. Optimizer
   - Unit test: in a WHERE clause with a cheap predicate and a model predicate, verify the optimizer orders the cheap predicate first under default costs.
   - Verify plan cost changes when `MODEL_PREDICT` appears.

2. Resource Group Accounting
   - Unit or integration test: run a query with mocked model inference time and assert RU accounting includes it.

3. INFORMATION_SCHEMA
   - Create a model/version and verify `TIDB_MODEL_VERSIONS` returns expected rows.
   - Populate process session cache and verify `TIDB_MODEL_CACHE` returns expected fields.

## Risks and Mitigations

- **Cost model mismatch:** Fixed costs may be inaccurate for some models. Mitigation: start with conservative defaults and tune later.
- **Metadata exposure:** Ensure I_S views do not leak sensitive data beyond existing system tables.

## Rollout

- Feature is always-on when `tidb_enable_model_inference` is enabled.
- No new configuration is introduced in Phase 7.

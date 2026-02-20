# In-DB ONNX Model Serving: User Spec

## Developer Spec

This feature lets application developers run ONNX (FP32) inference directly in SQL. You register a model once, then call it via `MODEL_PREDICT` in `SELECT`, `WHERE`, `HAVING`, or `JOIN` clauses. Model versions are MVCC-consistent with data snapshots; if you use `AS OF TIMESTAMP`, both data and model version resolve to that time.

Key behaviors:

- Deterministic by default; non-deterministic ops are rejected unless explicitly enabled.
- NULL inputs are errors by default (can be configured to return NULL).
- If a model/version isn’t visible at the snapshot, the statement errors.

### Samples

Register a model from S3 (or local dev path):

```sql
CREATE MODEL fraud_scoring (
  INPUT (age INT, amount DOUBLE, merchant_id INT)
  OUTPUT (score DOUBLE, label STRING)
) USING ONNX
LOCATION 's3://ml-models/fraud_scoring/v1/model.onnx'
CHECKSUM 'sha256:...';
```

Use in a filter:

```sql
SELECT id, MODEL_PREDICT(fraud_scoring, age, amount, merchant_id).score AS score
FROM transactions
WHERE status = 'posted'
  AND MODEL_PREDICT(fraud_scoring, age, amount, merchant_id).score > 0.9;
```

Time-travel query with snapshot-consistent model version:

```sql
SELECT id, MODEL_PREDICT(fraud_scoring, age, amount, merchant_id).label
FROM transactions AS OF TIMESTAMP '2025-12-01 12:00:00'
WHERE account_id = 42;
```

Update model (new version, old versions remain for snapshots):

```sql
ALTER MODEL fraud_scoring
SET LOCATION 's3://ml-models/fraud_scoring/v2/model.onnx'
CHECKSUM 'sha256:...';
```

## DBA/Ops Spec

This feature is default-off and controlled via sysvars. Models are stored as metadata in system tables; artifacts live in external storage (S3-compatible or local filesystem for dev/test). Each TiDB node caches models per statement and optionally in a shared LRU with TTL. Inference runs inside TiDB CPU/memory limits and is charged to the session’s resource group.

Operational controls:

- Enable/disable feature: `tidb_enable_model_inference`
- Safety: `tidb_model_allow_nondeterministic` (default OFF), `tidb_enable_model_custom_ops` (default OFF)
- Performance: `tidb_model_max_batch_size`, `tidb_model_timeout`
- Caching: `tidb_model_cache_capacity`, `tidb_model_cache_ttl`

Security:

- DDL controlled by `CREATE/ALTER/DROP MODEL`
- Inference controlled by `EXECUTE MODEL`
- No runtime network/FS access beyond artifact fetch

Observability:

- Metrics: load latency, inference latency, batch sizes, cache hit/miss, per-model errors.
- `EXPLAIN ANALYZE` shows model operator runtime stats.
- Slow query logs include per-model inference time and resolved model version.

TiFlash:

- v1: only non-model predicates/aggregations/projections are pushed down.
- Model inference runs in TiDB; pushdown of model predicates is a future extension.

### Ops samples

Enable inference:

```sql
SET GLOBAL tidb_enable_model_inference = ON;
```

Restrict a user to inference only:

```sql
GRANT EXECUTE MODEL ON *.* TO 'app_user'@'%';
```

Set conservative limits:

```sql
SET GLOBAL tidb_model_max_batch_size = 256;
SET GLOBAL tidb_model_timeout = '200ms';
SET GLOBAL tidb_model_cache_capacity = '1GB';
SET GLOBAL tidb_model_cache_ttl = '10m';
```

Disable nondeterministic/custom ops (defaults shown):

```sql
SET GLOBAL tidb_model_allow_nondeterministic = OFF;
SET GLOBAL tidb_enable_model_custom_ops = OFF;
```

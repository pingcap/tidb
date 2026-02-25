# In-DB Model Serving (ONNX + MLflow): User Spec

## Developer Spec

This feature lets application developers run ONNX (FP32) and MLflow PyFunc inference directly in SQL. You register a model once, then call it via `MODEL_PREDICT` in `SELECT`, `WHERE`, `HAVING`, or `JOIN` clauses. Model versions are MVCC-consistent with data snapshots; if you use `AS OF TIMESTAMP`, both data and model version resolve to that time. TiDB also provides built-in LLM SQL functions backed by external Vertex AI inference.

Key behaviors:

- Deterministic by default; non-deterministic ONNX ops are rejected unless explicitly enabled.
- NULL inputs are errors by default (can be configured to return NULL).
- If a model/version is not visible at the snapshot, the statement errors.
- MLflow PyFunc models must include `python_function` flavor and a signature with input/output names.
- LLM functions use a global default model and call Vertex AI over HTTPS.

### Samples

Register an ONNX model from S3 (or local dev path):

```sql
CREATE MODEL fraud_scoring (
  INPUT (age INT, amount DOUBLE, merchant_id INT)
  OUTPUT (score DOUBLE, label STRING)
) USING ONNX
LOCATION 's3://ml-models/fraud_scoring/v1/model.onnx'
CHECKSUM 'sha256:...';
```

Register an MLflow PyFunc model from a directory:

```sql
CREATE MODEL churn_mlflow (
  INPUT (tenure DOUBLE, spend DOUBLE)
  OUTPUT (score DOUBLE)
) USING MLFLOW
LOCATION 's3://ml-models/churn_pyfunc/v1'
CHECKSUM 'sha256:...';
```

Use in a filter:

```sql
SELECT id,
       MODEL_PREDICT(fraud_scoring, age, amount, merchant_id).score AS score
FROM transactions
WHERE status = 'posted'
  AND MODEL_PREDICT(fraud_scoring, age, amount, merchant_id).score > 0.9;
```

Hybrid predicate filtering with MLflow:

```sql
SELECT id
FROM customers
WHERE region = 'us'
  AND MODEL_PREDICT(churn_mlflow, tenure, spend).score > 0.7;
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

Use built-in LLM functions (Vertex AI):

```sql
SET GLOBAL tidb_llm_default_model = 'gemini-1.5-pro';
SET GLOBAL tidb_enable_llm_inference = ON;

SELECT LLM_COMPLETE('Summarize this ticket in one sentence.');
SELECT LLM_EMBED_TEXT('customer churn risk signals');
```

Note: TiDB vector search indexes and queries vectors but does not generate embeddings. `LLM_EMBED_TEXT` fills the text-to-vector step so you can persist embeddings into vector columns for similarity search.

### LLM real-world scenarios

Financial risk and compliance:

```sql
INSERT INTO risk_case_explanations(case_id, explanation)
SELECT case_id,
       LLM_COMPLETE(CONCAT('Explain risk: ip=', ip, ', device=', device, ', amount=', amount))
FROM risk_cases
WHERE score > 0.9;
```

E-commerce content and reviews:

```sql
UPDATE product_summary ps
SET highlights = LLM_COMPLETE(CONCAT('Summarize: ', description, ' ', features))
WHERE ps.updated_at > NOW() - INTERVAL 1 DAY;
```

Operations analytics:

```sql
INSERT INTO daily_insights(day, insight)
SELECT cur_day,
       LLM_COMPLETE(CONCAT(
         'KPI change summary. Revenue=', revenue,
         ', QoQ=', qoq, ', Top region=', top_region
       ))
FROM daily_kpi;
```

## DBA/Ops Spec

This feature is default-off and controlled via sysvars. Models are stored as metadata in system tables; artifacts live in external storage (S3-compatible or local filesystem for dev/test). Each TiDB node caches models per statement and optionally in a session LRU with TTL. Inference runs inside TiDB CPU/memory limits and is charged to the session's resource group.

Operational controls:

- Enable/disable DDL: `tidb_enable_model_ddl`
- Enable/disable inference: `tidb_enable_model_inference`
- NULL handling: `tidb_model_null_behavior` (`ERROR`/`RETURN_NULL`)
- Safety: `tidb_model_allow_nondeterministic` (default OFF), `tidb_enable_model_custom_ops` (default OFF)
- Performance: `tidb_model_max_batch_size`, `tidb_model_timeout`
- Caching: `tidb_model_cache_capacity`, `tidb_model_cache_ttl`
- LLM: `tidb_enable_llm_inference`, `tidb_llm_default_model`, `tidb_llm_timeout`

MLflow sidecar settings (config file):

- `model-mlflow.python`
- `model-mlflow.sidecar-workers`
- `model-mlflow.request-timeout`
- `model-mlflow.cache-entries`

LLM settings (config file):

- `llm.provider` (fixed to `vertex` in v1)
- `llm.vertex_project`
- `llm.vertex_location`
- `llm.credential_file` (optional service account JSON path)
- `llm.request_timeout`

LLM requests retry on 429/503 with exponential backoff, bounded by `tidb_llm_timeout`.

Security:

- DDL controlled by `CREATE/ALTER/DROP MODEL`
- Inference controlled by `EXECUTE MODEL`
- LLM functions controlled by `LLM_EXECUTE`
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

Grant LLM execution:

```sql
GRANT LLM_EXECUTE ON *.* TO 'app_user'@'%';
```

Set conservative limits:

```sql
SET GLOBAL tidb_model_max_batch_size = 256;
SET GLOBAL tidb_model_timeout = '200ms';
SET GLOBAL tidb_model_cache_capacity = '1GB';
SET GLOBAL tidb_model_cache_ttl = '10m';
```

Configure LLM defaults:

```sql
SET GLOBAL tidb_llm_default_model = 'gemini-1.5-pro';
SET GLOBAL tidb_llm_timeout = '5s';
```

Disable nondeterministic/custom ops (defaults shown):

```sql
SET GLOBAL tidb_model_allow_nondeterministic = OFF;
SET GLOBAL tidb_enable_model_custom_ops = OFF;
```

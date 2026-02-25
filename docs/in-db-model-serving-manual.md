# In-DB Model Serving Manual (ONNX + MLflow)

This manual covers setup, operations, and troubleshooting for ONNX and MLflow model serving in TiDB.

## Overview

- ONNX models run inside TiDB using the embedded ONNX Runtime (FP32 only).
- MLflow PyFunc models run via a per-node Python sidecar (FP32 only).
- Both engines use the same SQL surface: `CREATE MODEL` + `MODEL_PREDICT`.
- Built-in LLM SQL functions call external Vertex AI endpoints over HTTPS.
- Model versions are MVCC-consistent with data snapshots.

## Prerequisites

### ONNX Runtime

- Run `make onnxruntime_prepare` to download prebuilt libraries.
- By default, TiDB searches for the library under `lib/onnxruntime/<os>-<arch>/` relative to the `tidb-server` binary.
- Override with one of:
  - `TIDB_ONNXRUNTIME_LIB=/path/to/libonnxruntime.(so|dylib)`
  - `TIDB_ONNXRUNTIME_DIR=/path/to/onnxruntime/dir`

### MLflow PyFunc Sidecar

- Install Python and MLflow in an environment accessible by `tidb-server`.
- Configure `model-mlflow` in `config.toml`:

```toml
[model-mlflow]
python = "/path/to/python3"
sidecar-workers = 2
request-timeout = "2s"
cache-entries = 64
```

- The Python environment must have `mlflow` installed.

## Artifact Preparation

### ONNX (single file)

- Export a single ONNX model file.
- Compute the checksum as sha256 of the file bytes:

```bash
sha256sum model.onnx
```

### MLflow PyFunc (directory)

- The directory must contain `MLmodel` with `python_function` flavor and a signature.
- Compute the checksum using the manifest algorithm (paths sorted, each appended with file bytes):

```python
import hashlib
import os

root = "/path/to/mlflow/model"
paths = []
for dirpath, _, filenames in os.walk(root):
    for name in filenames:
        full = os.path.join(dirpath, name)
        rel = os.path.relpath(full, root).replace(os.sep, "/")
        paths.append(rel)
paths.sort()

h = hashlib.sha256()
for rel in paths:
    with open(os.path.join(root, rel), "rb") as f:
        data = f.read()
    h.update(rel.encode())
    h.update(b"\n")
    h.update(data)
    h.update(b"\n")

print(h.hexdigest())
```

## Configuration and Governance

### Sysvars

- Enable DDL: `tidb_enable_model_ddl`
- Enable inference: `tidb_enable_model_inference`
- NULL handling: `tidb_model_null_behavior` (`ERROR`/`RETURN_NULL`)
- Safety: `tidb_model_allow_nondeterministic`, `tidb_enable_model_custom_ops`
- Performance: `tidb_model_max_batch_size`, `tidb_model_timeout`
- Caching: `tidb_model_cache_capacity`, `tidb_model_cache_ttl`
- LLM: `tidb_enable_llm_inference`, `tidb_llm_default_model`, `tidb_llm_timeout`

### Privileges

- DDL: `CREATE MODEL`, `ALTER MODEL`, `DROP MODEL`
- Inference: `EXECUTE MODEL`
- LLM functions: `LLM_EXECUTE`

## Operations

### Create a model

```sql
CREATE MODEL fraud_scoring (
  INPUT (age INT, amount DOUBLE, merchant_id INT)
  OUTPUT (score DOUBLE, label STRING)
) USING ONNX
LOCATION 's3://ml-models/fraud_scoring/v1/model.onnx'
CHECKSUM 'sha256:...';
```

```sql
CREATE MODEL churn_mlflow (
  INPUT (tenure DOUBLE, spend DOUBLE)
  OUTPUT (score DOUBLE)
) USING MLFLOW
LOCATION 's3://ml-models/churn_pyfunc/v1'
CHECKSUM 'sha256:...';
```

### Use built-in LLM functions (Vertex AI)

```sql
SET GLOBAL tidb_llm_default_model = 'gemini-1.5-pro';
SET GLOBAL tidb_enable_llm_inference = ON;

SELECT LLM_COMPLETE('Summarize this ticket in one sentence.');
SELECT LLM_EMBED_TEXT('customer churn risk signals');
```

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

### Use in a query

```sql
SELECT id
FROM customers
WHERE region = 'us'
  AND MODEL_PREDICT(churn_mlflow, tenure, spend).score > 0.7;
```

### Inspect versions and cache

```sql
SELECT * FROM INFORMATION_SCHEMA.TIDB_MODEL_VERSIONS;
SELECT * FROM INFORMATION_SCHEMA.TIDB_MODEL_CACHE;
```

## LLM Configuration (Vertex AI)

Add the `llm` section to `config.toml`:

```toml
[llm]
provider = "vertex"
vertex_project = "my-project"
vertex_location = "us-central1"
credential_file = "/path/to/service-account.json"
request_timeout = "5s"
```

If `credential_file` is not set, TiDB uses Application Default Credentials (ADC).

LLM requests retry on 429/503 with exponential backoff, bounded by `tidb_llm_timeout`.

## Troubleshooting

### Checksum mismatch

- Ensure you are using the correct sha256 format: `sha256:<64-hex>`.
- For MLflow, make sure the manifest includes all files in the model directory.

### MLflow sidecar errors

- Verify the `model-mlflow.python` path points to a Python executable with `mlflow` installed.
- Increase `model-mlflow.request-timeout` or `tidb_model_timeout` if inference times out.
- MLflow PyFunc output must be a single output; dicts with multiple outputs are not supported in v1.

### ONNX runtime errors

- Verify the ONNX runtime library is present and readable.
- Set `TIDB_ONNXRUNTIME_LIB` or `TIDB_ONNXRUNTIME_DIR` to override the default path.

### LLM function errors

- Ensure `tidb_enable_llm_inference = ON` and `tidb_llm_default_model` is set.
- Ensure the user has `LLM_EXECUTE` privilege.
- Verify Vertex credentials are available (service account file or ADC).
- Increase `tidb_llm_timeout` or `llm.request_timeout` if requests time out.

### Inference disabled

- Ensure `tidb_enable_model_inference = ON`.
- Ensure the user has `EXECUTE MODEL` privilege.

## Limitations

- FP32 only in v1.
- MLflow PyFunc supports a single output in v1.
- Model predicate pushdown to TiFlash is not supported in v1 (non-model pushdown still applies).
- LLM functions depend on external Vertex AI availability and quotas.

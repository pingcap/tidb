# TiCI integrationtest2 Guide

## Dependencies

When running `tests/integrationtest2/run-tests.sh`:
- Required: `bash`
- Port check: `ss` or `nc` (macOS usually uses `nc`)
- Config rendering: `envsubst` (from `gettext`)

When downloading binaries (using `tici/download.sh`):
- `oras`
- `yq`
- `tar`/`gzip`

## Run directly (third_bin already prepared)

If `tests/integrationtest2/third_bin` already contains the binaries below, you can run directly:
- `pd-server`
- `tikv-server`
- `tidb-server`
- `dumpling`
- `cdc`
- `tiflash` (if it is a directory, `tiflash/tiflash` must exist)
- `tici-server`
- `minio`
- `mc`

Run:
```bash
cd /tidb/tests/integrationtest2
./run-tests.sh -t tici/tici_integration
```

## Download binaries (third_bin incomplete)

If `tests/integrationtest2/third_bin` is missing any of the binaries above, `run-tests.sh` will fail.
Download them first with `https://github.com/PingCAP-QE/ci/blob/main/scripts/artifacts/download_pingcap_oci_artifact.sh`. This script depends on `oras` and `yq`.

Example (download TiFlash from a specific branch):
```bash
cd /tidb/tests/integrationtest2/third_bin
OCI_ARTIFACT_HOST="us-docker.pkg.dev/pingcap-testing-account/hub" ./download_pingcap_oci_artifact.sh --tiflash=feature-fts
```

Example (download the latest TiCDC):
```bash
cd /tidb/tests/integrationtest2/tici/third_bin
OCI_ARTIFACT_HOST="us-docker.pkg.dev/pingcap-testing-account/hub" ./download_pingcap_oci_artifact.sh --ticdc-new=master
```

Example (download the latest TiCI):
```bash
cd /tidb/tests/integrationtest2/tici/third_bin
OCI_ARTIFACT_HOST="us-docker.pkg.dev/pingcap-testing-account/hub" ./download_pingcap_oci_artifact.sh --tici=master
```

## Hybrid vector e2e helper

For local hybrid-vector end-to-end validation against TiUP playground, use:

```bash
cd tests/integrationtest2/tici
./hybrid_vector_e2e.sh all
```

This helper is meant for iterative local development and covers:

- starting MinIO + `playground:v1.16.2-feature.fts`
- reusing the TiCDC S3 changefeed created by playground itself
- preparing a table with `1` inverted column + `1` vector column
- creating a hybrid index on an empty table
- loading about `10,000` rows through TiCDC
- inserting a post-index delta row to exercise CDC ingestion
- verifying both inverted lookup and `ORDER BY vec_l2_distance(...) LIMIT K`

Current limitation:

- `import into` / add-index backfill is not yet adapted for hybrid/vector index ingestion.
- For local e2e, use the CDC-only order: `CREATE TABLE` -> `CREATE HYBRID INDEX` -> insert rows.
- The harness no longer creates or removes changefeeds by itself; it waits for the
  playground-managed feed and verifies that its sink URI matches the expected
  `s3://<bucket>/<prefix>/cdc` prefix and includes `use-table-id-as-path=true`.

Latest local e2e notes:

- `2026-03-20`: local CDC-only hybrid-vector e2e passed with
  `PLAYGROUND_TAG=hybrid-vector-e2e-20260320g`.
  This run used the playground-managed changefeed directly, without a second
  manual `changefeed create` from the helper script.
  The discovered changefeed stayed `normal` with sink URI
  `s3://ticidefaultbucket/hybrid-vector-e2e-20260320g/cdc?...&use-table-id-as-path=true...`
  and queried sink config `date_separator = "none"`.
- `2026-03-20`: an earlier local CDC-only hybrid-vector e2e also passed with
  `PLAYGROUND_TAG=hybrid-vector-e2e-20260320d`.
  The table reached `10001` rows, `tici.tici_shard_meta.progress.cdc_s3_last_file`
  advanced to
  `hybrid-vector-e2e-20260320d/cdc/124/465034883516071938/2026-03-20/CDC00000000000000000001.json`,
  and `manifest.fragments[0].f.property.count` reached `10001`.
  Verified:
  inverted lookup on `anchor_title_keep` returned `1`,
  inverted lookup on `post_index_anchor` returned `10001`,
  vector top1 for the base anchor returned `1`,
  vector top1 for the post-index delta anchor returned `10001`,
  and `EXPLAIN FORMAT='brief'` showed `cop[tici]` with `vector search`.
- `2026-03-20`: inspecting `playground:v1.16.2-feature.fts` showed that the
  playground-managed changefeed already uses `use-table-id-as-path=true`.
  Its queried sink config reported `date_separator = "none"` even though that
  parameter was not explicitly present in the sink URI.

Useful overrides:

```bash
TIDB_BINPATH=/path/to/tidb-server \
TIFLASH_BINPATH=/path/to/tiflash \
TICI_BINPATH=/path/to/tici-server \
TICDC_BINPATH=/path/to/cdc \
./hybrid_vector_e2e.sh all
```

TiUP refresh behavior:

- By default the helper removes local `playground:v1.16.2-feature.fts` and `cdc:nightly`
  component caches before reinstalling, so a re-pushed mirror binary is picked up.
- Set `REFRESH_TIUP_COMPONENTS=0` if you explicitly want to reuse the local cache.

By default the helper isolates itself from any existing local playground by using:

- `PORT_OFFSET=20000`
- `MINIO_PORT=29000`

If you want different ports, override `PORT_OFFSET`, `TIDB_PORT`, `PD_PORT`, `CDC_PORT`, or `MINIO_PORT`.

The script assumes the following default paths (override with environment variables):

- TiDB: `<repo_root>/bin/tidb-server`
- TiFlash: `<workspace>/tiflash-fts/cmake-build-codex-release/dbms/src/Server/tiflash`
- TiCI: `<workspace>/tici/target/release/tici-server`

Available commands:

```bash
./hybrid_vector_e2e.sh up
./hybrid_vector_e2e.sh prepare
./hybrid_vector_e2e.sh verify
./hybrid_vector_e2e.sh status
./hybrid_vector_e2e.sh down
```

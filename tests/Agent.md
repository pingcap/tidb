# Session Context (TiCI CI work)

## What was added/changed
- Added a dedicated PR pipeline for TiCI integration:
  - `tests/ci/ci/pipelines/pingcap/tidb/latest/pull_integration_tici_test.groovy`
  - `tests/ci/ci/pipelines/pingcap/tidb/latest/pod-pull_integration_tici_test.yaml`
- Updated integrationtest2 for TiCI defaults and MinIO:
  - `tests/integrationtest2/run-tests.sh`:
    - `TICI_BIN_DEFAULT=./third_bin/tici-server`
    - `MINIO_BUCKET=ticidefaultbucket`, `MINIO_PREFIX=tici_default_prefix/cdc`
    - S3 sink URI includes `protocol=canal-json`, `enable-tidb-extension=true`, `output-row-key=true`
    - added `MINIO_MC_BIN` and creates bucket if `mc` available
    - TiFlash startup when `tici` cases; TiCI starts after changefeed
  - `tests/integrationtest2/download_integration_test_binaries.sh`:
    - downloads tici tarball; prefers `tici-server`; symlink `tici`
    - downloads minio tarball
  - `tests/integrationtest2/README.md` updated defaults
- Added new single-container runner:
  - `tests/integrationtest2/tici/run-docker-cluster.sh`
  - `tests/integrationtest2/tici/Dockerfile`
  - `tests/integrationtest2/tici/config/README.md` and placeholder `tici.toml`
  - Removed `tests/integrationtest2/tici/run-local-tici.sh`

## How to run (single container)
```bash
# Requires: tidb-server binary path (TIDB_BIN) and a TiCI config file
TIDB_BIN=/path/to/tidb-server \
  tests/integrationtest2/tici/run-docker-cluster.sh \
    /path/to/tiflash \
    /path/to/tici \
    tests/integrationtest2/tici/config
```

Switches:
- `USE_TIFLASH_BUILD=1|0` (build TiFlash from source or use `TIFLASH_BIN`)
- `USE_TICI_BUILD=1|0` (build TiCI from source or use `TICI_BIN`)
- `DOWNLOAD_BINARIES=1|0` (download PD/TiKV/TiCDC/MinIO via integrationtest2 script)
- `TICI_CONFIG=/workspace/tici-config/<file>.toml` (defaults to `config/tici.toml`)

Example using prebuilt binaries:
```bash
TIDB_BIN=/path/to/tidb-server \
USE_TIFLASH_BUILD=0 TIFLASH_BIN=/path/to/tiflash \
USE_TICI_BUILD=0 TICI_BIN=/path/to/tici-server \
tests/integrationtest2/tici/run-docker-cluster.sh \
  /path/to/tiflash /path/to/tici tests/integrationtest2/tici/config
```

## Notes about TiFlash build
- TiFlash expects tici source at `contrib/tici` (FFI searchlib build).
- In the container, the script mounts the tici repo at `contrib/tici`.
- Submodules: script only pulls `contrib/grpc`, `contrib/protobuf`, `contrib/tiflash-proxy` if missing; skips private `contrib/tici` and `contrib/tiflash-proxy-next-gen`.

## Open items
- CI tags for tici/tiflash/ticdc/minio not finalized; pipeline uses placeholders.
- If you want a TiOP-based k8s flow, it needs tici component + MinIO + changefeed wiring (not done).

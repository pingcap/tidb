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

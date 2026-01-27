#!/usr/bin/env bash
# Prepare third-party binaries for TiCI integration tests.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BASE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

DOWNLOAD_SH="${DOWNLOAD_SH:-$SCRIPT_DIR/download.sh}"
if [ ! -f "$DOWNLOAD_SH" ]; then
    DOWNLOAD_SH="$BASE_DIR/download.sh"
fi
if [ ! -f "$DOWNLOAD_SH" ]; then
    DOWNLOAD_SH="$BASE_DIR/../download.sh"
fi
if [ ! -f "$DOWNLOAD_SH" ]; then
    echo "Error: download.sh not found. Set DOWNLOAD_SH to its path." >&2
    exit 1
fi

export OCI_ARTIFACT_HOST="${OCI_ARTIFACT_HOST:-hub-zot.pingcap.net/mirrors/hub}"

cd "$BASE_DIR"

sh "$DOWNLOAD_SH" --ticdc-new=master
sh "$DOWNLOAD_SH" --pd=master
sh "$DOWNLOAD_SH" --tikv=master
sh "$DOWNLOAD_SH" --tiflash=feature/fts
sh "$DOWNLOAD_SH" --minio=RELEASE.2025-07-23T15-54-02Z
sh "$DOWNLOAD_SH" --tici=master

echo "Binaries downloaded into: $BASE_DIR/third_bin"

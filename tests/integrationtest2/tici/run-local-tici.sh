#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
INTEGRATION_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)
IMAGE_NAME=${IMAGE_NAME:-tiflash-tici-local}
BRANCH=${BRANCH:-master}

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <tiflash_dir> <tici_dir>" >&2
  exit 1
fi

TIFLASH_SRC=$(cd "$1" && pwd)
TICI_SRC=$(cd "$2" && pwd)

if [ -d "${TIFLASH_SRC}/tiflash" ]; then
  TIFLASH_SRC="${TIFLASH_SRC}/tiflash"
fi
if [ -d "${TICI_SRC}/tici" ]; then
  TICI_SRC="${TICI_SRC}/tici"
fi

docker build -t "${IMAGE_NAME}" -f "${SCRIPT_DIR}/Dockerfile" "${SCRIPT_DIR}"

docker run -it --rm \
  -v "${INTEGRATION_DIR}:/workspace/integrationtest2" \
  -v "${TIFLASH_SRC}:/workspace/ci/tiflash_code/tiflash" \
  -v "${TICI_SRC}:/workspace/ci/tici_code/tici" \
  -v "${TICI_SRC}:/workspace/ci/tiflash_code/tiflash/contrib/tici" \
  "${IMAGE_NAME}" \
  bash -lc "
    set -euo pipefail

    export CC=clang
    export CXX=clang++

    TIFLASH_DIR=/workspace/ci/tiflash_code/tiflash
    TICI_DIR=/workspace/ci/tici_code/tici
    INTEGRATION_DIR=/workspace/integrationtest2

    if [ ! -d \"\$TIFLASH_DIR\" ] || [ ! -d \"\$TICI_DIR\" ]; then
      echo \"Missing tiflash or tici repo under /workspace/ci\" >&2
      exit 1
    fi
    if [ ! -d \"\$TIFLASH_DIR/contrib\" ]; then
      echo \"tiflash contrib directory not found under \$TIFLASH_DIR\" >&2
      exit 1
    fi

    cd \"\$TIFLASH_DIR\"
    if [ -d .git ]; then
      if [ -d contrib/grpc ] && [ -d contrib/protobuf ]; then
        echo \"Submodules already present; skip update.\"
      else
        git submodule sync --recursive
        git -c submodule.\"contrib/tici\".update=none \
          -c submodule.\"contrib/tiflash-proxy-next-gen\".update=none \
          submodule update --init --recursive --depth 1 --jobs 8 --recommend-shallow
      fi
    else
      echo \"WARNING: .git not found in tiflash repo; submodules may be missing.\" >&2
    fi

    rm -rf cmake-build-debug
    cmake --workflow --preset dev

    TIFLASH_BIN=\$(find \"\$TIFLASH_DIR\" -type f -name tiflash -perm -u+x -print -quit)
    if [ -z \"\$TIFLASH_BIN\" ]; then
      echo \"tiflash binary not found after build\" >&2
      exit 1
    fi

    cd \"\$TICI_DIR\"
    cargo build --features \"openssl-vendored failpoints\"
    TICI_BIN=\"\$TICI_DIR/target/debug/tici-server\"
    if [ ! -x \"\$TICI_BIN\" ]; then
      echo \"tici-server not found after build\" >&2
      exit 1
    fi

    cd \"\$INTEGRATION_DIR\"
    ./download_integration_test_binaries.sh \"${BRANCH}\"

    TIFLASH_BIN=\"\$TIFLASH_BIN\" \
    TICI_BIN=\"\$TICI_BIN\" \
    MINIO_BIN=\"./third_bin/minio\" \
    MINIO_MC_BIN=\"./third_bin/mc\" \
    ./run-tests.sh -t tici/tici_integration
  "

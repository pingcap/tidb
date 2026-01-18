#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
INTEGRATION_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)
IMAGE_NAME=${IMAGE_NAME:-tiflash-tici-local}
BRANCH=${BRANCH:-master}
USE_TIFLASH_BUILD=${USE_TIFLASH_BUILD:-1}
USE_TICI_BUILD=${USE_TICI_BUILD:-1}
DOWNLOAD_BINARIES=${DOWNLOAD_BINARIES:-1}

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <tiflash_dir> <tici_dir> <config_dir>" >&2
  exit 1
fi

TIFLASH_SRC=$(cd "$1" && pwd)
TICI_SRC=$(cd "$2" && pwd)
CONFIG_SRC=$(cd "$3" && pwd)

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
  -v "${CONFIG_SRC}:/workspace/tici-config" \
  "${IMAGE_NAME}" \
  bash -lc "
    set -euo pipefail

    export CC=clang
    export CXX=clang++
    if [ -f /root/.cargo/env ]; then
      source /root/.cargo/env
    else
      export PATH=\"/root/.cargo/bin:\$PATH\"
    fi
    if ! command -v cargo >/dev/null 2>&1; then
      echo \"cargo not found; installing rustup\" >&2
      curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
      source /root/.cargo/env
    fi

    TIFLASH_DIR=/workspace/ci/tiflash_code/tiflash
    TICI_DIR=/workspace/ci/tici_code/tici
    INTEGRATION_DIR=/workspace/integrationtest2
    CONFIG_DIR=/workspace/tici-config

    if [ ! -d \"\$TIFLASH_DIR\" ] || [ ! -d \"\$TICI_DIR\" ]; then
      echo \"Missing tiflash or tici repo under /workspace/ci\" >&2
      exit 1
    fi
    if [ ! -d \"\$TIFLASH_DIR/contrib\" ]; then
      echo \"tiflash contrib directory not found under \$TIFLASH_DIR\" >&2
      exit 1
    fi
    if [ ! -d \"\$CONFIG_DIR\" ]; then
      echo \"config directory not found: \$CONFIG_DIR\" >&2
      exit 1
    fi

    if [ -d \"\$TIFLASH_DIR/.git\" ]; then
      missing_submodules=()
      if [ ! -d \"\$TIFLASH_DIR/contrib/grpc\" ]; then
        missing_submodules+=(\"contrib/grpc\")
      fi
      if [ ! -d \"\$TIFLASH_DIR/contrib/protobuf\" ]; then
        missing_submodules+=(\"contrib/protobuf\")
      fi
      if [ ! -d \"\$TIFLASH_DIR/contrib/tiflash-proxy\" ]; then
        missing_submodules+=(\"contrib/tiflash-proxy\")
      fi

      if [ \${#missing_submodules[@]} -eq 0 ]; then
        echo \"Submodules already present; skip update.\"
      else
        cd \"\$TIFLASH_DIR\"
        git submodule sync --recursive
        git -c submodule.\"contrib/tici\".update=none \
          -c submodule.\"contrib/tiflash-proxy-next-gen\".update=none \
          submodule update --init --recursive --depth 1 --jobs 8 --recommend-shallow \"\${missing_submodules[@]}\"
      fi
    else
      echo \"WARNING: .git not found in tiflash repo; submodules may be missing.\" >&2
    fi

    if [ \"${USE_TIFLASH_BUILD}\" = \"1\" ]; then
      cd \"\$TIFLASH_DIR\"
      rm -rf cmake-build-debug
      cmake --workflow --preset dev
      TIFLASH_BIN=\$(find \"\$TIFLASH_DIR\" -type f -name tiflash -perm -u+x -print -quit)
      if [ -z \"\$TIFLASH_BIN\" ]; then
        echo \"tiflash binary not found after build\" >&2
        exit 1
      fi
    else
      TIFLASH_BIN=\"\${TIFLASH_BIN:-}\"
    fi

    if [ \"${USE_TICI_BUILD}\" = \"1\" ]; then
      cd \"\$TICI_DIR\"
      cargo build --features \"openssl-vendored failpoints\"
      TICI_BIN=\"\$TICI_DIR/target/debug/tici-server\"
      if [ ! -x \"\$TICI_BIN\" ]; then
        echo \"tici-server not found after build\" >&2
        exit 1
      fi
    else
      TICI_BIN=\"\${TICI_BIN:-}\"
    fi

    cd \"\$INTEGRATION_DIR\"
    if [ \"${DOWNLOAD_BINARIES}\" = \"1\" ]; then
      ./download_integration_test_binaries.sh \"${BRANCH}\"
    fi

    if [ -z \"\${TIDB_BIN:-}\" ]; then
      if [ -x \"\$INTEGRATION_DIR/third_bin/tidb-server\" ]; then
        TIDB_BIN=\"\$INTEGRATION_DIR/third_bin/tidb-server\"
      else
        echo \"TIDB_BIN is required (path to tidb-server)\" >&2
        exit 1
      fi
    fi

    if [ -z \"\${TIFLASH_BIN:-}\" ]; then
      if [ -x \"\$INTEGRATION_DIR/third_bin/tiflash\" ]; then
        TIFLASH_BIN=\"\$INTEGRATION_DIR/third_bin/tiflash\"
      else
        echo \"TIFLASH_BIN is required when USE_TIFLASH_BUILD=0\" >&2
        exit 1
      fi
    fi

    if [ -z \"\${TICI_BIN:-}\" ]; then
      if [ -x \"\$INTEGRATION_DIR/third_bin/tici-server\" ]; then
        TICI_BIN=\"\$INTEGRATION_DIR/third_bin/tici-server\"
      else
        echo \"TICI_BIN is required when USE_TICI_BUILD=0\" >&2
        exit 1
      fi
    fi

    if [ -z \"\${TICI_CONFIG:-}\" ]; then
      TICI_CONFIG=\"\$CONFIG_DIR/tici.toml\"
    fi
    if [ ! -f \"\$TICI_CONFIG\" ]; then
      echo \"tici config not found: \$TICI_CONFIG\" >&2
      exit 1
    fi

    TICI_ARGS=\"\${TICI_ARGS:-} --config \$TICI_CONFIG\"

    TIFLASH_BIN=\"\$TIFLASH_BIN\" \
    TICI_BIN=\"\$TICI_BIN\" \
    TICI_ARGS=\"\$TICI_ARGS\" \
    MINIO_BIN=\"\$INTEGRATION_DIR/third_bin/minio\" \
    MINIO_MC_BIN=\"\$INTEGRATION_DIR/third_bin/mc\" \
    ./run-tests.sh -b n -s \"\$TIDB_BIN\" -t tici/tici_integration
  "

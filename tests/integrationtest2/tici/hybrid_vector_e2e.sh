#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TIDB_ROOT=$(cd "${SCRIPT_DIR}/../../.." && pwd)
WORKSPACE_ROOT=$(cd "${TIDB_ROOT}/.." && pwd)
PYTHON_HELPER="${SCRIPT_DIR}/hybrid_vector_e2e.py"

OUTPUT_ROOT=${HYBRID_VECTOR_E2E_OUTPUT_ROOT:-"${SCRIPT_DIR}/output/hybrid-vector-e2e"}
LOG_DIR="${OUTPUT_ROOT}/log"
PLAYGROUND_LOG="${LOG_DIR}/playground.log"
MINIO_LOG="${LOG_DIR}/minio.log"
MINIO_PID_FILE="${OUTPUT_ROOT}/minio.pid"
CHANGEFEED_INFO_FILE="${OUTPUT_ROOT}/playground-changefeed.json"

PLAYGROUND_VERSION=${PLAYGROUND_VERSION:-"v1.16.2-feature.fts"}
PLAYGROUND_TAG=${PLAYGROUND_TAG:-"hybrid-vector-e2e"}
if [[ ! "${PLAYGROUND_TAG}" =~ ^[a-zA-Z0-9._-]+$ ]]; then
    echo "ERROR: PLAYGROUND_TAG contains unsafe characters: ${PLAYGROUND_TAG}" >&2
    exit 1
fi
PLAYGROUND_COMPONENT="playground:${PLAYGROUND_VERSION}"
TIUP_MIRROR=${TIUP_MIRROR:-"http://tiup.pingcap.net:8988"}

TIDB_HOST=${TIDB_HOST:-"127.0.0.1"}
PORT_OFFSET=${PORT_OFFSET:-20000}
BASE_TIDB_PORT=${BASE_TIDB_PORT:-4000}
BASE_PD_PORT=${BASE_PD_PORT:-2379}
BASE_CDC_PORT=${BASE_CDC_PORT:-8300}
BASE_MINIO_PORT=${BASE_MINIO_PORT:-9000}
BASE_MINIO_CONSOLE_PORT=${BASE_MINIO_CONSOLE_PORT:-9001}
TIDB_PORT=${TIDB_PORT:-$((BASE_TIDB_PORT + PORT_OFFSET))}
PD_PORT=${PD_PORT:-$((BASE_PD_PORT + PORT_OFFSET))}
CDC_PORT=${CDC_PORT:-$((BASE_CDC_PORT + PORT_OFFSET))}
MINIO_PORT=${MINIO_PORT:-$((BASE_MINIO_PORT + PORT_OFFSET))}
MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-$((BASE_MINIO_CONSOLE_PORT + PORT_OFFSET))}

S3_BUCKET=${S3_BUCKET:-"ticidefaultbucket"}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-"minioadmin"}
S3_SECRET_KEY=${S3_SECRET_KEY:-"minioadmin"}
S3_PREFIX=${S3_PREFIX:-"${PLAYGROUND_TAG}"}
S3_ENDPOINT=${S3_ENDPOINT:-"http://127.0.0.1:${MINIO_PORT}"}

TIDB_BINPATH=${TIDB_BINPATH:-"${TIDB_ROOT}/bin/tidb-server"}
TIFLASH_REPO=${TIFLASH_REPO:-"${WORKSPACE_ROOT}/tiflash-fts"}
TIFLASH_BINPATH=${TIFLASH_BINPATH:-"${TIFLASH_REPO}/cmake-build-codex-release/dbms/src/Server/tiflash"}
TIFLASH_BUILD_DIR=${TIFLASH_BUILD_DIR:-"${TIFLASH_REPO}/cmake-build-codex-release"}
TIFLASH_CONFIG_VERSION_HEADER=${TIFLASH_CONFIG_VERSION_HEADER:-"${TIFLASH_BUILD_DIR}/dbms/src/Common/config_version.h"}
TIFLASH_SEARCH_LIB_DIR=${TIFLASH_SEARCH_LIB_DIR:-"${TIFLASH_REPO}/cmake-build-codex-release/contrib/tici-search-lib"}
TIFLASH_PROXY_LIB_DIR=${TIFLASH_PROXY_LIB_DIR:-"${TIFLASH_REPO}/cmake-build-codex-release/contrib/tiflash-proxy-cmake/release"}
TICI_REPO=${TICI_REPO:-"${WORKSPACE_ROOT}/tici"}
TICI_BINPATH=${TICI_BINPATH:-"${TICI_REPO}/target/release/tici-server"}
TICDC_BINPATH=${TICDC_BINPATH:-""}
TIFLASH_ENGINE_VERSION=${TIFLASH_ENGINE_VERSION:-""}
TIFLASH_ENGINE_GIT_HASH=${TIFLASH_ENGINE_GIT_HASH:-""}

ROWS=${ROWS:-10000}
HNSW_M=${HNSW_M:-16}
HNSW_EF_CONSTRUCTION=${HNSW_EF_CONSTRUCTION:-200}
WITHOUT_MONITOR=${WITHOUT_MONITOR:-1}
REFRESH_TIUP_COMPONENTS=${REFRESH_TIUP_COMPONENTS:-1}

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
    printf '[%s] ERROR: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
    exit 1
}

ensure_dirs() {
    mkdir -p "${OUTPUT_ROOT}" "${LOG_DIR}"
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

playground_data_dir() {
    printf '%s\n' "${HOME}/.tiup/data/${PLAYGROUND_TAG}"
}

refresh_runtime_dsn() {
    local dsn_file dsn parsed_host parsed_port

    dsn_file="$(playground_data_dir)/dsn"
    [ -f "${dsn_file}" ] || return 1

    dsn=$(cat "${dsn_file}")
    parsed_host=$(printf '%s\n' "${dsn}" | sed -n -E 's#mysql://[^@]+@([^:]+):([0-9]+).*#\1#p' | head -n 1)
    parsed_port=$(printf '%s\n' "${dsn}" | sed -n -E 's#mysql://[^@]+@([^:]+):([0-9]+).*#\2#p' | head -n 1)
    [ -n "${parsed_host}" ] || return 1
    [ -n "${parsed_port}" ] || return 1

    TIDB_HOST="${parsed_host}"
    TIDB_PORT="${parsed_port}"
    return 0
}

resolve_runtime_port_from_process() {
    local role_dir=${1:-}
    local port_flag=${2:-}
    local pid cmd

    [ -n "${role_dir}" ] || return 1
    [ -n "${port_flag}" ] || return 1

    pid=$(pgrep -f "$(playground_data_dir)/${role_dir}" | head -n 1 || true)
    [ -n "${pid}" ] || return 1

    cmd=$(ps -p "${pid}" -o command= 2>/dev/null || true)
    [ -n "${cmd}" ] || return 1

    printf '%s\n' "${cmd}" | sed -n -E "s#.*${port_flag}[^:]+:([0-9]+).*#\\1#p" | head -n 1
}

refresh_runtime_ports() {
    local port

    refresh_runtime_dsn >/dev/null 2>&1 || true

    port=$(resolve_runtime_port_from_process "pd-0" "--client-urls=http://" || true)
    if [ -n "${port}" ]; then
        PD_PORT="${port}"
    fi

    port=$(resolve_runtime_port_from_process "cdc-0" "--addr=" || true)
    if [ -n "${port}" ]; then
        CDC_PORT="${port}"
    fi
}

run_cdc_cli() {
    if [ -n "${TICDC_BINPATH}" ]; then
        "${TICDC_BINPATH}" cli "$@"
    else
        tiup cdc:nightly cli "$@"
    fi
}

mysql_ready() {
    mysql \
        --host "${TIDB_HOST}" \
        --port "${TIDB_PORT}" \
        --user root \
        --batch \
        --raw \
        --skip-column-names \
        -e "select 1" >/dev/null 2>&1
}

wait_for_mysql() {
    local attempts=${1:-60}
    local interval=${2:-5}
    local i

    for ((i = 1; i <= attempts; i++)); do
        refresh_runtime_ports
        if mysql_ready; then
            log "TiDB is ready on ${TIDB_HOST}:${TIDB_PORT}"
            return 0
        fi
        sleep "${interval}"
    done
    die "TiDB did not become ready in time; see ${PLAYGROUND_LOG}"
}

wait_for_tiflash() {
    local attempts=${1:-60}
    local interval=${2:-5}
    local sql="SELECT COUNT(*) FROM information_schema.cluster_info WHERE type='tiflash';"
    local count
    local i

    for ((i = 1; i <= attempts; i++)); do
        refresh_runtime_ports
        count=$(mysql \
            --host "${TIDB_HOST}" \
            --port "${TIDB_PORT}" \
            --user root \
            --batch \
            --raw \
            --skip-column-names \
            -e "${sql}" 2>/dev/null || true)
        if [ "${count:-0}" -ge 1 ] 2>/dev/null; then
            log "TiFlash is registered in TiDB"
            return 0
        fi
        sleep "${interval}"
    done
    die "TiFlash did not register in time; see ${PLAYGROUND_LOG}"
}

platform_lib_ext() {
    case "$(uname -s)" in
        Darwin) printf 'dylib' ;;
        *)      printf 'so' ;;
    esac
}

resolve_search_lib_dir() {
    local ext candidate
    ext=$(platform_lib_ext)
    for candidate in \
        "${TIFLASH_SEARCH_LIB_DIR}" \
        "${TIFLASH_SEARCH_LIB_DIR%/release}" \
        "$(dirname "${TIFLASH_BINPATH}")"
    do
        if [ -n "${candidate}" ] && [ -f "${candidate}/libtici_search_lib.${ext}" ]; then
            printf '%s\n' "${candidate}"
            return 0
        fi
    done
    return 1
}

prepare_runtime_env() {
    local ext search_lib_dir
    ext=$(platform_lib_ext)
    search_lib_dir=$(resolve_search_lib_dir) || die "cannot find libtici_search_lib.${ext} from ${TIFLASH_SEARCH_LIB_DIR}"
    [ -f "${TIFLASH_PROXY_LIB_DIR}/libtiflash_proxy.${ext}" ] || die "cannot find libtiflash_proxy.${ext} in ${TIFLASH_PROXY_LIB_DIR}"

    if [ -n "${DYLD_LIBRARY_PATH:-}" ]; then
        export DYLD_LIBRARY_PATH="${search_lib_dir}:${TIFLASH_PROXY_LIB_DIR}:${DYLD_LIBRARY_PATH}"
    else
        export DYLD_LIBRARY_PATH="${search_lib_dir}:${TIFLASH_PROXY_LIB_DIR}"
    fi

    if [ -n "${LD_LIBRARY_PATH:-}" ]; then
        export LD_LIBRARY_PATH="${search_lib_dir}:${TIFLASH_PROXY_LIB_DIR}:${LD_LIBRARY_PATH}"
    else
        export LD_LIBRARY_PATH="${search_lib_dir}:${TIFLASH_PROXY_LIB_DIR}"
    fi
}

resolve_tiflash_engine_version() {
    if [ -n "${TIFLASH_ENGINE_VERSION}" ]; then
        printf '%s\n' "${TIFLASH_ENGINE_VERSION}"
        return 0
    fi
    if [ -f "${TIFLASH_CONFIG_VERSION_HEADER}" ]; then
        sed -n 's/^#define TIFLASH_VERSION "\(.*\)"/\1/p' "${TIFLASH_CONFIG_VERSION_HEADER}" | head -n 1
        return 0
    fi
    if [ -d "${TIFLASH_REPO}" ]; then
        git -C "${TIFLASH_REPO}" describe --tags --always 2>/dev/null | sed 's/^v//'
        return 0
    fi
    return 1
}

resolve_tiflash_engine_git_hash() {
    if [ -n "${TIFLASH_ENGINE_GIT_HASH}" ]; then
        printf '%s\n' "${TIFLASH_ENGINE_GIT_HASH}"
        return 0
    fi
    if [ -f "${TIFLASH_CONFIG_VERSION_HEADER}" ]; then
        sed -n 's/^#define TIFLASH_GIT_HASH "\(.*\)"/\1/p' "${TIFLASH_CONFIG_VERSION_HEADER}" | head -n 1
        return 0
    fi
    if [ -d "${TIFLASH_REPO}" ]; then
        git -C "${TIFLASH_REPO}" rev-parse HEAD 2>/dev/null
        return 0
    fi
    return 1
}

prepare_tiflash_binpath() {
    local engine_version=${1:-}
    local local_version="v0.0.0-local"
    local patched_binpath

    if [ -z "${engine_version}" ]; then
        printf '%s\n' "${TIFLASH_BINPATH}"
        return 0
    fi

    if ! grep -Fq "${local_version}" < <(strings "${TIFLASH_BINPATH}" 2>/dev/null); then
        printf '%s\n' "${TIFLASH_BINPATH}"
        return 0
    fi

    if [ "${#engine_version}" -ne "${#local_version}" ]; then
        die "cannot patch TiFlash version string ${local_version} -> ${engine_version}: length mismatch"
    fi

    require_cmd perl
    patched_binpath="${OUTPUT_ROOT}/tiflash-patched"
    cp "${TIFLASH_BINPATH}" "${patched_binpath}"
    SOURCE_VERSION="${local_version}" TARGET_VERSION="${engine_version}" \
        perl -0pi -e 's/\Q$ENV{SOURCE_VERSION}\E/$ENV{TARGET_VERSION}/g' "${patched_binpath}"

    if ! grep -Fq "${engine_version}" < <(strings "${patched_binpath}" 2>/dev/null); then
        die "failed to patch TiFlash binary version string to ${engine_version}"
    fi

    if [ "$(uname -s)" = "Darwin" ]; then
        require_cmd codesign
        codesign -f -s - "${patched_binpath}" >/dev/null
    fi

    log "patched TiFlash binary version string ${local_version} -> ${engine_version}" >&2
    printf '%s\n' "${patched_binpath}"
}

prepare_tiflash_launcher() {
    local tiflash_binpath=${1:-}
    local engine_version=${2:-}
    local engine_git_hash=${3:-}
    local wrapper wrapper_log

    if [ -z "${tiflash_binpath}" ]; then
        printf '%s\n' "${TIFLASH_BINPATH}"
        return 0
    fi

    if [ -z "${engine_version}" ] || [ -z "${engine_git_hash}" ]; then
        printf '%s\n' "${tiflash_binpath}"
        return 0
    fi

    wrapper="${OUTPUT_ROOT}/tiflash-launcher.sh"
    wrapper_log="${OUTPUT_ROOT}/tiflash-launcher.args.log"
    cat > "${wrapper}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
{
    printf '[%s] ' "\$(date '+%Y-%m-%d %H:%M:%S')"
    printf '%q ' "${tiflash_binpath}" "\$@" --engine-version "${engine_version}" --engine-git-hash "${engine_git_hash}"
    printf '\n'
} >> "${wrapper_log}"
exec "${tiflash_binpath}" "\$@" --engine-version "${engine_version}" --engine-git-hash "${engine_git_hash}"
EOF
    chmod +x "${wrapper}"
    log "wrapping TiFlash ${tiflash_binpath} with engine-version=${engine_version} engine-git-hash=${engine_git_hash}" >&2
    printf '%s\n' "${wrapper}"
}

build_tici_if_needed() {
    if [ -x "${TICI_BINPATH}" ]; then
        return 0
    fi
    [ -d "${TICI_REPO}" ] || die "TICI_REPO does not exist: ${TICI_REPO}"
    log "building tici-server in ${TICI_REPO}"
    (
        cd "${TICI_REPO}"
        make release
    )
    [ -x "${TICI_BINPATH}" ] || die "tici-server still missing after build: ${TICI_BINPATH}"
}

ensure_minio() {
    ensure_dirs

    if mc alias set localminio "http://127.0.0.1:${MINIO_PORT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null 2>&1; then
        :
    else
        log "starting local MinIO on ${MINIO_PORT}"
        nohup minio server "${OUTPUT_ROOT}/minio-data" \
            --address ":${MINIO_PORT}" \
            --console-address ":${MINIO_CONSOLE_PORT}" \
            > "${MINIO_LOG}" 2>&1 &
        echo $! > "${MINIO_PID_FILE}"

        local i
        for ((i = 1; i <= 30; i++)); do
            if mc alias set localminio "http://127.0.0.1:${MINIO_PORT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null 2>&1; then
                break
            fi
            sleep 2
        done
        mc alias set localminio "http://127.0.0.1:${MINIO_PORT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null 2>&1 \
            || die "failed to start or connect to MinIO; see ${MINIO_LOG}"
    fi

    if ! mc ls "localminio/${S3_BUCKET}" >/dev/null 2>&1; then
        mc mb "localminio/${S3_BUCKET}" >/dev/null
    fi
    mc rm --recursive --force "localminio/${S3_BUCKET}/${S3_PREFIX}" >/dev/null 2>&1 || true
}

ensure_tiup_components() {
    local playground_component_dir cdc_component_dir

    log "using TiUP mirror ${TIUP_MIRROR}"
    tiup mirror set "${TIUP_MIRROR}" >/dev/null
    if [ "${REFRESH_TIUP_COMPONENTS}" = "1" ]; then
        playground_component_dir="${HOME}/.tiup/components/playground/${PLAYGROUND_VERSION}"
        cdc_component_dir="${HOME}/.tiup/components/cdc/nightly"
        log "refreshing TiUP components for ${PLAYGROUND_COMPONENT} and cdc:nightly"
        rm -rf "${playground_component_dir}" "${cdc_component_dir}"
    fi
    tiup install "${PLAYGROUND_COMPONENT}" >/dev/null
    if [ -z "${TICDC_BINPATH}" ]; then
        tiup install "cdc:nightly" >/dev/null
    fi
}

cleanup_previous_playground() {
    local playground_data_dir="${HOME}/.tiup/data/${PLAYGROUND_TAG}"

    pkill -f "tiup playground:.*--tag ${PLAYGROUND_TAG}" >/dev/null 2>&1 || true
    pkill -f "tiup-playground .*--tag ${PLAYGROUND_TAG}" >/dev/null 2>&1 || true
    if [ -d "${playground_data_dir}" ]; then
        pkill -f "${playground_data_dir}" >/dev/null 2>&1 || true
    fi
    tiup clean "${PLAYGROUND_TAG}" >/dev/null 2>&1 || true
    if [ -d "${playground_data_dir}" ]; then
        rm -rf "${playground_data_dir}"
    fi
}

start_playground() {
    ensure_dirs
    require_cmd tiup
    require_cmd mysql
    require_cmd minio
    require_cmd mc
    require_cmd python3

    [ -x "${TIDB_BINPATH}" ] || die "tidb-server not found: ${TIDB_BINPATH}"
    [ -x "${TIFLASH_BINPATH}" ] || die "tiflash not found: ${TIFLASH_BINPATH}"
    [ -f "${PYTHON_HELPER}" ] || die "python helper not found: ${PYTHON_HELPER}"

    build_tici_if_needed
    prepare_runtime_env
    ensure_minio
    ensure_tiup_components
    cleanup_previous_playground

    local engine_version engine_git_hash effective_tiflash_binpath tiflash_launcher_binpath
    engine_version=$(resolve_tiflash_engine_version || true)
    engine_git_hash=$(resolve_tiflash_engine_git_hash || true)
    effective_tiflash_binpath=$(prepare_tiflash_binpath "${engine_version}")
    tiflash_launcher_binpath=$(prepare_tiflash_launcher "${effective_tiflash_binpath}" "${engine_version}" "${engine_git_hash}")

    local args=(
        "--mode" "tidb-fts"
        "--tag" "${PLAYGROUND_TAG}"
        "--host" "${TIDB_HOST}"
        "--db" "1"
        "--db.host" "${TIDB_HOST}"
        "--db.port" "${TIDB_PORT}"
        "--tiflash" "1"
        "--ticdc" "1"
        "--ticdc.host" "${TIDB_HOST}"
        "--ticdc.port" "${CDC_PORT}"
        "--pd" "1"
        "--pd.host" "${TIDB_HOST}"
        "--pd.port" "${PD_PORT}"
        "--tici.worker" "1"
        "--port-offset" "${PORT_OFFSET}"
        "--s3.bucket" "${S3_BUCKET}"
        "--s3.endpoint" "${S3_ENDPOINT}"
        "--s3.access_key" "${S3_ACCESS_KEY}"
        "--s3.secret_key" "${S3_SECRET_KEY}"
        "--db.binpath" "${TIDB_BINPATH}"
        "--tiflash.binpath" "${tiflash_launcher_binpath}"
        "--tici.binpath" "${TICI_BINPATH}"
    )

    if [ -n "${TICDC_BINPATH}" ]; then
        args+=("--ticdc.binpath" "${TICDC_BINPATH}")
    fi
    if [ "${WITHOUT_MONITOR}" = "1" ]; then
        args+=("--without-monitor")
    fi

    log "starting playground ${PLAYGROUND_COMPONENT} with tag ${PLAYGROUND_TAG}"
    nohup env \
        TICDC_NEWARCH=true \
        DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-}" \
        LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" \
        tiup "${PLAYGROUND_COMPONENT}" "${args[@]}" \
        > "${PLAYGROUND_LOG}" 2>&1 &

    wait_for_mysql
    wait_for_tiflash
    wait_for_playground_changefeed
}

discover_playground_changefeed() {
    local server_addr=${1:-}
    local expected_prefix=${2:-}
    local list_output ids id query_output

    [ -n "${server_addr}" ] || return 1
    [ -n "${expected_prefix}" ] || return 1

    list_output=$(run_cdc_cli changefeed list --server="${server_addr}" 2>/dev/null) || return 1
    ids=$(printf '%s' "${list_output}" | python3 -c '
import json
import sys

feeds = json.load(sys.stdin)
for feed in feeds:
    print(feed.get("id", ""))
')
    [ -n "${ids}" ] || return 1

    for id in ${ids}; do
        [ -n "${id}" ] || continue
        query_output=$(run_cdc_cli changefeed query --server="${server_addr}" --changefeed-id="${id}" 2>/dev/null) || continue
        if printf '%s' "${query_output}" | EXPECTED_PREFIX="${expected_prefix}" python3 -c '
import json
import os
import sys

data = json.load(sys.stdin)
sink_uri = data.get("sink_uri", "")
state = data.get("state", "")
date_separator = data.get("config", {}).get("sink", {}).get("date_separator", "")
expected_prefix = os.environ["EXPECTED_PREFIX"]

if not sink_uri.startswith(expected_prefix):
    sys.exit(1)
if "use-table-id-as-path=true" not in sink_uri:
    sys.exit(1)
if state != "normal":
    sys.exit(1)

json.dump(
    {
        "id": data.get("id", ""),
        "state": state,
        "sink_uri": sink_uri,
        "date_separator": date_separator,
        "creator_version": data.get("creator_version", ""),
    },
    sys.stdout,
)
' > "${CHANGEFEED_INFO_FILE}.tmp"; then
            mv "${CHANGEFEED_INFO_FILE}.tmp" "${CHANGEFEED_INFO_FILE}"
            return 0
        fi
        rm -f "${CHANGEFEED_INFO_FILE}.tmp"
    done
    return 1
}

wait_for_playground_changefeed() {
    local i
    local server_addr expected_prefix changefeed_id sink_uri date_separator creator_version

    refresh_runtime_ports
    server_addr="http://127.0.0.1:${CDC_PORT}"
    expected_prefix="s3://${S3_BUCKET}/${S3_PREFIX}/cdc"

    for ((i = 1; i <= 30; i++)); do
        if discover_playground_changefeed "${server_addr}" "${expected_prefix}"; then
            changefeed_id=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["id"])' "${CHANGEFEED_INFO_FILE}")
            sink_uri=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["sink_uri"])' "${CHANGEFEED_INFO_FILE}")
            date_separator=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1])).get("date_separator", ""))' "${CHANGEFEED_INFO_FILE}")
            creator_version=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1])).get("creator_version", ""))' "${CHANGEFEED_INFO_FILE}")
            log "using playground-managed changefeed ${changefeed_id}"
            log "changefeed sink: ${sink_uri}"
            log "changefeed date_separator=${date_separator:-<unknown>} creator_version=${creator_version:-<unknown>}"
            return 0
        fi
        sleep 2
    done
    run_cdc_cli changefeed list --server="${server_addr}" >/dev/null 2>&1 \
        || die "TiCDC is not ready on port ${CDC_PORT}"
    die "playground-managed changefeed was not ready or did not match ${expected_prefix}; see ${PLAYGROUND_LOG}"
}

run_python_prepare() {
    require_cmd python3
    refresh_runtime_ports
    python3 "${PYTHON_HELPER}" \
        --host "${TIDB_HOST}" \
        --port "${TIDB_PORT}" \
        --database test \
        --table hybrid_vector_docs \
        --index-name idx_hybrid_vector \
        --rows "${ROWS}" \
        --hnsw-m "${HNSW_M}" \
        --hnsw-ef-construction "${HNSW_EF_CONSTRUCTION}" \
        prepare
}

run_python_verify() {
    require_cmd python3
    refresh_runtime_ports
    python3 "${PYTHON_HELPER}" \
        --host "${TIDB_HOST}" \
        --port "${TIDB_PORT}" \
        --database test \
        --table hybrid_vector_docs \
        --index-name idx_hybrid_vector \
        --rows "${ROWS}" \
        verify
}

run_python_recall() {
    require_cmd python3
    refresh_runtime_ports
    python3 "${PYTHON_HELPER}" \
        --host "${TIDB_HOST}" \
        --port "${TIDB_PORT}" \
        --database test \
        --table hybrid_vector_docs \
        --index-name idx_hybrid_vector \
        --rows "${ROWS}" \
        recall
}

run_python_bench() {
    require_cmd python3
    refresh_runtime_ports
    python3 "${PYTHON_HELPER}" \
        --host "${TIDB_HOST}" \
        --port "${TIDB_PORT}" \
        --database test \
        --table hybrid_vector_docs \
        --index-name idx_hybrid_vector \
        --rows "${ROWS}" \
        --bench-queries "${BENCH_QUERIES:-100}" \
        --bench-k "${BENCH_K:-10}" \
        bench
}

show_status() {
    tiup status || true
    tiup "${PLAYGROUND_COMPONENT}" display --tag "${PLAYGROUND_TAG}" || true
}

stop_minio_if_owned() {
    if [ -f "${MINIO_PID_FILE}" ]; then
        local pid
        pid=$(cat "${MINIO_PID_FILE}" 2>/dev/null || true)
        if [ -n "${pid}" ] && kill -0 "${pid}" >/dev/null 2>&1; then
            kill "${pid}" >/dev/null 2>&1 || true
        fi
        rm -f "${MINIO_PID_FILE}"
    fi
}

down() {
    cleanup_previous_playground
    stop_minio_if_owned
}

usage() {
    cat <<EOF
Usage: $0 <command>

Commands:
  up        Start MinIO, TiUP playground, and wait for the playground-managed TiCDC changefeed
  prepare   Prepare schema, create hybrid index, load data through CDC, and add delta data
  verify    Verify inverted/vector results and planner path
  recall    Run recall@K test (brute-force vs TiCI vector search)
  bench     Run latency benchmark (vector search, brute-force, inverted)
  all       Run up + prepare + verify + recall
  status    Print TiUP status and playground display output
  down      Clean playground and stop the MinIO started by this script

Common env overrides:
  PLAYGROUND_VERSION=${PLAYGROUND_VERSION}
  PLAYGROUND_TAG=${PLAYGROUND_TAG}
  TIDB_BINPATH=${TIDB_BINPATH}
  TIFLASH_BINPATH=${TIFLASH_BINPATH}
  TICI_BINPATH=${TICI_BINPATH}
  TICDC_BINPATH=${TICDC_BINPATH:-<empty>}
  PORT_OFFSET=${PORT_OFFSET}
  TIDB_PORT=${TIDB_PORT}
  PD_PORT=${PD_PORT}
  CDC_PORT=${CDC_PORT}
  MINIO_PORT=${MINIO_PORT}
  S3_ENDPOINT=${S3_ENDPOINT}
  TIFLASH_ENGINE_VERSION=${TIFLASH_ENGINE_VERSION:-<auto>}
  TIFLASH_ENGINE_GIT_HASH=${TIFLASH_ENGINE_GIT_HASH:-<auto>}
  TIFLASH_SEARCH_LIB_DIR=${TIFLASH_SEARCH_LIB_DIR}
  TIFLASH_PROXY_LIB_DIR=${TIFLASH_PROXY_LIB_DIR}
  ROWS=${ROWS}
EOF
}

main() {
    local command=${1:-}
    case "${command}" in
        up)
            start_playground
            ;;
        prepare)
            run_python_prepare
            ;;
        verify)
            run_python_verify
            ;;
        recall)
            run_python_recall
            ;;
        bench)
            run_python_bench
            ;;
        all)
            start_playground
            run_python_prepare
            run_python_verify
            run_python_recall
            ;;
        status)
            show_status
            ;;
        down)
            down
            ;;
        help|-h|--help|"")
            usage
            ;;
        *)
            usage
            die "unknown command: ${command}"
            ;;
    esac
}

main "$@"

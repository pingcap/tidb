#!/usr/bin/env bash
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

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
runner=${TXN_FILE_COMPOSE_RUNNER:-${script_dir}/run.sh}
if [[ ! -x ${runner} && -x ${script_dir}/docker-compose/run.sh ]]; then
    runner=${script_dir}/docker-compose/run.sh
fi
fake_bin=$(mktemp -d "${TMPDIR:-/tmp}/tidb-txn-file-compose-test.XXXXXX")
output="${fake_bin}/output.log"
trap 'rm -rf -- "${fake_bin}"' EXIT

cat >"${fake_bin}/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} == compose && ${2:-} == version ]]; then
    printf '2.27.1\n'
    exit 0
fi
if [[ ${1:-} == inspect ]]; then
    format=${3:-}
    container=${4:-}
    case "${format}:${container}" in
        *State.Status*:*minio-init) printf 'exited\n' ;;
        *State.ExitCode*:*minio-init) exit 1 ;;
        *State.Status*:*bootstrap-tidb|*State.Status*:*create-keyspace|*State.Status*:*activate-tidb) printf 'exited\n' ;;
        *State.ExitCode*:*bootstrap-tidb|*State.ExitCode*:*create-keyspace|*State.ExitCode*:*activate-tidb) printf '0\n' ;;
        *State.Status*:*) printf 'running\n' ;;
        *State.Health*:*) printf 'healthy\n' ;;
        *State.ExitCode*:*) printf '0\n' ;;
    esac
    exit 0
fi

for arg in "$@"; do
    if [[ ${arg} == ps ]]; then
        printf '%s\n' "container-${!#}"
        exit 0
    fi
done
exit 0
EOF
chmod +x "${fake_bin}/docker"

if PATH="${fake_bin}:${PATH}" COMPOSE_PROJECT_NAME=txn-file-runner-test COMPOSE_WAIT_ATTEMPTS=1 \
    "${runner}" >"${output}" 2>&1; then
    printf 'run.sh unexpectedly accepted an unknown one-shot service exit code\n' >&2
    exit 1
fi
if ! grep -Fq 'minio-init exited with exit code unknown' "${output}"; then
    printf 'run.sh did not report the unknown exit code:\n' >&2
    cat "${output}" >&2
    exit 1
fi

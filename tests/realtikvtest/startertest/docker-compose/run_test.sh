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
build_ready="${fake_bin}/build-ready"
real_git=$(command -v git)
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
        *State.ExitCode*:*minio-init)
            if [[ ${FAKE_UNKNOWN_EXIT:-0} == 1 ]]; then
                exit 1
            fi
            printf '0\n'
            ;;
        *State.Status*:*bootstrap-tidb|*State.Status*:*create-keyspace|*State.Status*:*activate-tidb) printf 'exited\n' ;;
        *State.ExitCode*:*bootstrap-tidb|*State.ExitCode*:*create-keyspace|*State.ExitCode*:*activate-tidb) printf '0\n' ;;
        *State.Status*:*) printf 'running\n' ;;
        *State.Health*:*tikv-3)
            [[ -z ${FAKE_BUILD_READY_FILE:-} ]] || : >"${FAKE_BUILD_READY_FILE}"
            printf 'healthy\n'
            ;;
        *State.Health*:*) printf 'healthy\n' ;;
        *State.ExitCode*:*) printf '0\n' ;;
    esac
    exit 0
fi

if [[ ${FAKE_REQUIRE_SOURCE_METADATA:-0} == 1 ]]; then
    is_build=0
    has_commit=0
    has_branch=0
    has_release_version=0
    has_status=0
    for arg in "$@"; do
        [[ ${arg} == build ]] && is_build=1
        [[ ${arg} == "SOURCE_COMMIT=${EXPECTED_SOURCE_COMMIT}" ]] && has_commit=1
        [[ ${arg} == "SOURCE_BRANCH=${EXPECTED_SOURCE_BRANCH}" ]] && has_branch=1
        [[ ${arg} == "SOURCE_RELEASE_VERSION=${EXPECTED_SOURCE_RELEASE_VERSION}" ]] && has_release_version=1
        [[ ${arg} == "SOURCE_STATUS=${EXPECTED_SOURCE_STATUS}" ]] && has_status=1
    done
    if [[ ${is_build} == 1 && (${has_commit} != 1 || ${has_branch} != 1 || ${has_release_version} != 1 || ${has_status} != 1) ]]; then
        printf 'compose build did not receive source metadata\n' >&2
        exit 1
    fi
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

cat >"${fake_bin}/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ ${FAKE_REQUIRE_METADATA_ORDER:-0} == 1 && ! -e ${FAKE_BUILD_READY_FILE} ]]; then
    printf 'source metadata captured before build readiness\n' >&2
    exit 1
fi
exec "${REAL_GIT}" "$@"
EOF
chmod +x "${fake_bin}/git"

if PATH="${fake_bin}:${PATH}" REAL_GIT="${real_git}" FAKE_UNKNOWN_EXIT=1 \
    COMPOSE_PROJECT_NAME=txn-file-runner-test COMPOSE_WAIT_ATTEMPTS=1 \
    "${runner}" >"${output}" 2>&1; then
    printf 'run.sh unexpectedly accepted an unknown one-shot service exit code\n' >&2
    exit 1
fi
if ! grep -Fq 'minio-init exited with exit code unknown' "${output}"; then
    printf 'run.sh did not report the unknown exit code:\n' >&2
    cat "${output}" >&2
    exit 1
fi

repo_root=$(CDPATH= cd -- "${script_dir}/../../../.." && pwd)
expected_commit=$(git -C "${repo_root}" rev-parse HEAD)
expected_branch=$(git -C "${repo_root}" rev-parse --abbrev-ref HEAD)
expected_release_version=$(cd "${repo_root}" && NEXT_GEN=1 ./build/compute-tidb-release-version.sh)
expected_status=$(git -C "${repo_root}" status --porcelain=v1)
if ! PATH="${fake_bin}:${PATH}" FAKE_REQUIRE_SOURCE_METADATA=1 \
    FAKE_REQUIRE_METADATA_ORDER=1 FAKE_BUILD_READY_FILE="${build_ready}" REAL_GIT="${real_git}" \
    EXPECTED_SOURCE_COMMIT="${expected_commit}" EXPECTED_SOURCE_BRANCH="${expected_branch}" \
    EXPECTED_SOURCE_RELEASE_VERSION="${expected_release_version}" EXPECTED_SOURCE_STATUS="${expected_status}" \
    COMPOSE_PROJECT_NAME=txn-file-runner-metadata-test COMPOSE_WAIT_ATTEMPTS=1 \
    "${runner}" >"${output}" 2>&1; then
    printf 'run.sh did not pass source metadata to the image build:\n' >&2
    cat "${output}" >&2
    exit 1
fi

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

usage() {
	cat <<'EOF'
Usage: tools/check/failpoint-go-test.sh <package_dir> [go test args...]

Run `go test` in a TiDB package with failpoints enabled and automatic cleanup.
If `-tags` is not provided, the script uses `-tags=intest,deadlock`.

Examples:
  tools/check/failpoint-go-test.sh pkg/planner/core -run TestName
  tools/check/failpoint-go-test.sh pkg/executor -run TestName -count=1
  tools/check/failpoint-go-test.sh pkg/executor -tags=intest,deadlock,nextgen -run TestName
  tools/check/failpoint-go-test.sh pkg/expression
EOF
}

if [ $# -eq 0 ]; then
	usage >&2
	exit 1
fi

case "$1" in
	-h|--help)
		usage
		exit 0
		;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
package_dir="$1"
shift

if [ "${1:-}" = "--" ]; then
	shift
fi

tags="intest,deadlock"
has_explicit_tags=0
go_test_args=()
while [ $# -gt 0 ]; do
	case "$1" in
		-tags|--tags)
			if [ $# -lt 2 ]; then
				echo "Missing value for $1" >&2
				exit 1
			fi
			tags="$2"
			has_explicit_tags=1
			go_test_args+=("$1" "$2")
			shift 2
			;;
		-tags=*|--tags=*)
			tags="${1#*=}"
			has_explicit_tags=1
			go_test_args+=("$1")
			shift
			;;
		*)
			go_test_args+=("$1")
			shift
			;;
	esac
done

if [[ "${package_dir}" = /* ]]; then
	package_path="${package_dir}"
else
	package_path="${repo_root}/${package_dir}"
fi

if [ ! -d "${package_path}" ]; then
	echo "Package directory does not exist: ${package_dir}" >&2
	exit 1
fi

package_path="$(cd "${package_path}" && pwd)"

case "${package_path}" in
	"${repo_root}"|"${repo_root}"/*)
		;;
	*)
		echo "Package directory must be inside the repository: ${package_dir}" >&2
		exit 1
		;;
esac

enabled=0
cleanup() {
	if [ "${enabled}" -eq 1 ]; then
		(
			cd "${repo_root}"
			make failpoint-disable
		)
	fi
}
trap cleanup EXIT INT TERM

(
	cd "${repo_root}"
	make failpoint-enable
)
enabled=1

(
	cd "${package_path}"
	if [ "${has_explicit_tags}" -eq 1 ]; then
		go test "${go_test_args[@]}"
	else
		go test -tags="${tags}" "${go_test_args[@]}"
	fi
)

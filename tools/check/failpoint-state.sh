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
Usage: tools/check/failpoint-state.sh <enable|disable> [go|bazel]

Serialize failpoint enable/disable operations in a repository and avoid
parallel state corruption from overlapping failpoint-ctl runs.

Arguments:
  enable|disable  Desired failpoint state transition.
  go|bazel        Backend to use (default: go).

Environment:
  FAILPOINT_LOCK_WAIT_SECONDS  Lock wait timeout (default: 600).
  FAILPOINT_LOG_CHANNEL        Log channel: stderr|stdout|both (default: stderr).
  FAILPOINT_LOG_COLOR          Color mode: auto|always|never (default: auto).
  BAZEL_GLOBAL_CONFIG          Optional bazel global flags.
  BAZEL_CMD_CONFIG             Optional bazel command flags.
EOF
}

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
	usage >&2
	exit 1
fi

action="$1"
backend="${2:-go}"

case "${action}" in
	enable|disable)
		;;
	*)
		usage >&2
		exit 1
		;;
esac

case "${backend}" in
	go|bazel)
		;;
	*)
		usage >&2
		exit 1
		;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"

if git_dir="$(git -C "${repo_root}" rev-parse --git-dir 2>/dev/null)"; then
	if [[ "${git_dir}" != /* ]]; then
		git_dir="${repo_root}/${git_dir}"
	fi
	state_dir="${git_dir}/.failpoint-state"
else
	state_dir="${repo_root}/.git/.failpoint-state"
fi
lock_dir="${state_dir}/lock"
lock_pid_file="${lock_dir}/pid"
refcount_file="${state_dir}/refcount"
lock_wait_seconds="${FAILPOINT_LOCK_WAIT_SECONDS:-600}"
log_channel="${FAILPOINT_LOG_CHANNEL:-stderr}"
log_color_mode="${FAILPOINT_LOG_COLOR:-auto}"

case "${log_channel}" in
	stdout|stderr|both)
		;;
	*)
		echo "invalid FAILPOINT_LOG_CHANNEL: ${log_channel} (expected stdout|stderr|both)" >&2
		exit 1
		;;
esac

case "${log_color_mode}" in
	auto|always|never)
		;;
	*)
		echo "invalid FAILPOINT_LOG_COLOR: ${log_color_mode} (expected auto|always|never)" >&2
		exit 1
		;;
esac

mkdir -p "${state_dir}"

read_refcount() {
	if [ ! -f "${refcount_file}" ]; then
		echo 0
		return
	fi
	local raw
	raw="$(tr -d '[:space:]' < "${refcount_file}")"
	if [[ ! "${raw}" =~ ^[0-9]+$ ]]; then
		echo "invalid failpoint refcount in ${refcount_file}: ${raw}" >&2
		exit 1
	fi
	echo "${raw}"
}

write_refcount() {
	local count="$1"
	printf '%s\n' "${count}" > "${refcount_file}"
}

log_state() {
	local message="$1"
	case "${log_channel}" in
		stdout)
			emit_log_to_fd 1 "${message}"
			;;
		stderr)
			emit_log_to_fd 2 "${message}"
			;;
		both)
			emit_log_to_fd 1 "${message}"
			emit_log_to_fd 2 "${message}"
			;;
	esac
}

emit_log_to_fd() {
	local fd="$1"
	local message="$2"
	if [ "${fd}" -eq 1 ]; then
		printf '[failpoint-state] %s\n' "${message}"
	else
		printf '[failpoint-state] %s\n' "${message}" >&2
	fi
}

use_color_for_fd() {
	local fd="$1"
	case "${log_color_mode}" in
		always)
			return 0
			;;
		never)
			return 1
			;;
		auto)
			[ -z "${NO_COLOR:-}" ] || return 1
			[ "${TERM:-}" != "dumb" ] || return 1
			[ -t "${fd}" ]
			return
			;;
	esac
}

format_decision_for_fd() {
	local fd="$1"
	local message="$2"
	local line="[DECISION] >>> ${message} <<<"
	local color_reset=$'\033[0m'
	local color_decision
	case "${message}" in
		decision=run-*)
			color_decision=$'\033[1;32m'
			;;
		decision=skip-*)
			color_decision=$'\033[1;33m'
			;;
		*)
			color_decision=$'\033[1;36m'
			;;
	esac
	if use_color_for_fd "${fd}"; then
		printf '%s%s%s' "${color_decision}" "${line}" "${color_reset}"
	else
		printf '%s' "${line}"
	fi
}

log_decision() {
	local message="$1"
	local formatted
	case "${log_channel}" in
		stdout)
			formatted="$(format_decision_for_fd 1 "${message}")"
			emit_log_to_fd 1 "${formatted}"
			;;
		stderr)
			formatted="$(format_decision_for_fd 2 "${message}")"
			emit_log_to_fd 2 "${formatted}"
			;;
		both)
			formatted="$(format_decision_for_fd 1 "${message}")"
			emit_log_to_fd 1 "${formatted}"
			formatted="$(format_decision_for_fd 2 "${message}")"
			emit_log_to_fd 2 "${formatted}"
			;;
	esac
}

collect_target_dirs() {
	local dir
	target_dirs=()
	while IFS= read -r -d '' dir; do
		case "$(basename "${dir}")" in
			.git|.idea|tools)
				continue
				;;
		esac
		target_dirs+=("${dir}")
	done < <(find "${repo_root}" -mindepth 1 -maxdepth 1 -type d -print0)
	if [ "${#target_dirs[@]}" -eq 0 ]; then
		echo "no target directories found for failpoint-ctl under ${repo_root}" >&2
		exit 1
	fi
}

run_go_failpoint_ctl() {
	local mode="$1"
	collect_target_dirs
	"${repo_root}/tools/bin/failpoint-ctl" "${mode}" "${target_dirs[@]}"
}

run_bazel_failpoint_ctl() {
	local mode="$1"
	local -a bazel_global_args=()
	local -a bazel_cmd_args=()
	if [ -n "${BAZEL_GLOBAL_CONFIG:-}" ]; then
		# shellcheck disable=SC2206
		bazel_global_args=(${BAZEL_GLOBAL_CONFIG})
	fi
	if [ -n "${BAZEL_CMD_CONFIG:-}" ]; then
		# shellcheck disable=SC2206
		bazel_cmd_args=(${BAZEL_CMD_CONFIG})
	fi
	collect_target_dirs
	bazel "${bazel_global_args[@]}" run "${bazel_cmd_args[@]}" @com_github_pingcap_failpoint//failpoint-ctl:failpoint-ctl -- "${mode}" "${target_dirs[@]}"
}

run_failpoint_ctl() {
	local mode="$1"
	case "${backend}" in
		go)
			run_go_failpoint_ctl "${mode}"
			;;
		bazel)
			run_bazel_failpoint_ctl "${mode}"
			;;
	esac
}

release_lock() {
	rm -f "${lock_pid_file}" 2>/dev/null || true
	rmdir "${lock_dir}" 2>/dev/null || true
}

acquire_lock() {
	local waited=0
	while ! mkdir "${lock_dir}" 2>/dev/null; do
		if [ -f "${lock_pid_file}" ]; then
			local holder
			holder="$(tr -d '[:space:]' < "${lock_pid_file}" || true)"
			if [ -n "${holder}" ] && ! kill -0 "${holder}" 2>/dev/null; then
				rm -f "${lock_pid_file}" 2>/dev/null || true
				rmdir "${lock_dir}" 2>/dev/null || true
				continue
			fi
		fi
		if [ "${waited}" -ge "${lock_wait_seconds}" ]; then
			echo "timed out waiting for failpoint lock after ${lock_wait_seconds}s: ${lock_dir}" >&2
			exit 1
		fi
		sleep 1
		waited=$((waited + 1))
	done
	printf '%s\n' "$$" > "${lock_pid_file}"
}

trap release_lock EXIT INT TERM
acquire_lock

refcount="$(read_refcount)"
log_state "action=${action} backend=${backend} log_channel=${log_channel} log_color=${log_color_mode} refcount=${refcount}"
case "${action}" in
	enable)
		if [ "${refcount}" -eq 0 ]; then
			log_decision "decision=run-enable reason=refcount-is-zero"
			run_failpoint_ctl enable
		else
			log_decision "decision=skip-enable reason=already-enabled"
		fi
		new_refcount=$((refcount + 1))
		write_refcount "${new_refcount}"
		log_state "new_refcount=${new_refcount}"
		;;
	disable)
		if [ "${refcount}" -le 0 ]; then
			write_refcount 0
			log_decision "decision=skip-disable reason=refcount-non-positive new_refcount=0"
			exit 0
		fi
		new_refcount=$((refcount - 1))
		if [ "${new_refcount}" -eq 0 ]; then
			log_decision "decision=run-disable reason=refcount-reaches-zero"
			run_failpoint_ctl disable
		else
			log_decision "decision=skip-disable reason=still-referenced"
		fi
		write_refcount "${new_refcount}"
		log_state "new_refcount=${new_refcount}"
		;;
esac

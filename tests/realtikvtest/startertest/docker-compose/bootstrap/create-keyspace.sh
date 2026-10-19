#!/bin/sh
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

set -eu

PD_ENDPOINT=http://pd:2379
KEYSPACE_NAME=startertest
GC_CONFIG=gc_management_type=keyspace_level
system_output=/tmp/system-keyspace.$$.out
keyspace_output=/tmp/startertest-keyspace.$$.out
error_output=/tmp/keyspace.$$.err

cleanup() {
    rm -f "${system_output}" "${keyspace_output}" "${error_output}"
}
trap cleanup 0

show_keyspace() {
    /pd-ctl -u "${PD_ENDPOINT}" keyspace show name "$1"
}

has_json_field() {
    output_file=$1
    field=$2
    expected_value=$3
    compact=$(tr -d '\r\n\t ' < "${output_file}")
    case "${compact}" in
        *"\"${field}\":\"${expected_value}\""*) return 0 ;;
        *) return 1 ;;
    esac
}

is_enabled_keyspace() {
    has_json_field "$2" name "$1" &&
        has_json_field "$2" state ENABLED
}

is_valid_startertest_keyspace() {
    has_json_field "${keyspace_output}" name "${KEYSPACE_NAME}" &&
        has_json_field "${keyspace_output}" state ENABLED &&
        has_json_field "${keyspace_output}" gc_management_type keyspace_level
}

attempt=1
while [ "${attempt}" -le 120 ]; do
    if /pd-ctl -u "${PD_ENDPOINT}" member >"${error_output}" 2>&1; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "${attempt}" -gt 120 ]; then
    echo "PD did not become ready at ${PD_ENDPOINT}" >&2
    cat "${error_output}" >&2 || true
    exit 1
fi

attempt=1
while [ "${attempt}" -le 120 ]; do
    if show_keyspace SYSTEM >"${system_output}" 2>"${error_output}" &&
        is_enabled_keyspace SYSTEM "${system_output}"; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "${attempt}" -gt 120 ]; then
    echo "SYSTEM keyspace validation failed: expected state ENABLED" >&2
    cat "${error_output}" "${system_output}" >&2 || true
    exit 1
fi

show_keyspace "${KEYSPACE_NAME}" >"${keyspace_output}" 2>"${error_output}" || true
if has_json_field "${keyspace_output}" name "${KEYSPACE_NAME}"; then
    echo "keyspace ${KEYSPACE_NAME} already exists"
else
    /pd-ctl -u "${PD_ENDPOINT}" keyspace create "${KEYSPACE_NAME}" --config "${GC_CONFIG}"
fi

attempt=1
while [ "${attempt}" -le 120 ]; do
    if show_keyspace "${KEYSPACE_NAME}" >"${keyspace_output}" 2>"${error_output}" &&
        is_valid_startertest_keyspace; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "${attempt}" -gt 120 ]; then
    echo "startertest keyspace validation failed: expected state ENABLED and gc_management_type keyspace_level" >&2
    cat "${error_output}" "${keyspace_output}" >&2 || true
    exit 1
fi

if ! show_keyspace SYSTEM >"${system_output}" 2>"${error_output}" ||
    ! is_enabled_keyspace SYSTEM "${system_output}"; then
    echo "SYSTEM keyspace revalidation failed: expected state ENABLED" >&2
    cat "${error_output}" "${system_output}" >&2 || true
    exit 1
fi

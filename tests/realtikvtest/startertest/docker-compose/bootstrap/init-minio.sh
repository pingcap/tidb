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

MINIO_ENDPOINT=http://minio:9000
MINIO_USER=minioadmin
MINIO_PASSWORD=minioadmin
BUCKET=tidbcloud-local-dfs

if command -v mc >/dev/null 2>&1; then
    mc_bin=$(command -v mc)
elif command -v mcli >/dev/null 2>&1; then
    mc_bin=$(command -v mcli)
else
    echo "mc or mcli is required to initialize MinIO" >&2
    exit 1
fi

attempt=1
while [ "${attempt}" -le 120 ]; do
    if "${mc_bin}" alias set local "${MINIO_ENDPOINT}" "${MINIO_USER}" "${MINIO_PASSWORD}" >/dev/null 2>&1 &&
        "${mc_bin}" ready local >/dev/null 2>&1; then
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
if [ "${attempt}" -gt 120 ]; then
    echo "MinIO did not become ready at ${MINIO_ENDPOINT}" >&2
    exit 1
fi

"${mc_bin}" mb --ignore-existing "local/${BUCKET}"
"${mc_bin}" stat "local/${BUCKET}"

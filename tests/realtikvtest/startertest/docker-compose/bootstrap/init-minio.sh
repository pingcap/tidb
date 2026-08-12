#!/bin/sh
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

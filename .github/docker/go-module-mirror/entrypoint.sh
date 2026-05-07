#!/usr/bin/env bash
set -euo pipefail

workspace="${MIRROR_WORKSPACE:-/workspace}"
base_runfiles="/opt/tidb-mirror/runfiles"
runfiles="$(mktemp -d)"

cp -a "${base_runfiles}/." "${runfiles}/"

runfiles_workspace="$(
  find "${runfiles}" -mindepth 2 -maxdepth 2 -name DEPS.bzl -exec dirname {} \; | head -n 1
)"
if [[ -z "${runfiles_workspace}" ]]; then
  echo "failed to locate mirror runfiles workspace" >&2
  exit 1
fi

for path in \
  DEPS.bzl \
  go.mod \
  go.sum \
  pkg/parser/go.mod \
  pkg/parser/go.sum
do
  if [[ -f "${workspace}/${path}" ]]; then
    mkdir -p "${runfiles_workspace}/$(dirname "${path}")"
    cp "${workspace}/${path}" "${runfiles_workspace}/${path}"
  fi
done

if [[ -d "${workspace}/build/patches" ]]; then
  mkdir -p "${runfiles_workspace}/build"
  rm -rf "${runfiles_workspace}/build/patches"
  cp -a "${workspace}/build/patches" "${runfiles_workspace}/build/patches"
fi

export RUNFILES_DIR="${runfiles}"
export TEST_WORKSPACE="$(basename "${runfiles_workspace}")"
exec /opt/tidb-mirror/mirror --mirror --upload "$@"

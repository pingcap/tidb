#!/usr/bin/env bash
set -euo pipefail

workspace="${MIRROR_WORKSPACE:-/workspace}"
runfiles="$(mktemp -d)"
runfiles_workspace="${runfiles}/tidb"

if [[ ! -d "${workspace}" ]]; then
  echo "workspace ${workspace} does not exist" >&2
  exit 1
fi

mkdir -p "${runfiles_workspace}/bin"
ln -sf /usr/local/go/bin/go "${runfiles_workspace}/bin/go"

link_runfile() {
  local path="$1"
  if [[ -f "${workspace}/${path}" ]]; then
    mkdir -p "${runfiles_workspace}/$(dirname "${path}")"
    ln -sf "${workspace}/${path}" "${runfiles_workspace}/${path}"
  fi
}

link_runfile DEPS.bzl
link_runfile go.mod
link_runfile go.sum
link_runfile pkg/parser/go.mod
link_runfile pkg/parser/go.sum

if [[ -d "${workspace}/build/patches" ]]; then
  mkdir -p "${runfiles_workspace}/build"
  ln -sfn "${workspace}/build/patches" "${runfiles_workspace}/build/patches"
fi

export RUNFILES_DIR="${runfiles}"
export TEST_WORKSPACE="tidb"
exec /opt/tidb-mirror/mirror --mirror --upload "$@"

#!/usr/bin/env bash
#
# Checks out the Go sources this port is transcreated FROM, at the exact
# versions this repository pins, so a behavioural claim can be READ rather
# than recalled.
#
# Why this exists: client-go is not vendored in the TiDB tree and there is no
# Go toolchain on every workstation, so it is easy to write "Go does X"
# from memory. That has been wrong in practice -- two contracts were ported
# stricter than Go's real behaviour and only a source read caught it (see
# `docs/go-oracle.md`). TiDB's own `pkg/**` is already in this checkout and
# needs nothing; this fetches the piece that is missing.
#
# The checkout is deliberately OUTSIDE version control (`.oracle/` is
# gitignored): it is a reference, not a dependency, and must never be
# committed into TiDB.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ORACLE_DIR="${REPO_ROOT}/rust/.oracle"
CLIENT_GO_DIR="${ORACLE_DIR}/client-go"

# The pin is read from go.mod rather than written here, so this can never
# drift from what TiDB actually builds against.
pin=$(awk '/github.com\/tikv\/client-go\/v2 /{print $2; exit}' "${REPO_ROOT}/go.mod")
if [[ -z "${pin}" ]]; then
  echo "could not find the client-go pin in go.mod" >&2
  exit 1
fi
# A Go pseudo-version `vX.Y.Z-0.<timestamp>-<commit>` names the commit in its
# last dash-separated field; a plain tag names itself.
commit="${pin##*-}"

mkdir -p "${ORACLE_DIR}"
if [[ ! -d "${CLIENT_GO_DIR}/.git" ]]; then
  echo "cloning client-go (blobless) into ${CLIENT_GO_DIR}"
  git clone --filter=blob:none --no-checkout \
    https://github.com/tikv/client-go.git "${CLIENT_GO_DIR}"
fi
cd "${CLIENT_GO_DIR}"
if ! git cat-file -e "${commit}^{commit}" 2>/dev/null; then
  git fetch --filter=blob:none origin
fi
git checkout --quiet "${commit}"
echo "client-go at ${pin}"
echo "  commit:  $(git rev-parse --short HEAD)"
echo "  subject: $(git log -1 --pretty=%s)"
echo "  path:    ${CLIENT_GO_DIR}"

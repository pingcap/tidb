#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc. Licensed under Apache License 2.0.
set -euo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO_DIR=$(cd -- "$SCRIPT_DIR/../../../.." && pwd -P)
BUNDLE_DIR=${1:?Usage: bash package.sh /new/absolute/bundle-directory}
[[ $BUNDLE_DIR == /* && ! -e $BUNDLE_DIR ]] || { printf 'Choose a new absolute bundle directory\n' >&2; exit 1; }
mkdir -p "$BUNDLE_DIR/src/sqlbench" "$BUNDLE_DIR/src/benchdata"
cd "$REPO_DIR"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$BUNDLE_DIR/sqlbench" ./tests/ftse2e/cmd/sqlbench
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o "$BUNDLE_DIR/fts-e2e.test" ./tests/ftse2e
cp "$SCRIPT_DIR/h124.sh" "$SCRIPT_DIR/sample.py" "$SCRIPT_DIR/README.md" "$BUNDLE_DIR/"
cp tests/ftse2e/cmd/sqlbench/*.go "$BUNDLE_DIR/src/sqlbench/"
cp tests/ftse2e/benchdata/*.go "$BUNDLE_DIR/src/benchdata/"
git rev-parse HEAD > "$BUNDLE_DIR/source-revision.txt"
git status --porcelain > "$BUNDLE_DIR/source-status.txt"
go version -m "$BUNDLE_DIR/sqlbench" > "$BUNDLE_DIR/build-info.txt"
cd "$BUNDLE_DIR"
if command -v sha256sum >/dev/null; then
    sha256sum h124.sh sample.py sqlbench fts-e2e.test > SHA256SUMS
else
    shasum -a 256 h124.sh sample.py sqlbench fts-e2e.test > SHA256SUMS
fi
printf 'Linux amd64 bundle: %s\n' "$BUNDLE_DIR"

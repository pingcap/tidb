#!/usr/bin/env bash
set -euo pipefail

# go mod tidy do not support symlink
cd -P .

GO111MODULE=on go mod tidy
git diff --quiet

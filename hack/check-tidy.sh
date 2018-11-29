#!/usr/bin/env bash
set -euo pipefail

GO111MODULE=on go mod tidy
git diff --quiet

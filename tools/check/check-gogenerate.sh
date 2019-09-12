#!/usr/bin/env bash
set -euo pipefail

go generate ./...
git diff --quiet

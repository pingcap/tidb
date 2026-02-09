#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
PROTO_FILE="forwardpb.proto"

GOPATH=$(go env GOPATH)
if [ -z "$GOPATH" ]; then
  echo "GOPATH is not set" >&2
  exit 1
fi

export PATH="$GOPATH/bin:$PATH"

go install github.com/gogo/protobuf/protoc-gen-gofast@latest

if ! command -v goimports >/dev/null 2>&1; then
  go install golang.org/x/tools/cmd/goimports@latest
fi

cd "$SCRIPT_DIR"
protoc \
  -I. \
  --gofast_out=plugins=grpc,paths=source_relative:. \
  "$PROTO_FILE"

if ls "$SCRIPT_DIR"/*.pb.go >/dev/null 2>&1; then
  goimports -w "$SCRIPT_DIR"/*.pb.go
  # Keep generated code consistent with the repo's formatting rule (nogo gofmt).
  # Bazel/nogo expects `any` instead of `interface{}` in generated sources.
  gofmt -w -r 'interface{} -> any' "$SCRIPT_DIR"/*.pb.go
fi

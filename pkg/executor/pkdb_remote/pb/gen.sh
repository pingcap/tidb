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

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest
command -v goimports >/dev/null 2>&1 || go install golang.org/x/tools/cmd/goimports@latest
command -v protoc >/dev/null 2>&1 || { echo "protoc not found in PATH"; exit 1; }

cd "$SCRIPT_DIR"
protoc \
  -I. \
  --go-grpc_opt=use_generic_streams_experimental=false \
  --go_out=paths=source_relative:. \
  --go-grpc_out=paths=source_relative:. \
  --go-vtproto_out=paths=source_relative,features=marshal+unmarshal+size+pool:. \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.FieldType \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.Param \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.TxnContext \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.SessionSnapshot \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.TableVersion \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.PlanCacheInfo \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.RemoteRequest \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.ColumnInfo \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.ChunkData \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.Row \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.Warning \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.DMLResult \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.RemoteExecFeedback \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.BatchRemoteRequest \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.StreamResponse \
  --go-vtproto_opt=pool=github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb.BatchRemoteResponse \
  "$PROTO_FILE"

if ls "$SCRIPT_DIR"/*.pb.go >/dev/null 2>&1; then
  goimports -w "$SCRIPT_DIR"/*.pb.go
  gofmt -w -r 'interface{} -> any' "$SCRIPT_DIR"/*.pb.go
fi

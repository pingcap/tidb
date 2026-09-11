#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Apply the existing Bazel etcd compatibility patch through a temporary Go
# module file and patched module copy. The module cache and repository
# go.mod/go.sum stay unchanged. Requires patch.
# Run from the TiDB repository root, for example:
#   bash build/go-with-etcd-patch.sh test -mod=readonly ./pkg/ddl -run TestFullTextParserConfigFromJob -count=1
set -euo pipefail

case "${1:-}" in
    build|test|run|vet) ;;
    *) echo "Usage: $0 {build|test|run|vet} [go arguments...]" >&2; exit 2 ;;
esac

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
etcd_dir=$(go list -mod=readonly -m -f '{{.Dir}}' go.etcd.io/etcd/server/v3)
source_file=etcdserver/api/v3rpc/grpc.go
if [[ ! -f "$etcd_dir/$source_file" ]]; then
    echo 'Download the etcd module first: go mod download go.etcd.io/etcd/server/v3' >&2
    exit 1
fi

patch_dir=$(mktemp -d "${TMPDIR:-/tmp}/tidb-etcd-patch.XXXXXX")
trap 'rm -rf -- "$patch_dir"' EXIT
cp -R "$etcd_dir" "$patch_dir/etcd"
chmod -R u+w "$patch_dir/etcd"
patch -s -d "$patch_dir/etcd" -p1 < "$script_dir/patches/io_etcd_go_etcd_server_v3.patch"
cp go.mod "$patch_dir/go.mod"
cp go.sum "$patch_dir/go.sum"
go mod edit -modfile="$patch_dir/go.mod" -replace="go.etcd.io/etcd/server/v3=$patch_dir/etcd"

go_command=$1
shift
go "$go_command" -modfile="$patch_dir/go.mod" "$@"

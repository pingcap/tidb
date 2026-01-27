#!/bin/bash
#
# Copyright 2025 PingCAP, Inc.
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

cd ../../ || exit 1
echo "building tidb-server..."
make
echo "build successfully"

cd - || exit 1

echo "Starting TiUP Playground in the background..."
playground_ver="${TIUP_PLAYGROUND_VERSION:-nightly}"
tidb_binpath="${TIDB_BINPATH:-../../bin/tidb-server}"

args=(playground "$playground_ver" --db=1 --kv=1 --tiflash=1 --db.binpath="$tidb_binpath" --db.config=./config.toml)
if [[ -n "${TIKV_BINPATH:-}" ]]; then
  args+=(--kv.binpath="$TIKV_BINPATH")
fi
if [[ -n "${TIFLASH_BINPATH:-}" ]]; then
  args+=(--tiflash.binpath="$TIFLASH_BINPATH")
fi

tiup "${args[@]}"

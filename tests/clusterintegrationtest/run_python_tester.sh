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

set -euo pipefail

CURRENT_DIR="$(dirname "$0")"
source $CURRENT_DIR/_include.sh

echo "+ Starting run python testers"

# prepare datasets
cd datasets
check_and_prepare_datasets
cd ..

start_tidb
wait_for_tidb
wait_for_tiflash

print_versions

echo "+ Running /root/python_testers/vector_recall.py"
uv run ./python_testers/vector_recall.py || { stop_tiup; }

stop_tiup
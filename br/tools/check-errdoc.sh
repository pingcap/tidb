#!/bin/bash
# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

cd -P .

cp errors.toml /tmp/errors.toml.before
./tools/bin/errdoc-gen --source . --module github.com/pingcap/tidb/br --output errors.toml
diff -q errors.toml /tmp/errors.toml.before

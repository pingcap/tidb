#! /usr/bin/env bash
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

# It need TCP ports:
# - pd: 2379, 2380, 2381, 2383, 2384
# - tikv: 20160, 20161, 20162, 20180, 20181, 20182
# - tikv-worker: 19000
function main() {
    local test_suite="$1"
    local suite_timeout="${2:-40m}"

    local self_dir=$(realpath $(dirname "${BASH_SOURCE[0]}"))
    "${self_dir}/bootstrap-test-with-cluster.sh" go test ./tests/realtikvtest/${test_suite} -v --tags=intest,nextgen -with-real-tikv -timeout ${suite_timeout}
}

main "$@"

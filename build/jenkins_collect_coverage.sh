#!/bin/bash
#
# Copyright 2023 PingCAP, Inc.
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

# This test is used to test compatible for BR restore.
# It will download backup data from internal file server.
# And make sure these backup data can restore through newly BR tools to newly cluster.

set -o pipefail

coverage_report=./bazel-out/_coverage/_coverage_report.dat
junit_report=./bazel.xml

go install github.com/hawkingrei/bazel_collect@latest
mkdir -p test_coverage
if [ -f "${coverage_report}" ]; then
    cp "${coverage_report}" ./coverage.dat
else
    : > ./coverage.dat
    echo "warning: coverage report ${coverage_report} not found, created empty coverage.dat" >&2
fi
if [ -f "${junit_report}" ]; then
    mv "${junit_report}" test_coverage/bazel.xml
else
    echo "warning: junit report ${junit_report} not found, skipping junit artifact collection" >&2
fi

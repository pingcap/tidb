#!/bin/bash
#
# Copyright 2022 PingCAP, Inc.
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

# Debug-only: run full coverage with cache disabled to compare results.
echo "=== nocache full coverage ==="
bazel --nohome_rc coverage --config=ci --repository_cache=/share/.cache/bazel-repository-cache \
	--instrument_test_targets --instrumentation_filter=//pkg/...,//br/...,//dumpling/... \
	--@io_bazel_rules_go//go/config:cover_format=go_cover --define gotags=deadlock,intest \
	--nocache_test_results --noremote_accept_cached \
	--remote_cache= --remote_upload_local_results=false \
	--test_keep_going=true \
	-- //... -//cmd/... -//tests/graceshutdown/... \
	-//tests/globalkilltest/... -//tests/readonlytest/... -//tests/realtikvtest/...
EXIT_STATUS=$?

# collect the junit and coverage report
bazel_collect
mkdir -p test_coverage
mv bazel.xml test_coverage/bazel.xml

output_path="$(bazel info output_path || true)"
echo "nocache output_path: ${output_path}"
testlogs_root="$(readlink -f bazel-testlogs || true)"
bazel_out_parent="$(dirname "$(dirname "${testlogs_root}")")"
echo "testlogs roots under bazel-out parent: ${bazel_out_parent}"
for root in "${bazel_out_parent}"/k8-fastbuild*/testlogs; do
	if [ -d "$root/br/pkg/rtree/rtree_test" ]; then
		echo "TestLogRanges shard mapping ($root):"
		for f in "$root"/br/pkg/rtree/rtree_test/shard_*_of_8/test.log; do
			grep -nH "TestLogRanges" "$f" 2>/dev/null || true
		done
		echo "Coverage entries for rtree/logging.go ($root):"
		for f in "$root"/br/pkg/rtree/rtree_test/shard_*_of_8/coverage.dat; do
			echo "---- $f"
			grep -nH "github.com/pingcap/tidb/br/pkg/rtree/logging.go" "$f" 2>/dev/null || true
		done
	fi
	if [ -d "$root/pkg/lightning/backend/local/local_test" ]; then
		echo "Coverage entries for lightning local.go ($root):"
		for f in "$root"/pkg/lightning/backend/local/local_test/shard_*_of_50/coverage.dat; do
			grep -nHE "github.com/pingcap/tidb/pkg/lightning/backend/local/local.go:(350|367|370)" "$f" 2>/dev/null || true
		done
	fi
done

exit ${EXIT_STATUS}

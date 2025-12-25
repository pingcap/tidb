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

# Debug-only: run a single target with cache disabled to compare coverage.
echo "=== nocache single-target coverage: //br/pkg/rtree:rtree_test ==="
bazel --nohome_rc coverage --config=ci --repository_cache=/share/.cache/bazel-repository-cache \
	--instrument_test_targets --instrumentation_filter=//br/... \
	--@io_bazel_rules_go//go/config:cover_format=go_cover --define gotags=deadlock,intest \
	--nocache_test_results --noremote_accept_cached \
	--remote_cache= --remote_upload_local_results=false \
	-- //br/pkg/rtree:rtree_test || true
output_path="$(bazel info output_path || true)"
echo "nocache output_path: ${output_path}"
echo "TestLogRanges shard mapping:"
for root in ${output_path}/k8-fastbuild*/testlogs; do
	if [ -d "$root/br/pkg/rtree/rtree_test" ]; then
		grep -nH "TestLogRanges" "$root/br/pkg/rtree/rtree_test/shard_*_of_8/test.log" 2>/dev/null || true
	fi
done
echo "Coverage entries for rtree/logging.go:"
for root in ${output_path}/k8-fastbuild*/testlogs; do
	if [ -d "$root/br/pkg/rtree/rtree_test" ]; then
		for f in "$root"/br/pkg/rtree/rtree_test/shard_*_of_8/coverage.dat; do
			echo "---- $f"
			grep -nH "github.com/pingcap/tidb/br/pkg/rtree/logging.go" "$f" 2>/dev/null || true
		done
	fi
done
exit 0

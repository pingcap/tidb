#!/usr/bin/env bash
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

GOROOT=$(bazel run @io_bazel_rules_go//go -- env GOROOT)
cd ${GOROOT}

gosrc_md5=()
gosrc_md5+=("src/internal/runtime/maps/map.go a29531cd3447fd3c90ceabfde5a08921")
gosrc_md5+=("src/internal/runtime/maps/table.go 1ff4f281722eb83ac7d64ae0453e9718")
gosrc_md5+=("src/internal/abi/map_swiss.go 7ef614406774c5be839e63aea0225b00")
gosrc_md5+=("src/internal/abi/type.go d0caafb471a5b971854ca6426510608c")

for x in "${gosrc_md5[@]}"; do
	x=($x)
	src="${x[0]}"
	md5="${x[1]}"
	echo "Checking ${src}"
	if [ $(md5sum "${src}" | cut -d' ' -f1) != "${md5}" ]; then
		echo "Unexpect checksum for ${src}"
		exit -1
	fi
done

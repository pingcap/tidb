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
GOVERSION=$(bazel run @io_bazel_rules_go//go -- env GOVERSION)

gosrc_md5=()
case "${GOVERSION}" in
	go1.25.*)
		gosrc_md5+=("src/internal/runtime/maps/map.go a29531cd3447fd3c90ceabfde5a08921")
		gosrc_md5+=("src/internal/runtime/maps/table.go 1ff4f281722eb83ac7d64ae0453e9718")
		gosrc_md5+=("src/internal/abi/map_swiss.go 7ef614406774c5be839e63aea0225b00")
		gosrc_md5+=("src/internal/abi/type.go d0caafb471a5b971854ca6426510608c")
		;;
	go1.26.*)
		gosrc_md5+=("src/internal/runtime/maps/map.go 3e68f31ef3238e389fdac4054fc8704e")
		gosrc_md5+=("src/internal/runtime/maps/table.go 30d2976dac7b7033b62b6362e2b6d557")
		gosrc_md5+=("src/internal/abi/map.go 4e5fb78cb88f2edc634fa040f99dcacc")
		gosrc_md5+=("src/internal/abi/type.go 3bd3a7cef23f68f1bc98b8a40551a425")
		;;
	*)
		echo "Unsupported Go version for ABI check: ${GOVERSION}. Expected go1.25.x or go1.26.x."
		exit 1
		;;
esac

for x in "${gosrc_md5[@]}"; do
	x=($x)
	src="${x[0]}"
	md5="${x[1]}"
	echo "Checking ${src}"
	src_path="${GOROOT}/${src}"
	if [ ! -f "${src_path}" ]; then
		echo "Missing source file: ${src_path}"
		exit 1
	fi
	actual_md5=$(md5sum "${src_path}" | cut -d' ' -f1)
	if [ "${actual_md5}" != "${md5}" ]; then
		echo "Unexpected checksum for ${src}: expected ${md5}, got ${actual_md5}"
		exit 1
	fi
done

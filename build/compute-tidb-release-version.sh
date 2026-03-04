#!/bin/bash
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

set -o errexit
set -o nounset
set -o pipefail

# Source release version:
# - Prefer caller override from env (for reproducible local/CI builds).
# - Otherwise fallback to git describe (legacy/default behavior).
tidb_release_version=${TIDB_RELEASE_VERSION:-$(git describe --tags --dirty --always)}

# Non-nextgen keeps historical behavior exactly as-is.
if [[ "${NEXT_GEN:-0}" != "1" ]]; then
    echo "${tidb_release_version}"
    exit 0
fi

# If input already matches nextgen release format, reuse it directly.
# This happens when callers already provide a nextgen version in TIDB_RELEASE_VERSION,
# for example release jobs/tagged builds like v26.3.1 (instead of legacy v8.x.y).
if echo "${tidb_release_version}" | grep -Eq '^v(2[5-9]|[3-9][0-9])\.(1[0-2]|[1-9])\.[0-9]+([+-].*)?$'; then
    echo "${tidb_release_version}"
    exit 0
fi

# Nextgen fallback: synthesize date-based major/minor and append commit identity.
# This branch is used when NEXT_GEN=1, and TIDB_RELEASE_VERSION is still
# legacy/non-nextgen (for example v8.5.0 or plain git-describe output on
# non-release branches).
tidb_x_release_version_by_date="v$(date '+%y').$(date '+%m' | sed 's/^0//').0-$(git describe --always --dirty --exclude '*')"
echo "${tidb_x_release_version_by_date}"

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

set -o errexit
set -o nounset
set -o pipefail

TiDB_RELEASE_VERSION=$(git describe --tags --dirty --always)
TiDB_BUILD_UTCTIME=$(date -u '+%Y-%m-%d %H:%M:%S')
TIDB_GIT_HASH=$(git rev-parse HEAD)
TIDB_GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
TIDB_EDITION=${TIDB_EDITION:-Community}
TIDB_ENTERPRISE_EXTENSION_GIT_HASH=""
if [ -a "extension/enterprise/.git" ]; then
    TIDB_ENTERPRISE_EXTENSION_GIT_HASH=$(cd extension/enterprise && git rev-parse HEAD)
fi

cat <<EOF
STABLE_TiDB_RELEASE_VERSION ${TiDB_RELEASE_VERSION}
STABLE_TiDB_BUILD_UTCTIME ${TiDB_BUILD_UTCTIME}
STABLE_TIDB_GIT_HASH  ${TIDB_GIT_HASH}
STABLE_TIDB_GIT_BRANCH ${TIDB_GIT_BRANCH}
STABLE_TIDB_EDITION ${TIDB_EDITION}
STABLE_TIDB_ENTERPRISE_EXTENSION_GIT_HASH ${TIDB_ENTERPRISE_EXTENSION_GIT_HASH}
EOF

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

unset CDPATH

# The root of the build/dist directory
TIDB_ROOT="$(cd "$(dirname "${BASH_SOURCE}")/../.." && pwd -P)"

TIDB_OUTPUT_SUBPATH="${TIDB_OUTPUT_SUBPATH:-_output/local}"
TIDB_OUTPUT="${TIDB_ROOT}/${TIDB_OUTPUT_SUBPATH}"
TIDB_OUTPUT_BINPATH="${TIDB_OUTPUT}/bin"

# This controls rsync compression. Set to a value > 0 to enable rsync
# compression for build container
TIDB_RSYNC_COMPRESS="${TIDB_RSYNC_COMPRESS:-0}"

# Set no_proxy for localhost if behind a proxy, otherwise,
# the connections to localhost in scripts will time out
export no_proxy=127.0.0.1,localhost
export bazel_version="5.3.1"
# This is a symlink to binaries for "this platform", e.g. build tools.
THIS_PLATFORM_BIN="${TIDB_ROOT}/_output/bin"

source "${TIDB_ROOT}/build/lib/util.sh"
source "${TIDB_ROOT}/build/lib/logging.sh"

function tidb::readlinkdashf {
  # run in a subshell for simpler 'cd'
  (
    if [[ -d "$1" ]]; then # This also catch symlinks to dirs.
      cd "$1"
      pwd -P
    else
      cd "$(dirname "$1")"
      local f
      f=$(basename "$1")
      if [[ -L "$f" ]]; then
        readlink "$f"
      else
        echo "$(pwd -P)/${f}"
      fi
    fi
  )
}

tidb::realpath() {
  if [[ ! -e "$1" ]]; then
    echo "$1: No such file or directory" >&2
    return 1
  fi
  tidb::readlinkdashf "$1"
}


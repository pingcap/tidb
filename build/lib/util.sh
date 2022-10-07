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

tidb::util::sortable_date() {
  date "+%Y%m%d-%H%M%S"
}

#tidb::util::ensure-gnu-sed
# Determines which sed binary is gnu-sed on linux/darwin
#
# Sets:
#  SED: The name of the gnu-sed binary
#
function tidb::util::ensure-gnu-sed {
  if LANG=C sed --help 2>&1 | grep -q GNU; then
    SED="sed"
  elif which gsed &>/dev/null; then
    SED="gsed"
  else
    tidb::log::error "Failed to find GNU sed as sed or gsed. If you are on Mac: brew install gnu-sed." >&2
    return 1
  fi
}

function tidb::util::ensure-homebrew {
  if ! brew --version > /dev/null ; then
    echo "install homebrew..."
    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
  fi
}

function tidb::util::ensure-homebrew-bazel {
  if ! brew ls --versions bazel > /dev/null && ! brew ls --versions bazelisk ; then
    echo "install bazel..."
    brew install bazelisk
  fi
}

function tidb::util::ensure-bazel {
  if ! bazel version > /dev/null && ! brew ls --versions bazelisk; then
    echo "Please install bazel by being compiled from code."
  fi
}

# Some useful colors.
if [[ -z "${color_start-}" ]]; then
  declare -r color_start="\033["
  declare -r color_red="${color_start}0;31m"
  declare -r color_yellow="${color_start}0;33m"
  declare -r color_green="${color_start}0;32m"
  declare -r color_norm="${color_start}0m"
fi

# ex: ts=2 sw=2 et filetype=sh

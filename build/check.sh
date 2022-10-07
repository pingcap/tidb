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

export TIDB_ROOT=$(dirname "${BASH_SOURCE}")/..
source "${TIDB_ROOT}/build/lib/init.sh"

tidb::util::ensure-gnu-sed

if  ! bazel version && ! bazelisk version |grep $bazel_version >/dev/null ; then
    tidb::log::info "We suggest you to use bazel $bazel_version for building quickly.
    Mac:           brew upgrade bazel
    Ubuntu:        sudo apt-get upgrade bazel
    Centos/Redhat: sudo yum update bazel
    Fedore:        sudo dnf update bazel
    For more information.Please read this document https://docs.bazel.build/versions/master/install.html
    " >&2
fi

if [ $(uname -s) = "Linux" ]; then
        tidb::util::ensure-bazel
fi

if [ $(uname -s) = "Darwin" ];
then
        tidb::util::ensure-homebrew
        tidb::util::ensure-homebrew-bazel
fi


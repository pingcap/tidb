#!/usr/bin/env bash
# Copyright 2024 PingCAP, Inc.
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

# help
# download some third party tools for br&lightning integration test
# example: ./download_integration_test_binaries.sh master


set -o errexit
set -o pipefail


# Specify which branch to be utilized for executing the test, which is
# exclusively accessible when obtaining binaries from
# http://fileserver.pingcap.net.
branch=${1:-master}
file_server_url=${2:-http://fileserver.pingcap.net}

tikv_sha1_url="${file_server_url}/download/refs/pingcap/tikv/${branch}/sha1"
pd_sha1_url="${file_server_url}/download/refs/pingcap/pd/${branch}/sha1"
tiflash_sha1_url="${file_server_url}/download/refs/pingcap/tiflash/${branch}/sha1"
ticdc_sha1_url="${file_server_url}/download/refs/pingcap/ticdc/${branch}/sha1"

pd_sha1=$(curl "$pd_sha1_url")
tikv_sha1=$(curl "$tikv_sha1_url")
tiflash_sha1=$(curl "$tiflash_sha1_url")
ticdc_sha1=$(curl "$ticdc_sha1_url")

# download pd / tikv / tiflash binary build from tibuid multibranch pipeline
pd_download_url="${file_server_url}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz"
tikv_download_url="${file_server_url}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
tiflash_download_url="${file_server_url}/download/builds/pingcap/tiflash/${branch}/${tiflash_sha1}/centos7/tiflash.tar.gz"
ticdc_download_url="${file_server_url}/download/builds/pingcap/ticdc/${ticdc_sha1}/centos7/ticdc-linux-amd64.tar.gz"

set -o nounset

# See https://misc.flogisoft.com/bash/tip_colors_and_formatting.
color-green() { # Green
	echo -e "\x1B[1;32m${*}\x1B[0m"
}

function download() {
    local url=$1
    local file_name=$2
    local file_path=$3
    if [[ -f "${file_path}" ]]; then
        echo "file ${file_name} already exists, skip download"
        return
    fi
    echo "download ${file_name} from ${url}"
    wget --no-verbose --retry-connrefused --waitretry=1 -t 3 -O "${file_path}" "${url}"
}

function main() { 
    rm -rf third_bin
    rm -rf tmp
    mkdir third_bin
    mkdir tmp

    #PD server
    download "$pd_download_url" "pd-server.tar.gz" "tmp/pd-server.tar.gz"
    # tar -xzf tmp/pd-server.tar.gz -C third_bin --wildcards 'bin/*'
    tar -xzf tmp/pd-server.tar.gz -C third_bin
    mv third_bin/bin/* third_bin/

    #TiKV server
    download "$tikv_download_url" "tikv-server.tar.gz" "tmp/tikv-server.tar.gz"
    # tar -xzf tmp/tikv-server.tar.gz -C third_bin --wildcards 'bin/*'
    tar -xzf tmp/tikv-server.tar.gz -C third_bin
    mv third_bin/bin/* third_bin/

    #TiFlash
    download "$tiflash_download_url" "tiflash.tar.gz" "tmp/tiflash.tar.gz"
    tar -xzf tmp/tiflash.tar.gz -C third_bin
    mv third_bin/tiflash third_bin/_tiflash
    mv third_bin/_tiflash/* third_bin && rm -rf third_bin/_tiflash

    #TiCDC
    download "$ticdc_download_url" "ticdc-linux-amd64.tar.gz" "tmp/ticdc-linux-amd64.tar.gz"
    # tar -xzf tmp/ticdc-linux-amd64.tar.gz -C third_bin --wildcards '*/bin/*'
    tar -xzf tmp/ticdc-linux-amd64.tar.gz -C third_bin
    mv third_bin/ticdc-linux-amd64/bin/* third_bin/

    chmod +x third_bin/*
    rm -rf tmp
    # rm -rf third_bin/bin
    ls -alh third_bin/
}

main "$@"

color-green "Download SUCCESS"

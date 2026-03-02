#!/usr/bin/env bash
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

set -euo pipefail

VERSION="1.24.1"
BASE_URL="https://github.com/microsoft/onnxruntime/releases/download/v${VERSION}"
ROOT_DIR=$(git rev-parse --show-toplevel)
DEST_BASE="${ROOT_DIR}/lib/onnxruntime"

usage() {
  echo "Usage: $0 [--all]" >&2
  exit 1
}

sha256_check() {
  local expected="$1"
  local file="$2"
  if command -v sha256sum >/dev/null 2>&1; then
    echo "${expected}  ${file}" | sha256sum -c -
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    echo "${expected}  ${file}" | shasum -a 256 -c -
    return
  fi
  echo "No sha256 checker (sha256sum or shasum) found" >&2
  exit 1
}

download_platform() {
  local asset="$1"
  local checksum="$2"
  local goos="$3"
  local goarch="$4"
  local platform_dir="${goos}-${goarch}"
  local dest="${DEST_BASE}/${platform_dir}"
  local url="${BASE_URL}/${asset}"
  local tmp
  tmp=$(mktemp)

  echo "Downloading ${asset}"
  curl -fsSL "${url}" -o "${tmp}"
  sha256_check "${checksum}" "${tmp}"

  rm -rf "${dest}"
  mkdir -p "${dest}"

  local base_dir="${asset%.tgz}"
  tar -xzf "${tmp}" -C "${dest}" --strip-components=2 "${base_dir}/lib"
  rm -f "${tmp}"
}

if [[ $# -gt 1 ]]; then
  usage
fi

if [[ $# -eq 1 ]]; then
  if [[ "$1" != "--all" ]]; then
    usage
  fi
  download_platform "onnxruntime-linux-x64-${VERSION}.tgz" \
    "9142552248b735920f9390027e4512a2cacf8946a1ffcbe9071a5c210531026f" \
    "linux" "amd64"
  download_platform "onnxruntime-linux-aarch64-${VERSION}.tgz" \
    "0f56edd68f7602df790b68b874a46b115add037e88385c6c842bb763b39b9f89" \
    "linux" "arm64"
  download_platform "onnxruntime-osx-arm64-${VERSION}.tgz" \
    "c2969315cd9ce0f5fa04f6b53ff72cb92f87f7dcf38e88cacfa40c8f983fbba9" \
    "darwin" "arm64"
  exit 0
fi

uname_s=$(uname -s)
uname_m=$(uname -m)
case "${uname_s}-${uname_m}" in
  Linux-x86_64)
    download_platform "onnxruntime-linux-x64-${VERSION}.tgz" \
      "9142552248b735920f9390027e4512a2cacf8946a1ffcbe9071a5c210531026f" \
      "linux" "amd64"
    ;;
  Linux-aarch64|Linux-arm64)
    download_platform "onnxruntime-linux-aarch64-${VERSION}.tgz" \
      "0f56edd68f7602df790b68b874a46b115add037e88385c6c842bb763b39b9f89" \
      "linux" "arm64"
    ;;
  Darwin-arm64)
    download_platform "onnxruntime-osx-arm64-${VERSION}.tgz" \
      "c2969315cd9ce0f5fa04f6b53ff72cb92f87f7dcf38e88cacfa40c8f983fbba9" \
      "darwin" "arm64"
    ;;
  *)
    echo "Unsupported platform: ${uname_s}-${uname_m}" >&2
    exit 1
    ;;
  esac

#!/bin/bash
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

# See https://gist.github.com/howardjohn/c0f5d0bc293ef7d7fada533a2c9ffaf4
# Usage: go test -exec=true -toolexec=go-compile-without-link -vet=off ./...
# Preferably as an alias like `alias go-test-compile='go test -exec=true -toolexec=go-compile-without-link -vet=off'`
# This will compile all tests, but not link them (which is the least cacheable part)

if [[ "${2}" == "-V=full" ]]; then
  "$@"
  exit 0
fi
case "$(basename ${1})" in
  link)
    # Output a dummy file
    touch "${3}"
    ;;
  # We could skip vet as well, but it can be done with -vet=off if desired
  *)
    "$@"
esac

# Copyright 2020 PingCAP, Inc.
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

BASE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
OUT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/bin)

include $(BASE_DIR)/Makefile.common

.PHONY: server buildsucc

default: server buildsucc

buildsucc:
	@echo "Build TiDB Server successfully!"

server:
ifeq ($(TARGET), "")
	cd ${BASE_DIR} && \
	CGO_ENABLED=1 $(GOBUILD) -ldflags '$(LDFLAGS)' -o $(OUT_DIR)/tidb-server ${BASE_DIR}/cmd/tidb-server
else
	cd ${BASE_DIR} && \
	CGO_ENABLED=1 $(GOBUILD) -ldflags '$(LDFLAGS)' -o '$(TARGET)' ${BASE_DIR}/cmd/tidb-server
endif

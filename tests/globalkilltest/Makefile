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

GLOBAL_KILL_TEST_SERVER_LDFLAGS =  -X "github.com/pingcap/tidb/domain.ldflagIsGlobalKillTest=1"
GLOBAL_KILL_TEST_SERVER_LDFLAGS += -X "github.com/pingcap/tidb/domain.ldflagServerIDTTL=10"
GLOBAL_KILL_TEST_SERVER_LDFLAGS += -X "github.com/pingcap/tidb/domain.ldflagServerIDTimeToKeepAlive=1"
GLOBAL_KILL_TEST_SERVER_LDFLAGS += -X "github.com/pingcap/tidb/domain.ldflagServerIDTimeToCheckPDConnectionRestored=1"
GLOBAL_KILL_TEST_SERVER_LDFLAGS += -X "github.com/pingcap/tidb/domain.ldflagLostConnectionToPDTimeout=5"
GLOBAL_KILL_TEST_SERVER_LDFLAGS += -X "github.com/pingcap/tidb/store.ldflagGetEtcdAddrsFromConfig=1"

.PHONY: server buildsucc

default: server buildsucc

buildsucc:
	@echo "Build TiDB Server (Global Kill Test) successfully! Setup a cluster with PD & TiKV, then Run ./run-tests.sh --pd=<pd client path>."

clean:
	rm -f *.log*
	rm -rf tikv-*/ pd-*/

server:
ifeq ($(TARGET), "")
	cd ${BASE_DIR} && \
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(GLOBAL_KILL_TEST_SERVER_LDFLAGS) $(CHECK_FLAG)' -o $(OUT_DIR)/globalkilltest_tidb-server tidb-server/main.go
else
	cd ${BASE_DIR} && \
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(GLOBAL_KILL_TEST_SERVER_LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' tidb-server/main.go
endif

# Copyright 2019 PingCAP, Inc.
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

include Makefile.common


.DEFAULT_GOAL := default

# Adapted from https://www.thapaliya.com/en/writings/well-documented-makefiles/
.PHONY: help
help: ## Display this help and any documented user-facing targets. Other undocumented targets may be present in the Makefile.
help:
	@awk 'BEGIN {FS = ": ##"; printf "Usage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_\.\-\/%]+: ##/ { printf "  %-45s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

default: server buildsucc

.PHONY: server-admin-check
server-admin-check: server_check buildsucc

.PHONY: buildsucc
buildsucc:
	@echo Build TiDB Server successfully!

.PHONY: all
all: dev server benchkv

.PHONY: dev
dev: checklist check integrationtest gogenerate br_unit_test test_part_parser_dev ut check-file-perm
	@>&2 echo "Great, all tests passed."

# Install the check tools.
.PHONY: check-setup
check-setup:tools/bin/revive

.PHONY: precheck
precheck: fmt bazel_prepare

.PHONY: check
check: check-bazel-prepare parser_yacc check-parallel lint tidy testSuite errdoc license

.PHONY: fmt
fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w -r 'interface{} -> any' $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

.PHONY: check-static
check-static: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(PACKAGE_DIRECTORIES)) --config .golangci.yml

.PHONY: check-file-perm
check-file-perm:
	@echo "check file permission"
	./tools/check/check-file-perm.sh

.PHONY: gogenerate
gogenerate:
	@echo "go generate ./..."
	./tools/check/check-gogenerate.sh

.PHONY: errdoc
errdoc:tools/bin/errdoc-gen
	@echo "generator errors.toml"
	./tools/check/check-errdoc.sh

.PHONY: lint
lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES_TIDB_TESTS)
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml ./lightning/...
	go run tools/dashboard-linter/main.go pkg/metrics/grafana/overview.json
	go run tools/dashboard-linter/main.go pkg/metrics/grafana/performance_overview.json
	go run tools/dashboard-linter/main.go pkg/metrics/grafana/tidb.json
	go run tools/dashboard-linter/main.go pkg/metrics/grafana/tidb_resource_control.json
	go run tools/dashboard-linter/main.go pkg/metrics/grafana/tidb_runtime.json
	go run tools/dashboard-linter/main.go pkg/metrics/grafana/tidb_summary.json

.PHONY: license
license:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) \
		--run_under="cd $(CURDIR) && " \
		 @com_github_apache_skywalking_eyes//cmd/license-eye:license-eye --run_under="cd $(CURDIR) && "  -- -c ./.github/licenserc.yml  header check

.PHONY: tidy
tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

.PHONY: testSuite
testSuite:
	@echo "testSuite"
	./tools/check/check_testSuite.sh

.PHONY: check-parallel
check-parallel:
# Make sure no tests are run in parallel to prevent possible unstable tests.
# See https://github.com/pingcap/tidb/pull/30692.
	@! find . -name "*_test.go" -not -path "./vendor/*" -print0 | \
		xargs -0 grep -F -n "t.Parallel()" || \
		! echo "Error: all the go tests should be run in serial."

CLEAN_UT_BINARY := find . -name '*.test.bin'| xargs rm -f

.PHONY: clean
clean: failpoint-disable
	$(GO) clean -i ./...
	rm -rf $(TEST_COVERAGE_DIR)
	@$(CLEAN_UT_BINARY)

# Split tests for CI to run `make test` in parallel.
.PHONY: test
test: test_part_1 test_part_2
	@>&2 echo "Great, all tests passed."

.PHONY: test_part_1
test_part_1: checklist integrationtest

.PHONY: test_part_2
test_part_2: test_part_parser ut gogenerate br_unit_test dumpling_unit_test

.PHONY: test_part_parser
test_part_parser: parser_yacc test_part_parser_dev

.PHONY: test_part_parser_dev
test_part_parser_dev: parser_fmt parser_unit_test

.PHONY: parser
parser:
	@cd pkg/parser && make parser

.PHONY: parser_yacc
parser_yacc:
	@cd pkg/parser && mv parser.go parser.go.committed && make parser && diff -u parser.go.committed parser.go && rm parser.go.committed

.PHONY: parser_fmt
parser_fmt:
	@cd pkg/parser && make fmt

.PHONY: parser_unit_test
parser_unit_test:
	@cd pkg/parser && make test

.PHONY: test_part_br
test_part_br: br_unit_test br_integration_test

.PHONY: test_part_dumpling
test_part_dumpling: dumpling_unit_test dumpling_integration_test

.PHONY: integrationtest
integrationtest: server_check
	@mkdir -p $(TEST_COVERAGE_DIR)
	@cd tests/integrationtest && GOCOVERDIR=../../$(TEST_COVERAGE_DIR) ./run-tests.sh -s ../../bin/tidb-server
	@$(GO) tool covdata textfmt -i=$(TEST_COVERAGE_DIR) -o=coverage.dat

.PHONY: ddltest
ddltest:
	@cd cmd/ddltest && $(GO) test --tags=deadllock,intest -o ../../bin/ddltest -c

.PHONY: ut
ut: tools/bin/ut tools/bin/xprog failpoint-enable
	tools/bin/ut $(X) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

.PHONY: gotest_in_verify_ci
gotest_in_verify_ci: tools/bin/xprog tools/bin/ut failpoint-enable
	@echo "Running gotest_in_verify_ci"
	@mkdir -p $(TEST_COVERAGE_DIR)
	tools/bin/ut --junitfile "$(TEST_COVERAGE_DIR)/tidb-junit-report.xml" --coverprofile "$(TEST_COVERAGE_DIR)/tidb_cov.unit_test.out" --except unstable.txt || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

.PHONY: gotest_unstable_in_verify_ci
gotest_unstable_in_verify_ci: tools/bin/xprog tools/bin/ut failpoint-enable
	@echo "Running gotest_unstable_in_verify_ci"
	@mkdir -p $(TEST_COVERAGE_DIR)
	tools/bin/ut --junitfile "$(TEST_COVERAGE_DIR)/tidb-junit-report.xml" --coverprofile "$(TEST_COVERAGE_DIR)/tidb_cov.unit_test.out" --only unstable.txt || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

.PHONY: race
race: failpoint-enable
	@mkdir -p $(TEST_COVERAGE_DIR)
	tools/bin/ut --race --junitfile "$(TEST_COVERAGE_DIR)/tidb-junit-report.xml" --coverprofile "$(TEST_COVERAGE_DIR)/tidb_cov.unit_test" --except unstable.txt || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

.PHONY: server
server:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server ./cmd/tidb-server
else
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' ./cmd/tidb-server
endif

.PHONY: server_debug
server_debug:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) -gcflags="all=-N -l" $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server-debug ./cmd/tidb-server
else
	CGO_ENABLED=1 $(GOBUILD) -gcflags="all=-N -l" $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' ./cmd/tidb-server
endif

.PHONY: init-submodule
init-submodule:
	git submodule init && git submodule update --force

.PHONY: enterprise-prepare
enterprise-prepare:
	cd pkg/extension/enterprise/generate && $(GO) generate -run genfile main.go

.PHONY: enterprise-clear
enterprise-clear:
	cd pkg/extension/enterprise/generate && $(GO) generate -run clear main.go

.PHONY: enterprise-docker
enterprise-docker: init-submodule enterprise-prepare
	docker build -t "$(DOCKERPREFIX)tidb:latest" --build-arg 'GOPROXY=$(shell go env GOPROXY),' -f Dockerfile.enterprise .

.PHONY: enterprise-server-build
enterprise-server-build: TIDB_EDITION=Enterprise
enterprise-server-build:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) -tags enterprise $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG) $(EXTENSION_FLAG)' -o bin/tidb-server cmd/tidb-server/main.go
else
	CGO_ENABLED=1 $(GOBUILD) -tags enterprise $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG) $(EXTENSION_FLAG)' -o '$(TARGET)' cmd/tidb-server/main.go
endif

.PHONY: enterprise-server
enterprise-server:
	$(MAKE) init-submodule
	$(MAKE) enterprise-prepare
	$(MAKE) enterprise-server-build

.PHONY: server_check
server_check:
ifeq ($(TARGET), "")
	$(GOBUILD) -cover $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o bin/tidb-server ./cmd/tidb-server
else
	$(GOBUILD) -cover $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o '$(TARGET)' ./cmd/tidb-server
endif

.PHONY: linux
linux:
ifeq ($(TARGET), "")
	GOOS=linux $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server-linux ./cmd/tidb-server
else
	GOOS=linux $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' ./cmd/tidb-server
endif

.PHONY: server_coverage
server_coverage:
ifeq ($(TARGET), "")
	$(GOBUILDCOVERAGE) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(COVERAGE_SERVER_LDFLAGS) $(CHECK_FLAG)' -o ../bin/tidb-server-coverage
else
	$(GOBUILDCOVERAGE) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(COVERAGE_SERVER_LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)'
endif

.PHONY: benchkv
benchkv:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/benchkv cmd/benchkv/main.go

.PHONY: benchraw
benchraw:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/benchraw cmd/benchraw/main.go

.PHONY: benchdb
benchdb:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/benchdb cmd/benchdb/main.go

.PHONY: importer
importer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/importer ./cmd/importer

.PHONY: checklist
checklist:
	cat checklist.md

.PHONY: failpoint-enable
failpoint-enable: tools/bin/failpoint-ctl
# Converting gofail failpoints...
	@$(FAILPOINT_ENABLE)

.PHONY: failpoint-disable
failpoint-disable: tools/bin/failpoint-ctl
# Restoring gofail failpoints...
	@$(FAILPOINT_DISABLE)

.PHONY: tools/bin/ut
tools/bin/ut: tools/check/ut.go
	cd tools/check; \
	$(GO) build -o ../bin/ut ut.go

.PHONY: tools/bin/xprog
tools/bin/xprog: tools/check/xprog.go
	cd tools/check; \
	$(GO) build -o ../bin/xprog xprog.go

.PHONY: tools/bin/revive
tools/bin/revive:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/mgechev/revive@v1.2.1

.PHONY: tools/bin/failpoint-ctl
tools/bin/failpoint-ctl:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/pingcap/failpoint/failpoint-ctl@9b3b6e3

.PHONY: tools/bin/errdoc-gen
tools/bin/errdoc-gen:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/pingcap/errors/errdoc-gen@518f63d

.PHONY: tools/bin/golangci-lint
tools/bin/golangci-lint:
	# Build from source is not recommand. See https://golangci-lint.run/usage/install/
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.47.2

.PHONY: tools/bin/vfsgendev
tools/bin/vfsgendev:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/shurcooL/vfsgen/cmd/vfsgendev@0d455de

.PHONY: tools/bin/gotestsum
tools/bin/gotestsum:
	GOBIN=$(shell pwd)/tools/bin $(GO) install gotest.tools/gotestsum@v1.8.1

# mockgen@v0.2.0 is imcompatible with v0.3.0, so install it always.
.PHONY: mockgen
mockgen:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/lance6716/mock/mockgen@v0.4.0-patch

# Usage:
#
# 	$ make vectorized-bench VB_FILE=Time VB_FUNC=builtinCurrentDateSig
.PHONY: vectorized-bench
vectorized-bench:
	cd ./expression && \
		go test -v -timeout=0 -benchmem \
			-bench=BenchmarkVectorizedBuiltin$(VB_FILE)Func \
			-run=BenchmarkVectorizedBuiltin$(VB_FILE)Func \
			-args "$(VB_FUNC)"

.PHONY: testpkg
testpkg: failpoint-enable
ifeq ("$(pkg)", "")
	@echo "Require pkg parameter"
else
	@echo "Running unit test for github.com/pingcap/tidb/$(pkg)"
	@export log_level=fatal; export TZ='Asia/Shanghai'; \
	$(GOTEST) -tags 'intest' -v -ldflags '$(TEST_LDFLAGS)' -cover github.com/pingcap/tidb/$(pkg) || { $(FAILPOINT_DISABLE); exit 1; }
endif
	@$(FAILPOINT_DISABLE)

# Collect the daily benchmark data.
# Usage:
#	make bench-daily TO=/path/to/file.json
.PHONY: bench-daily
bench-daily:
	go test github.com/pingcap/tidb/pkg/session -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/executor -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/executor/test/splittest -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/tablecodec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/expression -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/util/rowcodec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/util/codec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/distsql -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/statistics -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/pkg/util/benchdaily -run TestBenchDaily -bench Ignore \
		-date `git log -n1 --date=unix --pretty=format:%cd` \
		-commit `git log -n1 --pretty=format:%h` \
		-outfile $(TO)

.PHONY: build_tools
build_tools: build_br build_lightning build_lightning-ctl

.PHONY: lightning_web
lightning_web:
	@cd lightning/web && npm install && npm run build

.PHONY: build_br
build_br:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(BR_BIN) ./br/cmd/br

.PHONY: build_lightning_for_web
build_lightning_for_web:
	CGO_ENABLED=1 $(GOBUILD) -tags dev $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_BIN) lightning/cmd/tidb-lightning/main.go

.PHONY: build_lightning
build_lightning:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_BIN) ./lightning/cmd/tidb-lightning

.PHONY: build_lightning-ctl
build_lightning-ctl:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_CTL_BIN) ./lightning/cmd/tidb-lightning-ctl

.PHONY: build_for_lightning_integration_test
build_for_lightning_integration_test:
	@make failpoint-enable
	($(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tidb/lightning/...,github.com/pingcap/tidb/pkg/lightning/... \
		-o $(LIGHTNING_BIN).test \
		github.com/pingcap/tidb/lightning/cmd/tidb-lightning && \
	$(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tidb/lightning/...,github.com/pingcap/tidb/pkg/lightning/... \
		-o $(LIGHTNING_CTL_BIN).test \
		github.com/pingcap/tidb/lightning/cmd/tidb-lightning-ctl && \
	$(GOBUILD) $(RACE_FLAG) -o bin/fake-oauth tools/fake-oauth/main.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/parquet_gen tools/gen-parquet/main.go \
	) || (make failpoint-disable && exit 1)
	@make failpoint-disable

lightning_integration_test: build_lightning build_for_lightning_integration_test
	lightning/tests/run.sh

build_for_br_integration_test:
	@make failpoint-enable
	($(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tidb/br/... \
		-o $(BR_BIN).test \
		github.com/pingcap/tidb/br/cmd/br && \
	$(GOBUILD) $(RACE_FLAG) -o bin/locker br/tests/br_key_locked/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/gc br/tests/br_z_gc_safepoint/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/fake-oauth tools/fake-oauth/main.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/rawkv br/tests/br_rawkv/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/txnkv br/tests/br_txn/*.go \
	) || (make failpoint-disable && exit 1)
	@make failpoint-disable

.PHONY: br_unit_test
br_unit_test: export ARGS=$$($(BR_PACKAGES))
br_unit_test:
	@make failpoint-enable
	@export TZ='Asia/Shanghai';
	$(GOTEST) $(RACE_FLAG) -ldflags '$(LDFLAGS)' $(ARGS) -coverprofile=coverage.txt || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

.PHONY: br_unit_test_in_verify_ci
br_unit_test_in_verify_ci: export ARGS=$$($(BR_PACKAGES))
br_unit_test_in_verify_ci: tools/bin/gotestsum
	@make failpoint-enable
	@export TZ='Asia/Shanghai';
	@mkdir -p $(TEST_COVERAGE_DIR)
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile "$(TEST_COVERAGE_DIR)/br-junit-report.xml" -- $(RACE_FLAG) -ldflags '$(LDFLAGS)' \
	$(ARGS) -coverprofile="$(TEST_COVERAGE_DIR)/br_cov.unit_test.out" || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

.PHONY: br_integration_test
br_integration_test: br_bins build_br build_for_br_integration_test
	@cd br && tests/run.sh

.PHONY: br_integration_test_debug
br_integration_test_debug:
	@cd br && tests/run.sh --no-tiflash

.PHONY: br_compatibility_test_prepare
br_compatibility_test_prepare:
	@cd br && tests/run_compatible.sh prepare

.PHONY: br_compatibility_test
br_compatibility_test:
	@cd br && tests/run_compatible.sh run

.PHONY: mock_s3iface
mock_s3iface: mockgen
	tools/bin/mockgen -package mock github.com/aws/aws-sdk-go/service/s3/s3iface S3API > br/pkg/mock/s3iface.go

# mock interface for lightning and IMPORT INTO
.PHONY: mock_lightning
mock_lightning: mockgen
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/lightning/backend Backend,EngineWriter,TargetInfoGetter,ChunkFlushStatus > br/pkg/mock/backend.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/lightning/backend/encode Encoder,EncodingBuilder,Rows,Row > br/pkg/mock/encode.go
	tools/bin/mockgen -package mocklocal github.com/pingcap/tidb/pkg/lightning/backend/local DiskUsage,TiKVModeSwitcher,StoreHelper > br/pkg/mock/mocklocal/local.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/br/pkg/utils TaskRegister > br/pkg/mock/task_register.go

.PHONY: gen_mock
gen_mock: mockgen
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor TaskTable,Pool,TaskExecutor,Extension > pkg/disttask/framework/mock/task_executor_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/scheduler Scheduler,CleanUpRoutine,TaskManager > pkg/disttask/framework/mock/scheduler_mock.go
	tools/bin/mockgen -destination pkg/disttask/framework/scheduler/mock/scheduler_mock.go -package mock github.com/pingcap/tidb/pkg/disttask/framework/scheduler Extension
	tools/bin/mockgen -embed -package mockexecute github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute StepExecutor > pkg/disttask/framework/mock/execute/execute_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/importinto MiniTaskExecutor > pkg/disttask/importinto/mock/import_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/planner LogicalPlan,PipelineSpec > pkg/disttask/framework/mock/plan_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/util/sqlexec RestrictedSQLExecutor > pkg/util/sqlexec/mock/restricted_sql_executor_mock.go
	tools/bin/mockgen -package mockstorage github.com/pingcap/tidb/br/pkg/storage ExternalStorage > br/pkg/mock/storage/storage.go

# There is no FreeBSD environment for GitHub actions. So cross-compile on Linux
# but that doesn't work with CGO_ENABLED=1, so disable cgo. The reason to have
# cgo enabled on regular builds is performance.
ifeq ("$(GOOS)", "freebsd")
        GOBUILD  = CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags '$(LDFLAGS)'
endif

# TODO: adjust bins when br integraion tests reformat.
.PHONY: br_bins
br_bins:
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/pd-ctl
	@which bin/go-ycsb
	@which bin/minio
	@which bin/tiflash
	@which bin/libtiflash_proxy.so
	@which bin/cdc
	@which bin/fake-gcs-server
	@which bin/tikv-importer
	if [ ! -d bin/flash_cluster_manager ]; then echo "flash_cluster_manager not exist"; exit 1; fi

%_generated.go: %.rl
	ragel -Z -G2 -o tmp_parser.go $<
	@echo '// Code generated by ragel DO NOT EDIT.' | cat - tmp_parser.go | sed 's|//line |//.... |g' > $@
	@rm tmp_parser.go

.PHONY: data_parsers
data_parsers: tools/bin/vfsgendev pkg/lightning/mydump/parser_generated.go lightning_web
	PATH="$(GOPATH)/bin":"$(PATH)":"$(TOOLS)" protoc -I. -I"$(GOPATH)/src" pkg/lightning/checkpoints/checkpointspb/file_checkpoints.proto --gogofaster_out=.
	tools/bin/vfsgendev -source='"github.com/pingcap/tidb/lightning/pkg/web".Res' && mv res_vfsdata.go lightning/pkg/web/

.PHONY: build_dumpling
build_dumpling:
	$(DUMPLING_GOBUILD) $(RACE_FLAG) -tags codes -o $(DUMPLING_BIN) dumpling/cmd/dumpling/main.go

.PHONY: dumpling_unit_test
dumpling_unit_test: export DUMPLING_ARGS=$$($(DUMPLING_PACKAGES))
dumpling_unit_test: failpoint-enable
	$(DUMPLING_GOTEST) $(RACE_FLAG) -coverprofile=coverage.txt -covermode=atomic $(DUMPLING_ARGS) || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

.PHONY: dumpling_unit_test_in_verify_ci
dumpling_unit_test_in_verify_ci: export DUMPLING_ARGS=$$($(DUMPLING_PACKAGES))
dumpling_unit_test_in_verify_ci: failpoint-enable tools/bin/gotestsum
	@mkdir -p $(TEST_COVERAGE_DIR)
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile "$(TEST_COVERAGE_DIR)/dumpling-junit-report.xml" -- $(DUMPLING_ARGS) \
	$(RACE_FLAG) -coverprofile="$(TEST_COVERAGE_DIR)/dumpling_cov.unit_test.out" || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

.PHONY: dumpling_integration_test
dumpling_integration_test: dumpling_bins failpoint-enable
	@make build_dumpling
	@make failpoint-disable
	./dumpling/tests/run.sh $(CASE)

.PHONY: dumpling_bins
dumpling_bins:
	@which bin/tidb-server
	@which bin/minio
	@which bin/mc
	@which bin/tidb-lightning
	@which bin/sync_diff_inspector

.PHONY: generate_grafana_scripts
generate_grafana_scripts:
	@cd metrics/grafana && mv tidb_summary.json tidb_summary.json.committed && ./generate_json.sh && diff -u tidb_summary.json.committed tidb_summary.json && rm tidb_summary.json.committed

.PHONY: bazel_ci_prepare
bazel_ci_prepare:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) //:gazelle
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) //:gazelle -- update-repos -from_file=go.mod -to_macro DEPS.bzl%go_deps  -build_file_proto_mode=disable -prune
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  //cmd/mirror:mirror -- --mirror> tmp.txt
	mv tmp.txt DEPS.bzl
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  \
		--run_under="cd $(CURDIR) && " \
		 //tools/tazel:tazel

.PHONY: bazel_ci_simple_prepare
bazel_ci_simple_prepare:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) //:gazelle
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  \
		--run_under="cd $(CURDIR) && " \
		 //tools/tazel:tazel

.PHONY: bazel_prepare
bazel_prepare: ## Update and generate BUILD.bazel files. Please run this before commit.
	bazel run //:gazelle
	bazel run //:gazelle -- update-repos -from_file=go.mod -to_macro DEPS.bzl%go_deps  -build_file_proto_mode=disable -prune
	bazel run \
		--run_under="cd $(CURDIR) && " \
		 //tools/tazel:tazel
	$(eval $@TMP_OUT := $(shell mktemp -d -t tidbbzl.XXXXXX))
	bazel run  //cmd/mirror -- --mirror> $($@TMP_OUT)/tmp.txt
	cp $($@TMP_OUT)/tmp.txt DEPS.bzl
	rm -rf $($@TMP_OUT)

.PHONY: bazel_ci_prepare_rbe
bazel_ci_prepare_rbe:
	bazel run //:gazelle
	bazel run //:gazelle -- update-repos -from_file=go.mod -to_macro DEPS.bzl%go_deps  -build_file_proto_mode=disable -prune
	bazel run --//build:with_rbe_flag=true \
		--run_under="cd $(CURDIR) && " \
		 //tools/tazel:tazel

.PHONY: check-bazel-prepare
check-bazel-prepare:
	@echo "make bazel_prepare"
	./tools/check/check-bazel-prepare.sh

.PHONY: bazel_test
bazel_test: failpoint-enable bazel_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) test $(BAZEL_CMD_CONFIG) --build_tests_only --test_keep_going=false \
		--define gotags=deadlock,intest \
		-- //... -//cmd/... -//tests/graceshutdown/... \
		-//tests/globalkilltest/... -//tests/readonlytest/... -//br/pkg/task:task_test -//tests/realtikvtest/...

.PHONY: bazel_coverage_test
bazel_coverage_test: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) --nohome_rc coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --jobs=35 --build_tests_only --test_keep_going=false \
		--@io_bazel_rules_go//go/config:cover_format=go_cover --define gotags=deadlock,intest \
		-- //... -//cmd/... -//tests/graceshutdown/... \
		-//tests/globalkilltest/... -//tests/readonlytest/... -//br/pkg/task:task_test -//tests/realtikvtest/...

.PHONY: bazel_build
bazel_build:
	mkdir -p bin
	bazel $(BAZEL_GLOBAL_CONFIG) build $(BAZEL_CMD_CONFIG) \
		//... --//build:with_nogo_flag=true
	bazel $(BAZEL_GLOBAL_CONFIG) build $(BAZEL_CMD_CONFIG) \
		//cmd/importer:importer //cmd/tidb-server:tidb-server //cmd/tidb-server:tidb-server-check --//build:with_nogo_flag=true
	cp bazel-out/k8-fastbuild/bin/cmd/tidb-server/tidb-server_/tidb-server ./bin
	cp bazel-out/k8-fastbuild/bin/cmd/importer/importer_/importer      ./bin
	cp bazel-out/k8-fastbuild/bin/cmd/tidb-server/tidb-server-check_/tidb-server-check ./bin
	bazel $(BAZEL_GLOBAL_CONFIG) build $(BAZEL_CMD_CONFIG) \
		//cmd/tidb-server:tidb-server --stamp --workspace_status_command=./build/print-enterprise-workspace-status.sh --define gotags=enterprise
	./bazel-out/k8-fastbuild/bin/cmd/tidb-server/tidb-server_/tidb-server -V

.PHONY: bazel_fail_build
bazel_fail_build:  failpoint-enable bazel_ci_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) build $(BAZEL_CMD_CONFIG) \
		//...

.PHONY: bazel_clean
bazel_clean:
	bazel $(BAZEL_GLOBAL_CONFIG) clean

.PHONY: bazel_junit
bazel_junit:
	bazel_collect
	@mkdir -p $(TEST_COVERAGE_DIR)
	mv ./junit.xml `$(TEST_COVERAGE_DIR)/junit.xml`

.PHONY: bazel_golangcilinter
bazel_golangcilinter:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) \
		--run_under="cd $(CURDIR) && " \
		@com_github_golangci_golangci_lint//cmd/golangci-lint:golangci-lint \
	-- run  $$($(PACKAGE_DIRECTORIES)) --config ./.golangci.yaml

.PHONY: bazel_brietest
bazel_brietest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/brietest/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_pessimistictest
bazel_pessimistictest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/pessimistictest/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_sessiontest
bazel_sessiontest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/sessiontest/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_statisticstest
bazel_statisticstest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/statisticstest/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_txntest
bazel_txntest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/txntest/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_addindextest
bazel_addindextest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_addindextest1
bazel_addindextest1: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest1/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_addindextest2
bazel_addindextest2: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest2/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_addindextest3
bazel_addindextest3: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest3/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_addindextest4
bazel_addindextest4: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest4/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
.PHONY: bazel_importintotest
bazel_importintotest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
.PHONY: bazel_importintotest2
bazel_importintotest2: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest2/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
.PHONY: bazel_importintotest3
bazel_importintotest3: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest3/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
.PHONY: bazel_importintotest4
bazel_importintotest4: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest4/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
.PHONY: bazel_pipelineddmltest
bazel_pipelineddmltest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/pipelineddmltest/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
.PHONY: bazel_flashbacktest
bazel_flashbacktest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/flashbacktest/...
	./build/jenkins_collect_coverage.sh

.PHONY: bazel_lint
bazel_lint: bazel_prepare
	bazel build //... --//build:with_nogo_flag=true

.PHONY: docker
docker:
	docker build -t "$(DOCKERPREFIX)tidb:latest" --build-arg 'GOPROXY=$(shell go env GOPROXY),' -f Dockerfile .

.PHONY: docker-test
docker-test:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t "$(DOCKERPREFIX)tidb:latest" --build-arg 'GOPROXY=$(shell go env GOPROXY),' -f Dockerfile .

.PHONY: bazel_mirror
bazel_mirror:
	$(eval $@TMP_OUT := $(shell mktemp -d -t tidbbzl.XXXXXX))
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  //cmd/mirror:mirror -- --mirror> $($@TMP_OUT)/tmp.txt
	cp $($@TMP_OUT)/tmp.txt DEPS.bzl
	rm -rf $($@TMP_OUT)

.PHONY: bazel_sync
bazel_sync:
	bazel $(BAZEL_GLOBAL_CONFIG) sync $(BAZEL_SYNC_CONFIG)

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

.PHONY: help all clean test server dev benchkv benchraw check checklist parser tidy ddltest build_br build_lightning build_lightning-ctl build_dumpling ut bazel_build bazel_prepare bazel_test check-file-perm check-bazel-prepare bazel_lint tazel precheck

.DEFAULT_GOAL := default

# Adapted from https://www.thapaliya.com/en/writings/well-documented-makefiles/
help: ## Display this help and any documented user-facing targets. Other undocumented targets may be present in the Makefile.
help:
	@awk 'BEGIN {FS = ": ##"; printf "Usage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_\.\-\/%]+: ##/ { printf "  %-45s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

default: server buildsucc

server-admin-check: server_check buildsucc

buildsucc:
	@echo Build TiDB Server successfully!

all: dev server benchkv

dev: checklist check integrationtest gogenerate br_unit_test test_part_parser_dev ut check-file-perm
	@>&2 echo "Great, all tests passed."

# Install the check tools.
check-setup:tools/bin/revive

precheck: fmt bazel_prepare

check: check-bazel-prepare parser_yacc check-parallel lint tidy testSuite errdoc license

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

check-static: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(PACKAGE_DIRECTORIES)) --config .golangci.yml

check-file-perm:
	@echo "check file permission"
	./tools/check/check-file-perm.sh

gogenerate:
	@echo "go generate ./..."
	./tools/check/check-gogenerate.sh

errdoc:tools/bin/errdoc-gen
	@echo "generator errors.toml"
	./tools/check/check-errdoc.sh

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES_TIDB_TESTS)
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml ./br/pkg/lightning/...

license:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) \
		--run_under="cd $(CURDIR) && " \
		 @com_github_apache_skywalking_eyes//cmd/license-eye:license-eye --run_under="cd $(CURDIR) && "  -- -c ./.github/licenserc.yml  header check


tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

testSuite:
	@echo "testSuite"
	./tools/check/check_testSuite.sh

check-parallel:
# Make sure no tests are run in parallel to prevent possible unstable tests.
# See https://github.com/pingcap/tidb/pull/30692.
	@! find . -name "*_test.go" -not -path "./vendor/*" -print0 | \
		xargs -0 grep -F -n "t.Parallel()" || \
		! echo "Error: all the go tests should be run in serial."

clean: failpoint-disable
	$(GO) clean -i ./...
	rm -rf $(TEST_COVERAGE_DIR)
	
# Split tests for CI to run `make test` in parallel.
test: test_part_1 test_part_2
	@>&2 echo "Great, all tests passed."

test_part_1: checklist integrationtest

test_part_2: test_part_parser ut gogenerate br_unit_test dumpling_unit_test

test_part_parser: parser_yacc test_part_parser_dev

test_part_parser_dev: parser_fmt parser_unit_test

parser:
	@cd pkg/parser && make parser

parser_yacc:
	@cd pkg/parser && mv parser.go parser.go.committed && make parser && diff -u parser.go.committed parser.go && rm parser.go.committed

parser_fmt:
	@cd pkg/parser && make fmt

parser_unit_test:
	@cd pkg/parser && make test

test_part_br: br_unit_test br_integration_test

test_part_dumpling: dumpling_unit_test dumpling_integration_test

integrationtest: server_check
	@mkdir -p $(TEST_COVERAGE_DIR)
	@cd tests/integrationtest && GOCOVERDIR=../../$(TEST_COVERAGE_DIR) ./run-tests.sh -s ../../bin/tidb-server
	@$(GO) tool covdata textfmt -i=$(TEST_COVERAGE_DIR) -o=coverage.dat

ddltest:
	@cd cmd/ddltest && $(GO) test --tags=deadllock,intest -o ../../bin/ddltest -c

CLEAN_UT_BINARY := find . -name '*.test.bin'| xargs rm

ut: tools/bin/ut tools/bin/xprog failpoint-enable
	tools/bin/ut $(X) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

gotest_in_verify_ci: tools/bin/xprog tools/bin/ut failpoint-enable
	@echo "Running gotest_in_verify_ci"
	@mkdir -p $(TEST_COVERAGE_DIR)
	@export TZ='Asia/Shanghai'; \
	tools/bin/ut --junitfile "$(TEST_COVERAGE_DIR)/tidb-junit-report.xml" --coverprofile "$(TEST_COVERAGE_DIR)/tidb_cov.unit_test.out" --except unstable.txt || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

gotest_unstable_in_verify_ci: tools/bin/xprog tools/bin/ut failpoint-enable
	@echo "Running gotest_in_verify_ci"
	@mkdir -p $(TEST_COVERAGE_DIR)
	@export TZ='Asia/Shanghai'; \
	tools/bin/ut --junitfile "$(TEST_COVERAGE_DIR)/tidb-junit-report.xml" --coverprofile "$(TEST_COVERAGE_DIR)/tidb_cov.unit_test.out" --only unstable.txt || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

race: failpoint-enable
	@mkdir -p $(TEST_COVERAGE_DIR)
	@export TZ='Asia/Shanghai'; \
	tools/bin/ut --race --junitfile "$(TEST_COVERAGE_DIR)/tidb-junit-report.xml" --coverprofile "$(TEST_COVERAGE_DIR)/tidb_cov.unit_test" --except unstable.txt || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

server:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server ./cmd/tidb-server
else
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' ./cmd/tidb-server
endif

server_debug:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) -gcflags="all=-N -l" $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server-debug ./cmd/tidb-server
else
	CGO_ENABLED=1 $(GOBUILD) -gcflags="all=-N -l" $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' ./cmd/tidb-server
endif

init-submodule:
	git submodule init && git submodule update --force

enterprise-prepare:
	cd pkg/extension/enterprise/generate && $(GO) generate -run genfile main.go

enterprise-clear:
	cd pkg/extension/enterprise/generate && $(GO) generate -run clear main.go

enterprise-docker: init-submodule enterprise-prepare
	docker build -t "$(DOCKERPREFIX)tidb:latest" --build-arg 'GOPROXY=$(shell go env GOPROXY),' -f Dockerfile.enterprise .

enterprise-server-build: TIDB_EDITION=Enterprise
enterprise-server-build:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) -tags enterprise $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG) $(EXTENSION_FLAG)' -o bin/tidb-server cmd/tidb-server/main.go
else
	CGO_ENABLED=1 $(GOBUILD) -tags enterprise $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG) $(EXTENSION_FLAG)' -o '$(TARGET)' cmd/tidb-server/main.go
endif

enterprise-server:
	$(MAKE) init-submodule
	$(MAKE) enterprise-prepare
	$(MAKE) enterprise-server-build

server_check:
ifeq ($(TARGET), "")
	$(GOBUILD) -cover $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o bin/tidb-server ./cmd/tidb-server
else
	$(GOBUILD) -cover $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o '$(TARGET)' ./cmd/tidb-server
endif

linux:
ifeq ($(TARGET), "")
	GOOS=linux $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server-linux ./cmd/tidb-server
else
	GOOS=linux $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' ./cmd/tidb-server
endif

server_coverage:
ifeq ($(TARGET), "")
	$(GOBUILDCOVERAGE) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(COVERAGE_SERVER_LDFLAGS) $(CHECK_FLAG)' -o ../bin/tidb-server-coverage
else
	$(GOBUILDCOVERAGE) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(COVERAGE_SERVER_LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)'
endif

benchkv:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/benchkv cmd/benchkv/main.go

benchraw:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/benchraw cmd/benchraw/main.go

benchdb:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/benchdb cmd/benchdb/main.go

importer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/importer ./cmd/importer

checklist:
	cat checklist.md

failpoint-enable: tools/bin/failpoint-ctl
# Converting gofail failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable: tools/bin/failpoint-ctl
# Restoring gofail failpoints...
	@$(FAILPOINT_DISABLE)

tools/bin/ut: tools/check/ut.go
	cd tools/check; \
	$(GO) build -o ../bin/ut ut.go

tools/bin/xprog: tools/check/xprog.go
	cd tools/check; \
	$(GO) build -o ../bin/xprog xprog.go

tools/bin/revive:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/mgechev/revive@v1.2.1

tools/bin/failpoint-ctl:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/pingcap/failpoint/failpoint-ctl@2eaa328

tools/bin/errdoc-gen:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/pingcap/errors/errdoc-gen@518f63d

tools/bin/golangci-lint:
	# Build from source is not recommand. See https://golangci-lint.run/usage/install/
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.47.2

tools/bin/vfsgendev:
	GOBIN=$(shell pwd)/tools/bin $(GO) install github.com/shurcooL/vfsgen/cmd/vfsgendev@0d455de

tools/bin/gotestsum:
	GOBIN=$(shell pwd)/tools/bin $(GO) install gotest.tools/gotestsum@v1.8.1

tools/bin/mockgen:
	GOBIN=$(shell pwd)/tools/bin $(GO) install go.uber.org/mock/mockgen@v0.3.0

# Usage:
#
# 	$ make vectorized-bench VB_FILE=Time VB_FUNC=builtinCurrentDateSig
vectorized-bench:
	cd ./expression && \
		go test -v -timeout=0 -benchmem \
			-bench=BenchmarkVectorizedBuiltin$(VB_FILE)Func \
			-run=BenchmarkVectorizedBuiltin$(VB_FILE)Func \
			-args "$(VB_FUNC)"

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
bench-daily:
	go test -tags intest github.com/pingcap/tidb/pkg/session -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/executor -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/executor/test/splittest -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/tablecodec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/expression -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/util/rowcodec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/util/codec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/distsql -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/statistics -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test -tags intest github.com/pingcap/tidb/pkg/util/benchdaily -run TestBenchDaily -bench Ignore \
		-date `git log -n1 --date=unix --pretty=format:%cd` \
		-commit `git log -n1 --pretty=format:%h` \
		-outfile $(TO)

build_tools: build_br build_lightning build_lightning-ctl

br_web:
	@cd br/web && npm install && npm run build

build_br:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(BR_BIN) ./br/cmd/br

build_lightning_for_web:
	CGO_ENABLED=1 $(GOBUILD) -tags dev $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_BIN) br/cmd/tidb-lightning/main.go

build_lightning:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_BIN) ./br/cmd/tidb-lightning

build_lightning-ctl:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_CTL_BIN) ./br/cmd/tidb-lightning-ctl

build_for_br_integration_test:
	@make failpoint-enable
	($(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tidb/br/... \
		-o $(BR_BIN).test \
		github.com/pingcap/tidb/br/cmd/br && \
	$(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tidb/br/... \
		-o $(LIGHTNING_BIN).test \
		github.com/pingcap/tidb/br/cmd/tidb-lightning && \
	$(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tidb/br/... \
		-o $(LIGHTNING_CTL_BIN).test \
		github.com/pingcap/tidb/br/cmd/tidb-lightning-ctl && \
	$(GOBUILD) $(RACE_FLAG) -o bin/locker br/tests/br_key_locked/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/gc br/tests/br_z_gc_safepoint/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/oauth br/tests/br_gcs/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/rawkv br/tests/br_rawkv/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/txnkv br/tests/br_txn/*.go && \
	$(GOBUILD) $(RACE_FLAG) -o bin/parquet_gen br/tests/lightning_checkpoint_parquet/*.go \
	) || (make failpoint-disable && exit 1)
	@make failpoint-disable

br_unit_test: export ARGS=$$($(BR_PACKAGES))
br_unit_test:
	@make failpoint-enable
	@export TZ='Asia/Shanghai';
	$(GOTEST) $(RACE_FLAG) -ldflags '$(LDFLAGS)' $(ARGS) -coverprofile=coverage.txt || ( make failpoint-disable && exit 1 )
	@make failpoint-disable
br_unit_test_in_verify_ci: export ARGS=$$($(BR_PACKAGES))
br_unit_test_in_verify_ci: tools/bin/gotestsum
	@make failpoint-enable
	@export TZ='Asia/Shanghai';
	@mkdir -p $(TEST_COVERAGE_DIR)
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile "$(TEST_COVERAGE_DIR)/br-junit-report.xml" -- $(RACE_FLAG) -ldflags '$(LDFLAGS)' \
	$(ARGS) -coverprofile="$(TEST_COVERAGE_DIR)/br_cov.unit_test.out" || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

br_integration_test: br_bins build_br build_for_br_integration_test
	@cd br && tests/run.sh

br_integration_test_debug:
	@cd br && tests/run.sh --no-tiflash

br_compatibility_test_prepare:
	@cd br && tests/run_compatible.sh prepare

br_compatibility_test:
	@cd br && tests/run_compatible.sh run

mock_s3iface: tools/bin/mockgen
	tools/bin/mockgen -package mock github.com/aws/aws-sdk-go/service/s3/s3iface S3API > br/pkg/mock/s3iface.go

# mock interface for lightning and IMPORT INTO
mock_lightning: tools/bin/mockgen
	tools/bin/mockgen -package mock github.com/pingcap/tidb/br/pkg/lightning/backend Backend,EngineWriter,TargetInfoGetter,ChunkFlushStatus > br/pkg/mock/backend.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/br/pkg/lightning/backend/encode Encoder,EncodingBuilder,Rows,Row > br/pkg/mock/encode.go
	tools/bin/mockgen -package mocklocal github.com/pingcap/tidb/br/pkg/lightning/backend/local DiskUsage,TiKVModeSwitcher > br/pkg/mock/mocklocal/local.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/br/pkg/utils TaskRegister > br/pkg/mock/task_register.go

gen_mock: tools/bin/mockgen
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor TaskTable,Pool,TaskExecutor,Extension > pkg/disttask/framework/mock/task_executor_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/scheduler Scheduler,CleanUpRoutine,TaskManager > pkg/disttask/framework/mock/scheduler_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/scheduler Extension > pkg/disttask/framework/scheduler/mock/scheduler_mock.go
	tools/bin/mockgen -package execute github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute SubtaskExecutor > pkg/disttask/framework/mock/execute/execute_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/importinto MiniTaskExecutor > pkg/disttask/importinto/mock/import_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/planner LogicalPlan,PipelineSpec > pkg/disttask/framework/mock/plan_mock.go
	tools/bin/mockgen -package mock github.com/pingcap/tidb/pkg/util/sqlexec RestrictedSQLExecutor > pkg/util/sqlexec/mock/restricted_sql_executor_mock.go
  
# There is no FreeBSD environment for GitHub actions. So cross-compile on Linux
# but that doesn't work with CGO_ENABLED=1, so disable cgo. The reason to have
# cgo enabled on regular builds is performance.
ifeq ("$(GOOS)", "freebsd")
        GOBUILD  = CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags '$(LDFLAGS)'
endif

# TODO: adjust bins when br integraion tests reformat.
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

data_parsers: tools/bin/vfsgendev br/pkg/lightning/mydump/parser_generated.go br_web
	PATH="$(GOPATH)/bin":"$(PATH)":"$(TOOLS)" protoc -I. -I"$(GOPATH)/src" br/pkg/lightning/checkpoints/checkpointspb/file_checkpoints.proto --gogofaster_out=.
	tools/bin/vfsgendev -source='"github.com/pingcap/tidb/br/pkg/lightning/web".Res' && mv res_vfsdata.go br/pkg/lightning/web/

build_dumpling:
	$(DUMPLING_GOBUILD) $(RACE_FLAG) -tags codes -o $(DUMPLING_BIN) dumpling/cmd/dumpling/main.go

dumpling_unit_test: export DUMPLING_ARGS=$$($(DUMPLING_PACKAGES))
dumpling_unit_test: failpoint-enable
	$(DUMPLING_GOTEST) $(RACE_FLAG) -coverprofile=coverage.txt -covermode=atomic $(DUMPLING_ARGS) || ( make failpoint-disable && exit 1 )
	@make failpoint-disable
dumpling_unit_test_in_verify_ci: export DUMPLING_ARGS=$$($(DUMPLING_PACKAGES))
dumpling_unit_test_in_verify_ci: failpoint-enable tools/bin/gotestsum
	@mkdir -p $(TEST_COVERAGE_DIR)
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile "$(TEST_COVERAGE_DIR)/dumpling-junit-report.xml" -- $(DUMPLING_ARGS) \
	$(RACE_FLAG) -coverprofile="$(TEST_COVERAGE_DIR)/dumpling_cov.unit_test.out" || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

dumpling_integration_test: dumpling_bins failpoint-enable
	@make build_dumpling
	@make failpoint-disable
	./dumpling/tests/run.sh $(CASE)

dumpling_bins:
	@which bin/tidb-server
	@which bin/minio
	@which bin/mc
	@which bin/tidb-lightning
	@which bin/sync_diff_inspector

generate_grafana_scripts:
	@cd metrics/grafana && mv tidb_summary.json tidb_summary.json.committed && ./generate_json.sh && diff -u tidb_summary.json.committed tidb_summary.json && rm tidb_summary.json.committed

bazel_ci_prepare:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) //:gazelle
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) //:gazelle -- update-repos -from_file=go.mod -to_macro DEPS.bzl%go_deps  -build_file_proto_mode=disable -prune
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  //cmd/mirror:mirror -- --mirror> tmp.txt
	mv tmp.txt DEPS.bzl
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  \
		--run_under="cd $(CURDIR) && " \
		 //tools/tazel:tazel

bazel_ci_simple_prepare:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) //:gazelle
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  \
		--run_under="cd $(CURDIR) && " \
		 //tools/tazel:tazel

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

bazel_ci_prepare_rbe:
	bazel run //:gazelle
	bazel run //:gazelle -- update-repos -from_file=go.mod -to_macro DEPS.bzl%go_deps  -build_file_proto_mode=disable -prune
	bazel run --//build:with_rbe_flag=true \
		--run_under="cd $(CURDIR) && " \
		 //tools/tazel:tazel

check-bazel-prepare:
	@echo "make bazel_prepare"
	./tools/check/check-bazel-prepare.sh

bazel_test: failpoint-enable bazel_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) test $(BAZEL_CMD_CONFIG) --build_tests_only --test_keep_going=false \
		--define gotags=deadlock,intest \
		-- //... -//cmd/... -//tests/graceshutdown/... \
		-//tests/globalkilltest/... -//tests/readonlytest/... -//br/pkg/task:task_test -//tests/realtikvtest/...


bazel_coverage_test: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) --nohome_rc coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --jobs=35 --build_tests_only --test_keep_going=false \
		--@io_bazel_rules_go//go/config:cover_format=go_cover --define gotags=deadlock,intest \
		-- //... -//cmd/... -//tests/graceshutdown/... \
		-//tests/globalkilltest/... -//tests/readonlytest/... -//br/pkg/task:task_test -//tests/realtikvtest/...

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

bazel_fail_build:  failpoint-enable bazel_ci_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) build $(BAZEL_CMD_CONFIG) \
		//...

bazel_clean:
	bazel $(BAZEL_GLOBAL_CONFIG) clean

bazel_junit:
	bazel_collect
	@mkdir -p $(TEST_COVERAGE_DIR)
	mv ./junit.xml `$(TEST_COVERAGE_DIR)/junit.xml`

bazel_golangcilinter:
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG) \
		--run_under="cd $(CURDIR) && " \
		@com_github_golangci_golangci_lint//cmd/golangci-lint:golangci-lint \
	-- run  $$($(PACKAGE_DIRECTORIES)) --config ./.golangci.yaml

bazel_brietest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/brietest/...
	./build/jenkins_collect_coverage.sh

bazel_pessimistictest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/pessimistictest/...
	./build/jenkins_collect_coverage.sh

bazel_sessiontest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/sessiontest/...
	./build/jenkins_collect_coverage.sh

bazel_statisticstest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/statisticstest/...
	./build/jenkins_collect_coverage.sh

bazel_txntest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/txntest/...
	./build/jenkins_collect_coverage.sh

bazel_addindextest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest/...
	./build/jenkins_collect_coverage.sh

bazel_addindextest1: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest1/...
	./build/jenkins_collect_coverage.sh

bazel_addindextest2: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest2/...
	./build/jenkins_collect_coverage.sh

bazel_addindextest3: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest3/...
	./build/jenkins_collect_coverage.sh

bazel_addindextest4: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/addindextest4/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
bazel_importintotest: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
bazel_importintotest2: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest2/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
bazel_importintotest3: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest3/...
	./build/jenkins_collect_coverage.sh

# on timeout, bazel won't print log sometimes, so we use --test_output=all to print log always
bazel_importintotest4: failpoint-enable bazel_ci_simple_prepare
	bazel $(BAZEL_GLOBAL_CONFIG) coverage $(BAZEL_CMD_CONFIG) $(BAZEL_INSTRUMENTATION_FILTER) --test_output=all --test_arg=-with-real-tikv --define gotags=deadlock,intest \
	--@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //tests/realtikvtest/importintotest4/...
	./build/jenkins_collect_coverage.sh

bazel_lint: bazel_prepare
	bazel build //... --//build:with_nogo_flag=true

docker:
	docker build -t "$(DOCKERPREFIX)tidb:latest" --build-arg 'GOPROXY=$(shell go env GOPROXY),' -f Dockerfile .

docker-test:
	docker buildx build --platform linux/amd64,linux/arm64 --push -t "$(DOCKERPREFIX)tidb:latest" --build-arg 'GOPROXY=$(shell go env GOPROXY),' -f Dockerfile .

bazel_mirror:
	$(eval $@TMP_OUT := $(shell mktemp -d -t tidbbzl.XXXXXX))
	bazel $(BAZEL_GLOBAL_CONFIG) run $(BAZEL_CMD_CONFIG)  //cmd/mirror:mirror -- --mirror> $($@TMP_OUT)/tmp.txt
	cp $($@TMP_OUT)/tmp.txt DEPS.bzl
	rm -rf $($@TMP_OUT)

bazel_sync:
	bazel $(BAZEL_GLOBAL_CONFIG) sync $(BAZEL_SYNC_CONFIG)

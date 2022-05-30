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

.PHONY: all clean test gotest server dev benchkv benchraw check checklist parser tidy ddltest build_br build_lightning build_lightning-ctl build_dumpling ut

default: server buildsucc

server-admin-check: server_check buildsucc

buildsucc:
	@echo Build TiDB Server successfully!

all: dev server benchkv

parser:
	@echo "remove this command later, when our CI script doesn't call it"

dev: checklist check explaintest gogenerate br_unit_test test_part_parser_dev ut
	@>&2 echo "Great, all tests passed."

# Install the check tools.
check-setup:tools/bin/revive tools/bin/goword

check: fmt check-parallel unconvert lint tidy testSuite check-static vet errdoc

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

goword:tools/bin/goword
	tools/bin/goword $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

check-static: tools/bin/golangci-lint
	GO111MODULE=on CGO_ENABLED=0 tools/bin/golangci-lint run -v $$($(PACKAGE_DIRECTORIES)) --config .golangci.yml

unconvert:tools/bin/unconvert
	@echo "unconvert check(skip check the generated or copied code in lightning)"
	@GO111MODULE=on tools/bin/unconvert $(UNCONVERT_PACKAGES)

gogenerate:
	@echo "go generate ./..."
	./tools/check/check-gogenerate.sh

errdoc:tools/bin/errdoc-gen
	@echo "generator errors.toml"
	./tools/check/check-errdoc.sh

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES_TIDB_TESTS)

vet:
	@echo "vet"
	$(GO) vet -all $(PACKAGES_TIDB_TESTS) 2>&1 | $(FAIL_ON_STDOUT)

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

# Split tests for CI to run `make test` in parallel.
test: test_part_1 test_part_2
	@>&2 echo "Great, all tests passed."

test_part_1: checklist explaintest

test_part_2: test_part_parser gotest gogenerate br_unit_test dumpling_unit_test

test_part_parser: parser_yacc test_part_parser_dev

test_part_parser_dev: parser_fmt parser_unit_test

parser_yacc:
	@cd parser && mv parser.go parser.go.committed && make parser && diff -u parser.go.committed parser.go && rm parser.go.committed

parser_fmt:
	@cd parser && make fmt

parser_unit_test:
	@cd parser && make test

test_part_br: br_unit_test br_integration_test

test_part_dumpling: dumpling_unit_test dumpling_integration_test

explaintest: server_check
	@cd cmd/explaintest && ./run-tests.sh -s ../../bin/tidb-server

ddltest:
	@cd cmd/ddltest && $(GO) test -o ../../bin/ddltest -c

CLEAN_UT_BINARY := find . -name '*.test.bin'| xargs rm

ut: tools/bin/ut tools/bin/xprog failpoint-enable
	tools/bin/ut $(X) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)
	@$(CLEAN_UT_BINARY)

gotest: failpoint-enable
	@echo "Running in native mode."
	@export log_level=info; export TZ='Asia/Shanghai'; \
	$(GOTEST) -ldflags '$(TEST_LDFLAGS)' $(EXTRA_TEST_ARGS) -timeout 20m -cover $(PACKAGES_TIDB_TESTS) -coverprofile=coverage.txt > gotest.log || { $(FAILPOINT_DISABLE); cat 'gotest.log'; exit 1; }
	@$(FAILPOINT_DISABLE)

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
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server tidb-server/main.go
else
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' tidb-server/main.go
endif

server_debug:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) -gcflags="all=-N -l" $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server-debug tidb-server/main.go
else
	CGO_ENABLED=1 $(GOBUILD) -gcflags="all=-N -l" $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' tidb-server/main.go
endif

server_check:
ifeq ($(TARGET), "")
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o bin/tidb-server tidb-server/main.go
else
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o '$(TARGET)' tidb-server/main.go
endif

linux:
ifeq ($(TARGET), "")
	GOOS=linux $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server-linux tidb-server/main.go
else
	GOOS=linux $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' tidb-server/main.go
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

tools/bin/megacheck: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/megacheck honnef.co/go/tools/cmd/megacheck

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/goword: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/goword github.com/chzchzchz/goword

tools/bin/unconvert: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/unconvert github.com/mdempsky/unconvert

tools/bin/failpoint-ctl: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

tools/bin/errdoc-gen: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/errdoc-gen github.com/pingcap/errors/errdoc-gen

tools/bin/golangci-lint:
	cd tools/check; \
	$(GO) build -o ../bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint
	# Build from source is not recommand. See https://golangci-lint.run/usage/install/
	# But the following script from their website doesn't work with Go1.18:
	# curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ./tools/bin v1.44.2

tools/bin/vfsgendev: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/vfsgendev github.com/shurcooL/vfsgen/cmd/vfsgendev

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
	$(GOTEST) -ldflags '$(TEST_LDFLAGS)' -cover github.com/pingcap/tidb/$(pkg) || { $(FAILPOINT_DISABLE); exit 1; }
endif
	@$(FAILPOINT_DISABLE)

# Collect the daily benchmark data.
# Usage:
#	make bench-daily TO=/path/to/file.json
bench-daily:
	go test github.com/pingcap/tidb/session -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/executor -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/tablecodec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/expression -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/util/rowcodec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/util/codec -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/distsql -run TestBenchDaily -bench Ignore --outfile bench_daily.json
	go test github.com/pingcap/tidb/util/benchdaily -run TestBenchDaily -bench Ignore \
		-date `git log -n1 --date=unix --pretty=format:%cd` \
		-commit `git log -n1 --pretty=format:%h` \
		-outfile $(TO)

build_tools: build_br build_lightning build_lightning-ctl

br_web:
	@cd br/web && npm install && npm run build

build_br:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(BR_BIN) br/cmd/br/*.go

build_lightning_for_web:
	CGO_ENABLED=1 $(GOBUILD) -tags dev $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_BIN) br/cmd/tidb-lightning/main.go

build_lightning:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_BIN) br/cmd/tidb-lightning/main.go

build_lightning-ctl:
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o $(LIGHTNING_CTL_BIN) br/cmd/tidb-lightning-ctl/main.go

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

br_compatibility_test_prepare:
	@cd br && tests/run_compatible.sh prepare

br_compatibility_test:
	@cd br && tests/run_compatible.sh run

# There is no FreeBSD environment for GitHub actions. So cross-compile on Linux
# but that doesn't work with CGO_ENABLED=1, so disable cgo. The reason to have
# cgo enabled on regular builds is performance.
ifeq ("$(GOOS)", "freebsd")
        GOBUILD  = CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags '$(LDFLAGS)'
endif

br_coverage:
	tools/bin/gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go|.*__failpoint_binding__.go" > "$(TEST_DIR)/all_cov.out"
ifeq ("$(JenkinsCI)", "1")
	tools/bin/goveralls -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
	grep -F '<option' "$(TEST_DIR)/all_cov.html"
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

dumpling_integration_test: dumpling_bins failpoint-enable build_dumpling
	@make failpoint-disable
	./dumpling/tests/run.sh $(CASE)

dumpling_tools:
	@echo "install dumpling tools..."
	@cd dumpling/tools && make

dumpling_tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --exit-code go.mod go.sum dumpling/tools/go.mod dumpling/tools/go.sum

dumpling_bins:
	@which bin/tidb-server
	@which bin/minio
	@which bin/tidb-lightning
	@which bin/sync_diff_inspector

tools/bin/gotestsum: tools/check/go.mod
	cd tools/check && $(GO) build -o ../bin/gotestsum gotest.tools/gotestsum

generate_grafana_scripts:
	@cd metrics/grafana && mv tidb_summary.json tidb_summary.json.committed && ./generate_json.sh && diff -u tidb_summary.json.committed tidb_summary.json && rm tidb_summary.json.committed

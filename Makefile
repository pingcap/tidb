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
# See the License for the specific language governing permissions and
# limitations under the License.

PROJECT=tidb
GOPATH ?= $(shell go env GOPATH)
P=8

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH))):$(PWD)/tools/bin
export PATH := $(path_to_add):$(PATH)

GO              := GO111MODULE=on go
GOBUILD         := $(GO) build $(BUILD_FLAG) -tags codes
GOBUILDCOVERAGE := GOPATH=$(GOPATH) cd tidb-server; $(GO) test -coverpkg="../..." -c .
GOTEST          := $(GO) test -p $(P)
OVERALLS        := GO111MODULE=on overalls
STATICCHECK     := GO111MODULE=on staticcheck
TIDB_EDITION    ?= Community

# Ensure TIDB_EDITION is set to Community or Enterprise before running build process.
ifneq "$(TIDB_EDITION)" "Community"
ifneq "$(TIDB_EDITION)" "Enterprise"
  $(error Please set the correct environment variable TIDB_EDITION before running `make`)
endif
endif

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"
PACKAGE_LIST  := go list ./...| grep -vE "cmd"
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")

FAILPOINT_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable)
FAILPOINT_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable)

LDFLAGS += -X "github.com/pingcap/parser/mysql.TiDBReleaseVersion=$(shell git describe --tags --dirty --always)"
LDFLAGS += -X "github.com/pingcap/tidb/util/versioninfo.TiDBBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb/util/versioninfo.TiDBGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb/util/versioninfo.TiDBGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb/util/versioninfo.TiDBEdition=$(TIDB_EDITION)"

TEST_LDFLAGS =  -X "github.com/pingcap/tidb/config.checkBeforeDropLDFlag=1"
COVERAGE_SERVER_LDFLAGS =  -X "github.com/pingcap/tidb/tidb-server.isCoverageServer=1"

CHECK_LDFLAGS += $(LDFLAGS) ${TEST_LDFLAGS}

TARGET = ""

# VB = Vector Benchmark
VB_FILE =
VB_FUNC =


.PHONY: all build update clean todo test gotest interpreter server dev benchkv benchraw check checklist parser tidy ddltest

default: server buildsucc

server-admin-check: server_check buildsucc

buildsucc:
	@echo Build TiDB Server successfully!

all: dev server benchkv

parser:
	@echo "remove this command later, when our CI script doesn't call it"

dev: checklist check test

build:
	$(GOBUILD)

# Install the check tools.
check-setup:tools/bin/revive tools/bin/goword tools/bin/gometalinter tools/bin/gosec

check: fmt errcheck lint tidy testSuite check-static vet staticcheck errdoc

# These need to be fixed before they can be ran regularly
check-fail: goword check-slow

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

goword:tools/bin/goword
	tools/bin/goword $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

gosec:tools/bin/gosec
	tools/bin/gosec $$($(PACKAGE_DIRECTORIES))

check-static: tools/bin/golangci-lint
	tools/bin/golangci-lint run -v --disable-all --deadline=3m \
	  --enable=misspell \
	  --enable=ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

check-slow:tools/bin/gometalinter tools/bin/gosec
	tools/bin/gometalinter --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))

errcheck:tools/bin/errcheck
	@echo "errcheck"
	@GO111MODULE=on tools/bin/errcheck -exclude ./tools/check/errcheck_excludes.txt -ignoretests -blank $(PACKAGES)

gogenerate:
	@echo "go generate ./..."
	./tools/check/check-gogenerate.sh

errdoc:tools/bin/errdoc-gen
	@echo "generator errors.toml"
	./tools/check/check-errdoc.sh

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

vet:
	@echo "vet"
	$(GO) vet -all $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

staticcheck:
	$(GO) get honnef.co/go/tools/cmd/staticcheck
	$(STATICCHECK) ./...

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

testSuite:
	@echo "testSuite"
	./tools/check/check_testSuite.sh

clean:
	$(GO) clean -i ./...
	rm -rf *.out
	rm -rf parser

# Split tests for CI to run `make test` in parallel.
test: test_part_1 test_part_2
	@>&2 echo "Great, all tests passed."

test_part_1: checklist explaintest

test_part_2: checkdep gotest gogenerate

explaintest: server
	@cd cmd/explaintest && ./run-tests.sh -s ../../bin/tidb-server

ddltest:
	@cd cmd/ddltest && $(GO) test -o ../../bin/ddltest -c

upload-coverage: SHELL:=/bin/bash
upload-coverage:
ifeq ("$(TRAVIS_COVERAGE)", "1")
	mv overalls.coverprofile coverage.txt
	bash <(curl -s https://codecov.io/bash)
endif

gotest: failpoint-enable
ifeq ("$(TRAVIS_COVERAGE)", "1")
	@echo "Running in TRAVIS_COVERAGE mode."
	$(GO) get github.com/go-playground/overalls
	@export log_level=error; \
	$(OVERALLS) -project=github.com/pingcap/tidb \
			-covermode=count \
			-ignore='.git,vendor,cmd,docs,LICENSES' \
			-concurrency=4 \
			-- -coverpkg=./... \
			|| { $(FAILPOINT_DISABLE); exit 1; }
else
	@echo "Running in native mode."
	@export log_level=fatal; export TZ='Asia/Shanghai'; \
	$(GOTEST) -ldflags '$(TEST_LDFLAGS)' -cover $(PACKAGES) -check.p true -check.timeout 4s || { $(FAILPOINT_DISABLE); exit 1; }
endif
	@$(FAILPOINT_DISABLE)

race: failpoint-enable
	@export log_level=debug; \
	$(GOTEST) -timeout 20m -race $(PACKAGES) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

leak: failpoint-enable
	@export log_level=debug; \
	$(GOTEST) -tags leak $(PACKAGES) || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

tikv_integration_test: failpoint-enable
	$(GOTEST) ./store/tikv/. -with-tikv=true || { $(FAILPOINT_DISABLE); exit 1; }
	@$(FAILPOINT_DISABLE)

RACE_FLAG =
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GOBUILD   = GOPATH=$(GOPATH) $(GO) build
endif

CHECK_FLAG =
ifeq ("$(WITH_CHECK)", "1")
	CHECK_FLAG = $(TEST_LDFLAGS)
endif

server:
ifeq ($(TARGET), "")
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server tidb-server/main.go
else
	CGO_ENABLED=1 $(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' tidb-server/main.go
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

checkdep:
	$(GO) list -f '{{ join .Imports "\n" }}' github.com/pingcap/tidb/store/tikv | grep ^github.com/pingcap/parser$$ || exit 0; exit 1

tools/bin/megacheck: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/megacheck honnef.co/go/tools/cmd/megacheck

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/goword: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/goword github.com/chzchzchz/goword

tools/bin/gometalinter: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/gometalinter gopkg.in/alecthomas/gometalinter.v3

tools/bin/gosec: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/gosec github.com/securego/gosec/cmd/gosec

tools/bin/errcheck: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/errcheck github.com/kisielk/errcheck

tools/bin/failpoint-ctl: go.mod
	$(GO) build -o $@ github.com/pingcap/failpoint/failpoint-ctl

tools/bin/misspell:tools/check/go.mod
	$(GO) get -u github.com/client9/misspell/cmd/misspell

tools/bin/ineffassign:tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/ineffassign github.com/gordonklaus/ineffassign

tools/bin/errdoc-gen: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/errdoc-gen github.com/pingcap/errors/errdoc-gen

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ./tools/bin v1.21.0

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
	$(GOTEST) -ldflags '$(TEST_LDFLAGS)' -cover github.com/pingcap/tidb/$(pkg) -check.p true -check.timeout 4s || { $(FAILPOINT_DISABLE); exit 1; }
endif
	@$(FAILPOINT_DISABLE)
<<<<<<< HEAD
=======

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

build_for_lightning_test:
	@make failpoint-enable
	$(GOTEST) -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tidb/br/... \
		-o $(LIGHTNING_BIN).test \
		github.com/pingcap/tidb/br/cmd/tidb-lightning
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

dumpling_bins:
	@which bin/tidb-server
	@which bin/minio
	@which bin/tidb-lightning
	@which bin/sync_diff_inspector

tools/bin/gotestsum: tools/check/go.mod
	cd tools/check && $(GO) build -o ../bin/gotestsum gotest.tools/gotestsum

generate_grafana_scripts:
	@cd metrics/grafana && mv tidb_summary.json tidb_summary.json.committed && ./generate_json.sh && diff -u tidb_summary.json.committed tidb_summary.json && rm tidb_summary.json.committed

bazel_ci_prepare:
	bazel --output_user_root=/home/jenkins/.tidb/tmp run --config=ci  //:gazelle

bazel_prepare:
	bazel run  //:gazelle

bazel_test: failpoint-enable bazel_ci_prepare
	bazel --output_user_root=/home/jenkins/.tidb/tmp test --config=ci  \
		-- //... -//cmd/... -//tests/graceshutdown/... \
		-//tests/globalkilltest/... -//tests/readonlytest/... -//br/pkg/task:task_test


bazel_coverage_test: failpoint-enable bazel_ci_prepare
	bazel --output_user_root=/home/jenkins/.tidb/tmp coverage --config=ci --@io_bazel_rules_go//go/config:cover_format=go_cover \
		-- //... -//cmd/... -//tests/graceshutdown/... \
		-//tests/globalkilltest/... -//tests/readonlytest/... -//br/pkg/task:task_test

bazel_build: bazel_ci_prepare
	mkdir -p bin
	bazel --output_user_root=/home/jenkins/.tidb/tmp build --config=ci  //tidb-server/... //br/cmd/... //cmd/...
	cp bazel-out/k8-fastbuild/bin/tidb-server/tidb-server_/tidb-server ./bin
	cp bazel-out/k8-fastbuild/bin/cmd/importer/importer_/importer      ./bin
	cp bazel-out/k8-fastbuild/bin/tidb-server/tidb-server-check_/tidb-server-check ./bin

bazel_fail_build:  failpoint-enable bazel_ci_prepare
	bazel --output_user_root=/home/jenkins/.tidb/tmp build --config=ci  //...

bazel_clean:
	bazel --output_user_root=/home/jenkins/.tidb/tmp clean

bazel_junit:
	bazel_collect
	@mkdir -p $(TEST_COVERAGE_DIR)
	mv ./junit.xml `$(TEST_COVERAGE_DIR)/junit.xml`
>>>>>>> 674def3b7... lightning: sample files and pre-allocate rowID before restoring chunk (#34288)

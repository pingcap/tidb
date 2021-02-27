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

include Makefile.common

.PHONY: all clean test gotest server dev benchkv benchraw check checklist parser tidy ddltest

default: server buildsucc

server-admin-check: server_check buildsucc

buildsucc:
	@echo Build TiDB Server successfully!

all: dev server benchkv

parser:
	@echo "remove this command later, when our CI script doesn't call it"

dev: checklist check test

# Install the check tools.
check-setup:tools/bin/revive tools/bin/goword tools/bin/gometalinter tools/bin/gosec

check: fmt errcheck unconvert lint tidy testSuite check-static vet staticcheck errdoc

# These need to be fixed before they can be ran regularly
check-fail: goword check-slow

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@cd cmd/importcheck && $(GO) run . ../..

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

unconvert:tools/bin/unconvert
	@echo "unconvert check"
	@GO111MODULE=on tools/bin/unconvert ./...

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

clean: failpoint-disable
	$(GO) clean -i ./...

# Split tests for CI to run `make test` in parallel.
test: test_part_1 test_part_2
	@>&2 echo "Great, all tests passed."

test_part_1: checklist explaintest

test_part_2: checkdep gotest gogenerate

explaintest: server_check
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
	@export log_level=info; \
	$(OVERALLS) -project=github.com/pingcap/tidb \
			-covermode=count \
			-ignore='.git,vendor,cmd,docs,tests,LICENSES' \
			-concurrency=4 \
			-- -coverpkg=./... \
			|| { $(FAILPOINT_DISABLE); exit 1; }
else
	@echo "Running in native mode."
	@export log_level=info; export TZ='Asia/Shanghai'; \
	$(GOTEST) -ldflags '$(TEST_LDFLAGS)' $(EXTRA_TEST_ARGS) -cover $(PACKAGES) -check.p true -check.timeout 4s || { $(FAILPOINT_DISABLE); exit 1; }
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

tools/bin/unconvert: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/unconvert github.com/mdempsky/unconvert

tools/bin/failpoint-ctl: go.mod
	$(GO) build -o $@ github.com/pingcap/failpoint/failpoint-ctl

tools/bin/errdoc-gen: go.mod
	$(GO) build -o $@ github.com/pingcap/errors/errdoc-gen

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b ./tools/bin v1.29.0

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

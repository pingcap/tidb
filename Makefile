PROJECT=tidb
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH))):$(PWD)/tools/bin
export PATH := $(path_to_add):$(PATH)

GO        := GO111MODULE=on go
GOBUILD   := CGO_ENABLED=1 $(GO) build $(BUILD_FLAG) -tags codes
GOBUILDCOVERAGE := GOPATH=$(GOPATH) CGO_ENABLED=1 cd tidb-server; $(GO) test -coverpkg="../..." -c .
GOTEST    := CGO_ENABLED=1 $(GO) test -p 4
OVERALLS  := CGO_ENABLED=1 GO111MODULE=on overalls

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"
PACKAGE_LIST  := go list ./...| grep -vE "cmd" | grep -vE "test"
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")

FAILPOINT_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl enable)
FAILPOINT_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs tools/bin/failpoint-ctl disable)

LDFLAGS += -X "github.com/pingcap/parser/mysql.TiDBReleaseVersion=$(shell git describe --tags --dirty --always)"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.GoVersion=$(shell go version)"

TEST_LDFLAGS =  -X "github.com/pingcap/tidb/config.checkBeforeDropLDFlag=1"
COVERAGE_SERVER_LDFLAGS =  -X "github.com/pingcap/tidb/tidb-server.isCoverageServer=1"

CHECK_LDFLAGS += $(LDFLAGS) ${TEST_LDFLAGS}

TARGET = ""

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

check: fmt errcheck lint tidy testSuite check-static vet

# These need to be fixed before they can be ran regularly
check-fail: goword check-slow

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

goword:tools/bin/goword
	tools/bin/goword $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

gosec:tools/bin/gosec
	tools/bin/gosec $$($(PACKAGE_DIRECTORIES))

check-static:tools/bin/gometalinter tools/bin/misspell tools/bin/ineffassign
	@ # TODO: enable megacheck.
	@ # TODO: gometalinter has been DEPRECATED.
	@ # https://github.com/alecthomas/gometalinter/issues/590
	tools/bin/gometalinter --disable-all --deadline 120s \
	  --enable misspell \
	  --enable ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

check-slow:tools/bin/gometalinter tools/bin/gosec
	tools/bin/gometalinter --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))

errcheck:tools/bin/errcheck
	@echo "errcheck"
	@GO111MODULE=on tools/bin/errcheck -exclude ./tools/check/errcheck_excludes.txt -blank $(PACKAGES) | grep -v "_test\.go" | awk '{print} END{if(NR>0) {exit 1}}'

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

vet:
	@echo "vet"
	$(GO) vet -all $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

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

test: checklist checkdep gotest explaintest

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
	@export log_level=error; \
	$(GOTEST) -ldflags '$(TEST_LDFLAGS)' -cover $(PACKAGES) || { $(FAILPOINT_DISABLE); exit 1; }
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
	GOBUILD   = GOPATH=$(GOPATH) CGO_ENABLED=1 $(GO) build
endif

CHECK_FLAG =
ifeq ("$(WITH_CHECK)", "1")
	CHECK_FLAG = $(TEST_LDFLAGS)
endif

server:
ifeq ($(TARGET), "")
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o bin/tidb-server tidb-server/main.go
else
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(LDFLAGS) $(CHECK_FLAG)' -o '$(TARGET)' tidb-server/main.go
endif

server_check:
ifeq ($(TARGET), "")
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o bin/tidb-server tidb-server/main.go
else
	$(GOBUILD) $(RACE_FLAG) -ldflags '$(CHECK_LDFLAGS)' -o '$(TARGET)' tidb-server/main.go
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

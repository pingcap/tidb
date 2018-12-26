PROJECT=tidb
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

GO        := GO111MODULE=on go
GOBUILD   := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST    := CGO_ENABLED=1 $(GO) test -p 3
OVERALLS  := CGO_ENABLED=1 overalls
GOVERALLS := goveralls

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"
PACKAGE_LIST  := go list ./...
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")

GOFAIL_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs gofail enable)
GOFAIL_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs gofail disable)

LDFLAGS += -X "github.com/pingcap/parser/mysql.TiDBReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.GoVersion=$(shell go version)"

TEST_LDFLAGS =  -X "github.com/pingcap/tidb/config.checkBeforeDropLDFlag=1"

CHECK_LDFLAGS += $(LDFLAGS) ${TEST_LDFLAGS}

TARGET = ""

.PHONY: all build update clean todo test gotest interpreter server dev benchkv benchraw check checklist parser tidy

default: server buildsucc

server-admin-check: server_check buildsucc

buildsucc:
	@echo Build TiDB Server successfully!

all: dev server benchkv

parser:
	@echo "remove this command later, when our CI script doesn't call it"

dev: checklist test check

build:
	$(GOBUILD)

# Install the check tools.
check-setup:tools/bin/megacheck tools/bin/revive tools/bin/goword tools/bin/gometalinter tools/bin/gosec

check: fmt errcheck lint tidy

# These need to be fixed before they can be ran regularly
check-fail: goword check-static check-slow

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

goword:tools/bin/goword
	tools/bin/goword $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

gosec:tools/bin/gosec
	tools/bin/gosec $$($(PACKAGE_DIRECTORIES))

check-static:tools/bin/gometalinter
	@ # vet and fmt have problems with vendor when ran through metalinter
	tools/bin/gometalinter --disable-all --deadline 120s \
	  --enable misspell \
	  --enable megacheck \
	  --enable ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

check-slow:tools/bin/gometalinter tools/bin/gosec
	tools/bin/gometalinter --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))

errcheck:tools/bin/errcheck
	@echo "errcheck"
	@$(GO) mod vendor
	@tools/bin/errcheck -exclude ./tools/check/errcheck_excludes.txt -blank $(PACKAGES) | grep -v "_test\.go" | awk '{print} END{if(NR>0) {exit 1}}'

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

vet:
	@echo "vet"
	$(GO) vet -all -shadow $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

clean:
	$(GO) clean -i ./...
	rm -rf *.out
	rm -rf parser

test: checklist checkdep gotest explaintest

explaintest: server
	@cd cmd/explaintest && ./run-tests.sh -s ../../bin/tidb-server

gotest:
	@rm -rf $GOPATH/bin/gofail
	$(GO) get github.com/pingcap/gofail
	@which gofail
	@$(GOFAIL_ENABLE)
ifeq ("$(TRAVIS_COVERAGE)", "1")
	@echo "Running in TRAVIS_COVERAGE mode."
	@export log_level=error; \
	go get github.com/go-playground/overalls
	go get github.com/mattn/goveralls
	$(OVERALLS) -project=github.com/pingcap/tidb -covermode=count -ignore='.git,vendor,cmd,docs,LICENSES' || { $(GOFAIL_DISABLE); exit 1; }
	$(GOVERALLS) -service=travis-ci -coverprofile=overalls.coverprofile || { $(GOFAIL_DISABLE); exit 1; }
else
	@echo "Running in native mode."
	@export log_level=error; \
	$(GOTEST) -ldflags '$(TEST_LDFLAGS)' -cover $(PACKAGES) || { $(GOFAIL_DISABLE); exit 1; }
endif
	@$(GOFAIL_DISABLE)

race:
	$(GO) get github.com/pingcap/gofail
	@$(GOFAIL_ENABLE)
	@export log_level=debug; \
	$(GOTEST) -timeout 20m -race $(PACKAGES) || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

leak:
	$(GO) get github.com/pingcap/gofail
	@$(GOFAIL_ENABLE)
	@export log_level=debug; \
	$(GOTEST) -tags leak $(PACKAGES) || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

tikv_integration_test:
	$(GO) get github.com/pingcap/gofail
	@$(GOFAIL_ENABLE)
	$(GOTEST) ./store/tikv/. -with-tikv=true || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

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

gofail-enable:
# Converting gofail failpoints...
	@$(GOFAIL_ENABLE)

gofail-disable:
# Restoring gofail failpoints...
	@$(GOFAIL_DISABLE)

checkdep:
	$(GO) list -f '{{ join .Imports "\n" }}' github.com/pingcap/tidb/store/tikv | grep ^github.com/pingcap/parser$$ || exit 0; exit 1

tools/bin/megacheck:
	cd tools/check; \
	$go build -o ../bin/megacheck honnef.co/go/tools/cmd/megacheck

tools/bin/revive:
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/goword:
	cd tools/check; \
	$(GO) build -o ../bin/goword github.com/chzchzchz/goword

tools/bin/gometalinter:
	cd tools/check; \
	$(GO) build -o ../bin/gometalinter gopkg.in/alecthomas/gometalinter.v2

tools/bin/gosec:
	cd tools/check; \
	$(GO) build -o ../bin/gosec github.com/securego/gosec/cmd/gosec

tools/bin/errcheck:
	cd tools/check; \
	$(GO) build -o ../bin/errcheck github.com/kisielk/errcheck

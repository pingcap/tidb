.PHONY: build importer checker dump_region binlogctl sync_diff_inspector test check deps

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
	$(error Please set the environment variable GOPATH before running `make`)
endif


CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)


LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.Version=1.0.0~rc2+git.$(shell git rev-parse --short HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-tools/pkg/utils.GitHash=$(shell git rev-parse HEAD)"

CURDIR   := $(shell pwd)
GO       := GO15VENDOREXPERIMENT="1" go
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3
PACKAGES := $$(go list ./... | grep -vE 'vendor')
FILES     := $$(find . -name '*.go' -type f | grep -vE 'vendor')


build: check test importer checker dump_region binlogctl sync_diff_inspector

importer:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/importer ./importer

checker:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/checker ./checker

dump_region:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/dump_region ./dump_region

binlogctl:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/binlogctl ./tidb-binlog/binlogctl

sync_diff_inspector:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/sync_diff_inspector ./sync_diff_inspector

test:
	@export log_level=error; \
	$(GOTEST) -cover $(PACKAGES)

fmt:
	go fmt ./...
	@goimports -w $(FILES)

check:
	go get github.com/golang/lint/golint
	@echo "vet"
	@ go tool vet $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "vet --shadow"
	@ go tool vet --shadow $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	#@echo "golint"
	#@ golint ./... 2>&1 | grep -vE '\.pb\.go' | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
ifdef PKG
	glide get -s -v --skip-test ${PKG}
else
	glide update -s -v -u --skip-test
endif
	@echo "removing test files"
	glide vc --use-lock-file --only-code --no-tests


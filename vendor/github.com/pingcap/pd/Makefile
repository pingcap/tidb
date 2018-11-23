PD_PKG := github.com/pingcap/pd

TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     uniq | sed -e "s/^\./github.com\/pingcap\/pd/")
BASIC_TEST_PKGS := $(filter-out github.com/pingcap/pd/pkg/integration_test,$(TEST_PKGS))

PACKAGES := go list ./...
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's|github.com/pingcap/pd/||'
GOCHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'
RETOOL:= ./hack/retool

GOFAIL_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|vendor)" | xargs ./hack/retool do gofail enable)
GOFAIL_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|vendor)" | xargs ./hack/retool do gofail disable)

LDFLAGS += -X "$(PD_PKG)/server.PDReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(PD_PKG)/server.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(PD_PKG)/server.PDGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(PD_PKG)/server.PDGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

GOMOD := -mod=vendor

GOVER_MAJOR := $(shell go version | sed -e "s/.*go\([0-9]\+\)[.]\([0-9]\+\).*/\1/")
GOVER_MINOR := $(shell go version | sed -e "s/.*go\([0-9]\+\)[.]\([0-9]\+\).*/\2/")
GO111 := $(shell [ $(GOVER_MAJOR) -gt 1 ] || [ $(GOVER_MAJOR) -eq 1 ] && [ $(GOVER_MINOR) -ge 11 ]; echo $$?)
ifeq ($(GO111), 1)
$(warning "go below 1.11 does not support modules")
GOMOD :=
endif

# Ignore following files's coverage.
#
# See more: https://godoc.org/path/filepath#Match
COVERIGNORE := "cmd/*/*,pdctl/*,pdctl/*/*,server/api/bindata_assetfs.go"

default: build

all: dev

dev: build check test

ci: build check basic-test

build: export GO111MODULE=on
build:
ifeq ("$(WITH_RACE)", "1")
	CGO_ENABLED=1 go build $(GOMOD) -race -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
else
	CGO_ENABLED=0 go build $(GOMOD) -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
endif
	CGO_ENABLED=0 go build $(GOMOD) -ldflags '$(LDFLAGS)' -o bin/pd-ctl tools/pd-ctl/main.go
	CGO_ENABLED=0 go build $(GOMOD) -o bin/pd-tso-bench tools/pd-tso-bench/main.go
	CGO_ENABLED=0 go build $(GOMOD) -o bin/pd-recover tools/pd-recover/main.go

test: retool-setup
	# testing..
	@$(GOFAIL_ENABLE)
	CGO_ENABLED=1 GO111MODULE=on go test $(GOMOD) -race -cover $(TEST_PKGS) || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

basic-test:
	@$(GOFAIL_ENABLE)
	GO111MODULE=on go test $(GOMOD) $(BASIC_TEST_PKGS) || { $(GOFAIL_DISABLE); exit 1; }
	@$(GOFAIL_DISABLE)

# These need to be fixed before they can be ran regularly
check-fail:
	CGO_ENABLED=0 ./hack/retool do gometalinter.v2 --disable-all \
	  --enable errcheck \
	  $$($(PACKAGE_DIRECTORIES))
	CGO_ENABLED=0 ./hack/retool do gosec $$($(PACKAGE_DIRECTORIES))

check-all: static lint
	@echo "checking"

retool-setup: export GO111MODULE=off
retool-setup: 
	@which retool >/dev/null 2>&1 || go get github.com/twitchtv/retool
	@./hack/retool sync

check: retool-setup check-all

static:
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	gofmt -s -l $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)
	./hack/retool do govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)

	CGO_ENABLED=0 ./hack/retool do gometalinter.v2 --disable-all --deadline 120s \
	  --enable misspell \
	  --enable megacheck \
	  --enable ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

lint:
	@echo "linting"
	CGO_ENABLED=0 ./hack/retool do revive -formatter friendly -config revive.toml $$($(PACKAGES))

travis_coverage:
ifeq ("$(TRAVIS_COVERAGE)", "1")
	GOPATH=$(VENDOR) $(HOME)/gopath/bin/goveralls -service=travis-ci -ignore $(COVERIGNORE)
else
	@echo "coverage only runs in travis."
endif

vendor:
	GO111MODULE=on go mod vendor
	bash ./hack/clean_vendor.sh

simulator:
	CGO_ENABLED=0 go build -o bin/pd-simulator tools/pd-simulator/main.go

clean-test:
	rm -rf /tmp/test_pd*
	rm -rf /tmp/pd-integration-test*
	rm -rf /tmp/test_etcd*

gofail-enable:
	# Converting gofail failpoints...
	@$(GOFAIL_ENABLE)

gofail-disable:
	# Restoring gofail failpoints...
	@$(GOFAIL_DISABLE)

.PHONY: all ci vendor clean-test

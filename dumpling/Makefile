PACKAGES := go list ./... | sed '/github.com\/pingcap\/dumpling\/bin/d'
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's/github.com\/pingcap\/dumpling\/*//'
DUMPLING_PKG := github.com/pingcap/dumpling
CHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.ReleaseVersion=$(shell git describe --tags --dirty='-dev')"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.BuildTimestamp=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GoVersion=$(shell go version)"

GOBUILD := CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags '$(LDFLAGS)'
GOTEST  := CGO_ENABLED=1 GO111MODULE=on go test -ldflags '$(LDFLAGS)'

ifeq ("$(WITH_RACE)", "1")
	RACEFLAG = -race
endif

all: build check test

build: bin/dumpling

bin/%: cmd/%/main.go $(wildcard v4/**/*.go)
	$(GOBUILD) $(RACEFLAG) -tags codes -o $@ $<

test: failpoint-enable
	$(GOTEST) $(RACEFLAG) -coverprofile=coverage.txt -covermode=atomic -tags leak ./... || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

integration_test: bins failpoint-enable bin/dumpling
	@make failpoint-disable
	./tests/run.sh $(CASE)

tools:
	@echo "install tools..."
	@cd tools && make

failpoint-enable: tools
	tools/bin/failpoint-ctl enable

failpoint-disable: tools
	tools/bin/failpoint-ctl disable

check:
	@# Tidy first to avoid go.mod being affected by static and lint
	@make tidy
	@# Build tools for static and lint
	@make tools static lint

static: export GO111MODULE=on
static: tools
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	tools/bin/govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(CHECKER)

	@# why some lints are disabled?
	@#   gochecknoglobals - disabled because we do use quite a lot of globals
	@#              gofmt - ditto
	@#                gci - ditto
	@#                wsl - too pedantic about the formatting
	@#             funlen - PENDING REFACTORING
	@#           gocognit - PENDING REFACTORING
	@#              godox - TODO
	@#              gomnd - too many magic numbers, and too pedantic (even 2*x got flagged...)
	@#        testpackage - several test packages still rely on private functions
	@#             nestif - PENDING REFACTORING
	@#           goerr113 - it mistaken pingcap/errors with standard errors
	@#                lll - pingcap/errors may need to write a long line
	@#       paralleltest - no need to run test parallel
	@#           nlreturn - no need to ensure a new line before continue or return
	@#   exhaustivestruct - Protobuf structs have hidden fields, like "XXX_NoUnkeyedLiteral"
	@#         exhaustive - no need to check exhaustiveness of enum switch statements
	@#              gosec - too many false positive
	@#          errorlint - can't detect errors.Cause
	@#      sqlclosecheck - the rows in dumpling is created in one function but closed in other functions
	CGO_ENABLED=0 tools/bin/golangci-lint run --enable-all --deadline 180s \
		--disable gochecknoglobals \
		--disable gofmt \
		--disable gci \
		--disable wsl \
		--disable funlen \
		--disable gocognit \
		--disable godox \
		--disable gomnd \
		--disable testpackage \
		--disable nestif \
		--disable goerr113 \
		--disable lll \
		--disable paralleltest \
		--disable nlreturn \
		--disable exhaustivestruct \
		--disable exhaustive \
		--disable godot \
		--disable gosec \
		--disable errorlint \
		--disable sqlclosecheck \
		--disable scopelint \
		--disable gofumpt \
		--disable interfacer \
		$$($(PACKAGE_DIRECTORIES))
	# pingcap/errors APIs are mixed with multiple patterns 'pkg/errors',
	# 'juju/errors' and 'pingcap/parser'. To avoid confusion and mistake,
	# we only allow a subset of APIs, that's "Normalize|Annotate|Trace|Cause".
	@# TODO: delete Errorf and New after we support standard code.
	@# TODO: allow more APIs when we need to support "workaound".
	grep -Rn --exclude="*_test.go" -E "(\t| )errors\.[A-Z]" cmd v4 | \
		grep -vE "Normalize|Annotate|Trace|Cause|Errorf|New" 2>&1 | $(CHECKER)

lint: tools
	@echo "linting"
	CGO_ENABLED=0 tools/bin/revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --exit-code go.mod go.sum tools/go.mod tools/go.sum

bins:
	@which bin/tidb-server
	@which bin/minio
	@which bin/tidb-lightning
	@which bin/sync_diff_inspector

.PHONY: build test integration_test tools failpoint-enable failpoint-disable check static lint tidy bins

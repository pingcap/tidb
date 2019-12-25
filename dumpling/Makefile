LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.BuildTimestamp=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GoVersion=$(shell go version)"

GO = go
GOFLAGS = -ldflags '$(LDFLAGS)'
ifeq ("$(WITH_RACE)", "1")
	GOFLAGS += -race
endif

.PHONY: build test

build: bin/dumpling

bin/%: cmd/%/main.go $(wildcard v4/**/*.go)
	$(GO) build $(GOFLAGS) -tags codes -o $@ $<

test:
	$(GO) list ./... | xargs $(GO) test $(GOFLAGS)

LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.BuildTimestamp=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/pingcap/dumpling/v4/cli.GoVersion=$(shell go version)"

FAILPOINT_ENABLE  := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs bin/failpoint-ctl enable)
FAILPOINT_DISABLE := $$(find $$PWD/ -type d | grep -vE "(\.git|tools)" | xargs bin/failpoint-ctl disable)

GO = go
GOLDFLAGS = -ldflags '$(LDFLAGS)'
ifeq ("$(WITH_RACE)", "1")
	GOLDFLAGS += -race
endif

.PHONY: build test

build: bin/dumpling

bin/%: cmd/%/main.go $(wildcard v4/**/*.go)
	$(GO) build $(GOLDFLAGS) -tags codes -o $@ $<

test: failpoint-enable
	$(GO) list ./... | xargs $(GO) test $(GOLDFLAGS) -coverprofile=coverage.txt -covermode=atomic  ||{ $(FAILPOINT_DISABLE); exit 1; }
	@make failpoint-disable

integration_test: failpoint-enable bin/dumpling
	@make failpoint-disable
	./tests/run.sh ||{ $(FAILPOINT_DISABLE); exit 1; }

bin/failpoint-ctl: go.mod
	$(GO) build -o $@ github.com/pingcap/failpoint/failpoint-ctl

failpoint-enable: bin/failpoint-ctl
# Converting gofail failpoints...
	@$(FAILPOINT_ENABLE)

failpoint-disable: bin/failpoint-ctl
# Restoring gofail failpoints...
	@$(FAILPOINT_DISABLE)

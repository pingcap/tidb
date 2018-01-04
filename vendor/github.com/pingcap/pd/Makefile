PD_PKG := github.com/pingcap/pd
VENDOR := $(shell rm -rf _vendor/src/$(PD_PKG) &&\
                 ln -s `pwd` _vendor/src/$(PD_PKG) &&\
                 pwd)/_vendor
TEST_PKGS := $(shell find . -iname "*_test.go" -exec dirname {} \; | \
                     uniq | sed -e "s/^\./github.com\/pingcap\/pd/")

GOFILTER := grep -vE 'vendor|testutil'
GOCHECKER := $(GOFILTER) | awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "$(PD_PKG)/server.PDReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(PD_PKG)/server.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(PD_PKG)/server.PDGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(PD_PKG)/server.PDGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

# Ignore following files's coverage.
#
# See more: https://godoc.org/path/filepath#Match
COVERIGNORE := "cmd/*/*,pdctl/*,pdctl/*/*,server/api/bindata_assetfs.go"

default: build

all: dev

dev: build simulator check test

build:
ifeq ("$(WITH_RACE)", "1")
	GOPATH=$(VENDOR) CGO_ENABLED=1 go build -race -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
else
	GOPATH=$(VENDOR) CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
endif
	GOPATH=$(VENDOR) CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o bin/pd-ctl cmd/pd-ctl/main.go
	GOPATH=$(VENDOR) CGO_ENABLED=0 go build -o bin/pd-tso-bench cmd/pd-tso-bench/main.go
	GOPATH=$(VENDOR) CGO_ENABLED=0 go build -o bin/pd-recover cmd/pd-recover/main.go

test:
	# testing..
	@GOPATH=$(VENDOR) CGO_ENABLED=1 go test -race -cover $(TEST_PKGS)

check:
	go get github.com/golang/lint/golint

	@echo "vet"
	@ go tool vet . 2>&1 | $(GOCHECKER)
	@ go tool vet --shadow . 2>&1 | $(GOCHECKER)
	@echo "golint"
	@ golint ./... 2>&1 | $(GOCHECKER)
	@echo "gofmt"
	@ gofmt -s -l . 2>&1 | $(GOCHECKER)

travis_coverage:
ifeq ("$(TRAVIS_COVERAGE)", "1")
	GOPATH=$(VENDOR) $(HOME)/gopath/bin/goveralls -service=travis-ci -ignore $(COVERIGNORE)
else
	@echo "coverage only runs in travis."
endif

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -rf vendor && mv _vendor/src vendor || true
	rm -rf _vendor
ifdef PKG
	glide get --strip-vendor --skip-test ${PKG}
else
	glide update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/src

simulator:
	GOPATH=$(VENDOR) CGO_ENABLED=0 go build -o bin/simulator cmd/simulator/main.go

.PHONY: update clean

GO=GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 go
GOTEST=GO15VENDOREXPERIMENT="1" CGO_ENABLED=1 go test # go race detector requires cgo

PACKAGES := $$(go list ./...| grep -vE 'vendor|pd-server')

GOFILTER := grep -vE 'vendor|render.Delims|bindata_assetfs|testutil'
GOCHECKER := $(GOFILTER) | awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "github.com/pingcap/pd/server.PDBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/pd/server.PDGitHash=$(shell git rev-parse HEAD)"

RACE_FLAG =
ifeq ("$(WITH_RACE)", "1")
	RACE_FLAG = -race
	GO = GO15VENDOREXPERIMENT="1" CGO_ENABLED=1 go
endif

# Ignore following files's coverage.
#
# See more: https://godoc.org/path/filepath#Match
COVERIGNORE := "cmd/*/*,pdctl/*,pdctl/*/*,server/api/bindata_assetfs.go"

default: build

all: dev install

dev: build check test

build:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) build $(RACE_FLAG) -ldflags '$(LDFLAGS)' -o bin/pd-server cmd/pd-server/main.go
	$(GO) build -o bin/pd-ctl cmd/pd-ctl/main.go
	$(GO) build -o bin/pd-tso-bench cmd/pd-tso-bench/main.go
	$(GO) build -o bin/pd-recover cmd/pd-recover/main.go
	rm -rf vendor

install:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GO) install ./...
	rm -rf vendor

test:
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(GOTEST) --race $(PACKAGES)
	rm -rf vendor

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
	rm -rf vendor && ln -s _vendor/vendor vendor
	$(HOME)/gopath/bin/goveralls -service=travis-ci -ignore $(COVERIGNORE)
	rm -rf vendor
else
	@echo "coverage only runs in travis."
endif

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -rf vendor && mv _vendor/vendor vendor || true
	rm -rf _vendor
ifdef PKG
	glide get --strip-vendor --skip-test ${PKG}
else
	glide update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/vendor

clean:
	# clean unix socket
	find . -type s -exec rm {} \;

.PHONY: update clean

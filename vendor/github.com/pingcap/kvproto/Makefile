### Makefile for kvproto

CURDIR := $(shell pwd)

KEEP_FILE := '**/*.proto,**/*.sh'

export PATH := $(CURDIR)/_vendor/bin:$(PATH)
export GOPATH := $(GOPATH):$(CURDIR)/_vendor

all: go rust test

test:
	# Build test.
	go build ./pkg/...
	cargo check

go: link_gopath_src
	# Standalone GOPATH
	GOPATH=$(CURDIR)/_vendor ./generate_go.sh

rust: link_gopath_src
	GOPATH=$(CURDIR)/_vendor ./generate_rust.sh

link_gopath_src:
	rm -f _vendor/src
	ln -s ./vendor _vendor/src

update_go_pkg:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -rf vendor && mv _vendor vendor || true
	rm -rf _vendor
ifdef PKG
	glide get --strip-vendor --skip-test ${PKG}
else
	glide update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests --use-lock-file --keep $(KEEP_FILE)
	mkdir -p _vendor
	mv vendor _vendor

.PHONY: update_go_pkg link_gopath_src all

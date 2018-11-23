### Makefile for kvproto

CURDIR := $(shell pwd)

KEEP_FILE := '**/*.proto,**/*.sh'

export PATH := $(CURDIR)/_vendor/bin:$(PATH)
export GOPATH := $(CURDIR)/_vendor

all: go rust

go:
	# Standalone GOPATH
	./generate_go.sh
	go build ./pkg/...

rust:
	./generate_rust.sh
	cargo check

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

.PHONY: update_go_pkg all

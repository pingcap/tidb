# build rules.
GO=go

.PHONY: all build install clean

all: build install

build:
	$(GO) build

install:
	$(GO) install ./...

clean:
	$(GO) clean -i ./...

test: 
	$(GO) test -cover ./...

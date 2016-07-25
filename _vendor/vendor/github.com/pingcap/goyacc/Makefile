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
	rm -f *~ y.output y.go tmp.go goyacc

test: 
	$(GO) test -cover ./...

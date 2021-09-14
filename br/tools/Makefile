all: bin/gofumports bin/govet bin/revive bin/golangci-lint \
	bin/failpoint-ctl bin/errdoc-gen bin/vfsgendev bin/protoc-gen-gogofaster \
	bin/gocovmerge bin/goveralls

bin/gofumports:
	go build -o $@ mvdan.cc/gofumpt/gofumports

bin/govet:
	go build -o $@ github.com/dnephin/govet

bin/revive:
	go build -o $@ github.com/mgechev/revive

bin/golangci-lint:
	go build -o $@ github.com/golangci/golangci-lint/cmd/golangci-lint

bin/failpoint-ctl:
	go build -o $@ github.com/pingcap/failpoint/failpoint-ctl

bin/errdoc-gen:
	go build -o $@ github.com/pingcap/errors/errdoc-gen

bin/vfsgendev:
	go build -o $@ github.com/shurcooL/vfsgen/cmd/vfsgendev

bin/protoc-gen-gogofaster:
	go build -o $@ github.com/gogo/protobuf/protoc-gen-gogofaster

bin/gocovmerge:
	go build -o $@ github.com/wadey/gocovmerge

bin/goveralls:
	go build -o $@ github.com/mattn/goveralls


all: bin/govet bin/revive bin/golangci-lint bin/failpoint-ctl

bin/govet:
	go build -o $@ github.com/dnephin/govet

bin/revive:
	go build -o $@ github.com/mgechev/revive

bin/golangci-lint:
	go build -o $@ github.com/golangci/golangci-lint/cmd/golangci-lint

bin/failpoint-ctl:
	go build -o $@ github.com/pingcap/failpoint/failpoint-ctl


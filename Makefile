# tidb build rules.
ARCH="`uname -s`"

LINUX="Linux"
MAC="Darwin"

GO=godep go

LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb/util/printer.TiDBGitHash=$(shell git rev-parse HEAD)"

TARGET = ""

.PHONY: godep deps all build install parser clean todo test gotest interpreter server

all: godep parser ast-parser build test check

godep:
	go get github.com/tools/godep

build:
	$(GO) build

install:
	$(GO) install ./...

parser:
	go get github.com/qiuyesuifeng/goyacc
	go get github.com/qiuyesuifeng/golex
	a=`mktemp temp.XXXXXX`; \
	goyacc -o /dev/null -xegen $$a parser/parser.y; \
	goyacc -o parser/parser.go -xe $$a parser/parser.y 2>&1 | grep "shift/reduce" | awk '{print} END {if (NR > 0) {print "Find conflict in parser.y. Please check y.output for more information."; exit 1;}}';  \
	rm -f $$a; \
	rm -f y.output 

	@if [ $(ARCH) = $(LINUX) ]; \
	then \
		sed -i -e 's|//line.*||' -e 's/yyEofCode/yyEOFCode/' parser/parser.go; \
	elif [ $(ARCH) = $(MAC) ]; \
	then \
		/usr/bin/sed -i "" 's|//line.*||' parser/parser.go; \
		/usr/bin/sed -i "" 's/yyEofCode/yyEOFCode/' parser/parser.go; \
	fi

	golex -o parser/scanner.go parser/scanner.l

ast-parser:
	a=`mktemp temp.XXXXXX`; \
	goyacc -o /dev/null -xegen $$a ast/parser/parser.y; \
	goyacc -o ast/parser/parser.go -xe $$a ast/parser/parser.y 2>&1 | grep "shift/reduce" | awk '{print} END {if (NR > 0) {print "Find conflict in parser.y. Please check y.output for more information."; exit 1;}}';  \
	rm -f $$a; \
	rm -f y.output

	@if [ $(ARCH) = $(LINUX) ]; \
	then \
		sed -i -e 's|//line.*||' -e 's/yyEofCode/yyEOFCode/' ast/parser/parser.go; \
	elif [ $(ARCH) = $(MAC) ]; \
	then \
		/usr/bin/sed -i "" 's|//line.*||' ast/parser/parser.go; \
		/usr/bin/sed -i "" 's/yyEofCode/yyEOFCode/' ast/parser/parser.go; \
	fi

	golex -o ast/parser/scanner.go ast/parser/scanner.l

check:
	go get github.com/golang/lint/golint

	@echo "vet"
	@ go tool vet . 2>&1 | grep -vE 'Godeps|parser/scanner.*unreachable code' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "vet --shadow"
	@ go tool vet --shadow . 2>&1 | grep -vE 'Godeps' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "golint"
	@ golint ./... 2>&1 | grep -vE 'LastInsertId|NewLexer' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "gofmt (simplify)"
	@ gofmt -s -l . 2>&1 | grep -vE 'Godeps|parser/parser.go|parser/scanner.go' | awk '{print} END{if(NR>0) {exit 1}}'

deps:
	go list -f '{{range .Deps}}{{printf "%s\n" .}}{{end}}{{range .TestImports}}{{printf "%s\n" .}}{{end}}' ./... | \
		sort | uniq | grep -E '[^/]+\.[^/]+/' |grep -v "pingcap/tidb" | \
		awk 'BEGIN{ print "#!/bin/bash" }{ printf("go get -u %s\n", $$1) }' > deps.sh
	chmod +x deps.sh
	bash deps.sh

clean:
	$(GO) clean -i ./...
	rm -rf *.out
	rm -f deps.sh

todo:
	@grep -n ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alpha:]][[:alnum:]]* */*.go parser/scanner.l parser/parser.y || true
	@grep -n TODO */*.go parser/scanner.l parser/parser.y || true
	@grep -n BUG */*.go parser/scanner.l parser/parser.y || true
	@grep -n println */*.go parser/scanner.l parser/parser.y || true

test: gotest 

gotest:
	$(GO) test -cover ./...

race:
	$(GO) test --race -cover ./...

interpreter:
	@cd interpreter && $(GO) build -ldflags '$(LDFLAGS)'

server:
ifeq ($(TARGET), "")
	@cd tidb-server && $(GO) build -ldflags '$(LDFLAGS)'
else
	@cd tidb-server && $(GO) build -ldflags '$(LDFLAGS)' -o '$(TARGET)'
endif

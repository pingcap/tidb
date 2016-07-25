# Copyright 2015 The parser Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

.PHONY:	all clean cover cpu editor internalError later mem nuke todo

grep=--include=*.go --include=*.l --include=*.y

all: editor
	go vet || true
	golint || true
	make todo

clean:
	go clean
	rm -f *~ cpu.test mem.test

cover:
	t=$(shell tempfile) ; go test -coverprofile $$t && go tool cover -html $$t && unlink $$t

cpu:
	go test -c -o cpu.test
	./cpu.test -noerr -test.cpuprofile cpu.out
	go tool pprof --lines cpu.test cpu.out

editor: scanner.go goscanner.go parser.go
	gofmt -l -s -w *.go
	go test
	go install

goscanner.go: go.l parser.go
	golex -o $@ $<

internalError:
	egrep -ho '"internal error.*"' *.go | sort | cat -n

later:
	@grep -n $(grep) LATER * || true
	@grep -n $(grep) MAYBE * || true

mem:
	go test -c -o mem.test
	./mem.test -test.bench . -test.memprofile mem.out
	go tool pprof --lines --web --alloc_space mem.test mem.out

nuke: clean
	go clean -i

# parser.go: parser.y
# 	a=$(shell tempfile) ; ( \
# 		goyacc -o /dev/null -xegen $$a $< && \
# 		cat xerrors >> $$a && \
# 		goyacc -xe $$a -o $@ -dlvalf "%v" -dlval "lval.token" $< \
# 	|| true ) ; \
# 	rm -f $$a

parser.go: parser.y Makefile
	a=$(shell tempfile) e=$(shell tempfile) ; ( \
		y \
			-import "\"go/token\"" \
			-keepCase \
			-lxType "*lexer" \
			-nodeString prettyString \
			yy $< log.y ast.go $$e && \
		goyacc -o /dev/null -xegen $$a log.y && \
		cat xerrors >> $$a && \
		goyacc -xe $$a -o $@ -dlvalf "%v" -dlval "lval.token" log.y && \
		go run gen_examples.go $$e ast_test.go && \
		go test -run ^Example | fe || echo "package parser" > ast_test.go && \
		egrep -nH "failed|panic|\/\/ <nil>|// false|// -1" *_test.go \
	|| true ) ; \
	rm -f $$a $$e

scanner.go: y.l parser.go
	golex -o $@ $<

todo:
	@grep -n $(grep) ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alpha:]][[:alnum:]]* * || true
	@grep -n $(grep) TODO * || true
	@grep -n $(grep) BUG * || true
	@grep -n $(grep) [^[:alpha:]]println * || true

# Copyright 2014 The y Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

.PHONY: all clean editor later nuke todo internalError cover mem cpu y.test bison

grep=--include=*.go

all: editor
	go tool vet -printfuncs "Log:0,Logf:1" *.go
	golint .
	make todo

bison:
	find -name \*.y -execdir bison -r all --report-file {}.bison -o/dev/null {} \;

clean:
	go clean
	rm -f *~ y.output mem.out cpu.out y.test

cover:
	t=$(shell tempfile) ; go test -coverprofile $$t && go tool cover -html $$t && unlink $$t

cpu: y.test
	./$< -noerr -test.cpuprofile cpu.out
	go tool pprof --lines $< cpu.out

editor:
	gofmt -l -s -w .
	go test -i
	go test
	go install

internalError:
	egrep -ho '".*internal error.*"' *.go | sort | cat -n

later:
	@grep -n $(grep) LATER * || true
	@grep -n $(grep) MAYBE * || true

mem: y.test
	./$< -noerr -test.memprofile mem.out -test.v # -test.memprofilerate 1
	go tool pprof --lines --alloc_space $< mem.out

nuke: clean
	go clean -i

todo:
	@grep -nr $(grep) ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alpha:]][[:alnum:]]* * || true
	@grep -nr $(grep) TODO * || true
	@grep -nr $(grep) BUG * || true
	@grep -nr $(grep) [^[:alpha:]]println * || true

y.test:
	go test -c

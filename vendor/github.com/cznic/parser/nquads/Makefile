# Copyright (c) 2014 The parser Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

.PHONY: all clean editor todo cover nuke

grep=--include=*.go *.y

all: editor
	go vet || true
	golint . || true
	go install
	make todo

cover:
	t=$(shell tempfile) ; go test -coverprofile $$t && go tool cover -html $$t && unlink $$t

editor: parser.go
	go fmt
	go test -i
	go test
	go build

parser.go: parser.y
	go tool yacc -o $@ -v /dev/null $<
	sed -i -e 's|//line.*||' -e 's/yyEofCode/yyEOFCode/' $@

todo:
	@grep -n ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alpha:]][[:alnum:]]* $(grep) || true
	@grep -n TODO $(grep) || true
	@grep -n BUG $(grep) || true
	@grep -n println $(grep) || true

clean:
	go clean

nuke: clean
	go clean -i

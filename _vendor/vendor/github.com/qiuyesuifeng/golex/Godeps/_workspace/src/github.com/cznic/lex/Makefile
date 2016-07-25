# Copyright (c) 2014 The lex Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

.PHONY: all

all: editor

clean:
	go clean
	rm -f y.output *~

editor: parser.go
	go fmt
	go test -i
	go test
	go install

parser.go: parser.y
	go tool yacc -o $@ $<

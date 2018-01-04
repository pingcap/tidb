# Copyright (c) 2011 CZ.NIC z.s.p.o. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# blame: jnml, labs.nic.cz

all: calc.go tokenizer.go
	gofmt -l -s -w *.go
	go build $^

run: all
	./calc

tokenizer.go: tokenizer.l
	golex -o $@ $<

calc.go: calc.y
	go tool yacc -o $@ $<

clean:
	go clean
	rm -f tokenizer.go calc.go lex.yy.go y.go y.output *~

nuke: clean
	rm -f calc

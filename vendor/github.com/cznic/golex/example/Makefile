# Copyright (c) 2011 CZ.NIC z.s.p.o. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# blame: jnml, labs.nic.cz

all: example.go
	go build $<

run: all
	./example

example.go: example.l
	golex -t $< | gofmt > $@

clean:
	rm -f example.go lex.yy.go y.output *~

nuke: clean
	rm -f example

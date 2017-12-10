# Copyright (c) 2011 CZ.NIC z.s.p.o. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# blame: jnml, labs.nic.cz

all: example2.go
	go build $<

run: all
	./example2

example2.go: example2.l
	golex -t $< | gofmt > $@

clean:
	rm -f example2.go lex.yy.go y.output *~

nuke: clean
	rm -f example2 *.[568]

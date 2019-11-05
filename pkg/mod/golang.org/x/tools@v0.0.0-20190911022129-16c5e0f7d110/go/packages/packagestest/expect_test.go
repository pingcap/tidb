// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package packagestest_test

import (
	"go/token"
	"testing"

	"golang.org/x/tools/go/expect"
	"golang.org/x/tools/go/packages/packagestest"
	"golang.org/x/tools/internal/span"
)

func TestExpect(t *testing.T) {
	exported := packagestest.Export(t, packagestest.GOPATH, []packagestest.Module{{
		Name:  "golang.org/fake",
		Files: packagestest.MustCopyFileTree("testdata"),
	}})
	defer exported.Cleanup()
	count := 0
	if err := exported.Expect(map[string]interface{}{
		"check": func(src, target token.Position) {
			count++
		},
		"boolArg": func(n *expect.Note, yes, no bool) {
			if !yes {
				t.Errorf("Expected boolArg first param to be true")
			}
			if no {
				t.Errorf("Expected boolArg second param to be false")
			}
		},
		"intArg": func(n *expect.Note, i int64) {
			if i != 42 {
				t.Errorf("Expected intarg to be 42")
			}
		},
		"stringArg": func(n *expect.Note, name expect.Identifier, value string) {
			if string(name) != value {
				t.Errorf("Got string arg %v expected %v", value, name)
			}
		},
		"directNote": func(n *expect.Note) {},
		"range": func(r span.Range) {
			if r.Start == token.NoPos || r.Start == 0 {
				t.Errorf("Range had no valid starting position")
			}
			if r.End == token.NoPos || r.End == 0 {
				t.Errorf("Range had no valid ending position")
			} else if r.End <= r.Start {
				t.Errorf("Range ending was not greater than start")
			}
		},
		"checkEOF": func(n *expect.Note, p token.Pos) {
			if p <= n.Pos {
				t.Errorf("EOF was before the checkEOF note")
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatalf("No tests were run")
	}
}

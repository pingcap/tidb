// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package parser

import (
	"flag"
	"fmt"
	"go/token"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func caller(s string, va ...interface{}) {
	_, fn, fl, _ := runtime.Caller(2)
	fmt.Fprintf(os.Stderr, "caller: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
	_, fn, fl, _ = runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "\tcallee: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintln(os.Stderr)
}

func dbg(s string, va ...interface{}) {
	if s == "" {
		s = strings.Repeat("%v ", len(va))
	}
	_, fn, fl, _ := runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "dbg %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
}

func TODO(...interface{}) string {
	_, fn, fl, _ := runtime.Caller(1)
	return fmt.Sprintf("TODO: %s:%d:\n", path.Base(fn), fl)
}

func use(...interface{}) {}

// ============================================================================

func init() {
	flag.IntVar(&yyDebug, "yydebug", 0, "")
}

func test0(t *testing.T, root string) {
	if err := filepath.Walk(root, func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		ok, err := filepath.Match("*.y", filepath.Base(pth))
		if err != nil {
			t.Fatal(err)
		}

		if !ok {
			return nil
		}

		if filepath.Base(pth) == "go.y" { // Uses non standard %error directive.
			return nil
		}

		file, err := os.Open(pth)
		if err != nil {
			t.Fatal(err)
		}

		defer file.Close()
		src, err := ioutil.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}

		ast, err := Parse(token.NewFileSet(), pth, src)
		if err != nil {
			t.Error(err)
			return nil
		}

		if ast == nil {
			t.Fatal(ast)
		}

		t.Log(pth)
		return nil

	}); err != nil {
		t.Fatal(err)
	}
}

func Test0(t *testing.T) {
	test0(t, ".")
	test0(t, filepath.Join(runtime.GOROOT(), "src"))
}

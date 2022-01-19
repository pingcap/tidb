package main

import (
	"fmt"
	"os"
	"strings"
	"bufio"
	"path/filepath"
)

func main() {
	// compile all tests without running
	// go test --exec=xprog ./...
	// See https://github.com/golang/go/issues/15513#issuecomment-773994959
	cwd := os.Args[0]
	cwd = cwd[:len(cwd)-len("tools/bin/xprog")]

	// Command line args looks like 'xprog /tmp/go-build2662369829/b1382/aggfuncs.test -test.paniconexit0 -test.timeout=10m0s'
	testBinaryPath := os.Args[1]
	dir, _ := filepath.Split(testBinaryPath)

	// Read the /tmp/go-build2662369829/b1382/importcfg.link file to get the package information
	f, err := os.Open(filepath.Join(dir, "importcfg.link"))
	if err != nil {
		os.Exit(-1)
	}

	r := bufio.NewReader(f)
	// packagefile github.com/pingcap/tidb/session.test=/home/genius/.cache/go-build/fb/fb1587cce5727fa9461131eab8260a52878da04f5c8da49dd3c7b2d941430c63-d
	line, _, err := r.ReadLine()
	if err != nil {
		os.Exit(-1)
	}
	start := strings.IndexByte(string(line), ' ')
	end := strings.IndexByte(string(line), '=')
	pkg := string(line[start+1:end])
	const prefix = "github.com/pingcap/tidb/"
	if !strings.HasPrefix(pkg, prefix) {
		os.Exit(-1)
	}
	// github.com/pingcap/tidb/util/topsql.test => util/topsql
	pkg = pkg[len(prefix) : len(pkg)-len(".test")]

	_, file := filepath.Split(pkg)

	// $CWD/util/topsql/topsql.test.bin
	newName := filepath.Join(cwd, pkg, file+".test.bin")

	l, err := os.Create("/tmp/xxx.log")
	fmt.Fprintf(l, "cwd %s\n", cwd)
	fmt.Fprintf(l, "testBinaryPath %s\n", testBinaryPath)
	fmt.Fprintf(l, "the path is %s\n", newName)

	err = os.Rename(testBinaryPath, newName)
	if err != nil {
		os.Exit(-1)
	}
}

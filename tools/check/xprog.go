package main

import (
	"io"
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
		os.Exit(-2)
	}
	start := strings.IndexByte(string(line), ' ')
	end := strings.IndexByte(string(line), '=')
	pkg := string(line[start+1:end])
	const prefix = "github.com/pingcap/tidb/"
	if !strings.HasPrefix(pkg, prefix) {
		os.Exit(-3)
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

	if err1 := os.Rename(testBinaryPath, newName); err1 != nil {
		// Don't use os.Rename, because of error "invalid cross-device linkcd tools/check"
		err1 = MoveFile(testBinaryPath, newName)
		if err1 != nil {
			fmt.Fprintf(l, "erro == %s", err.Error(), "===", err1.Error())
			os.Exit(-4)
		}
	}
}

func MoveFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}

	// Handle the permissions
	si, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("Stat error: %s", err)
	}
	err = os.Chmod(destPath, si.Mode())
	if err != nil {
		return fmt.Errorf("Chmod error: %s", err)
	}

	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
}

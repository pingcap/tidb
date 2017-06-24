// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build ignore

package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func yy() (nm string, err error) {
	y, err := os.Create("parser.y")
	if err != nil {
		return "", err
	}

	nm = y.Name()
	cmd := exec.Command(
		"yy",
		"-astImport", "\"go/token\"",
		"-kind", "Case",
		"-o", nm,
		"parser.yy",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		os.Remove(nm)
		log.Printf("%s", out)
		return "", err
	}

	return nm, nil
}

func goyacc(y string) (err error) {
	t, err := ioutil.TempFile("", "go-generate-xegen-")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if e := os.Remove(t.Name()); e != nil && err == nil {
			err = e
		}
	}()

	cmd := exec.Command("goyacc", "-o", os.DevNull, "-xegen", t.Name(), y)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Printf("%s\n", out)
		return err
	}

	xerrors, err := ioutil.ReadFile("xerrors")
	if err != nil {
		return err
	}

	if _, err := t.Seek(0, 2); err != nil {
		return err
	}

	if _, err := t.Write(xerrors); err != nil {
		return err
	}

	cmd = exec.Command("goyacc", "-cr", "-xe", t.Name(), "-o", "parser.go", "-dlvalf", "%v", "-dlval", "prettyString(lval.Token)", y)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Printf("%s", out)
		return err
	} else {
		log.Printf("%s", out)
	}

	return nil
}

func main() {
	if err := main0(); err != nil {
		log.Fatal(err)
	}
}

func main0() (err error) {
	log.SetFlags(log.Lshortfile)
	p2 := flag.Bool("2", false, "")
	flag.Parse()
	if *p2 {
		return main2()
	}

	os.Remove("ast.go")
	os.Remove("ast_test.go")
	y, err := yy()
	if err != nil {
		return err
	}

	return goyacc(y)
}

func main2() (err error) {
	goCmd := exec.Command("go", "test", "-run", "^Example")
	out, err := goCmd.CombinedOutput() // Errors are expected and wanted here.
	feCmd := exec.Command("fe")
	feCmd.Stdin = bytes.NewBuffer(out)
	if out, err = feCmd.CombinedOutput(); err != nil {
		log.Printf("%s", out)
		return err
	}

	matches, err := filepath.Glob("*_test.go")
	if err != nil {
		return err
	}

	cmd := exec.Command("pcregrep", append([]string{"-nM", `failed|panic|\/\/ <nil>|// false|// -1|Output:\n}`}, matches...)...)
	if out, _ = cmd.CombinedOutput(); len(out) != 0 { // Error != nil when no matches
		log.Printf("%s", out)
	}
	return nil
}

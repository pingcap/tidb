// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build ignore

// gen runs go generate on Unicode- and CLDR-related package in the text
// repositories, taking into account dependencies and versions.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"unicode"

	"golang.org/x/text/internal/gen"
)

var (
	verbose     = flag.Bool("v", false, "verbose output")
	force       = flag.Bool("force", false, "ignore failing dependencies")
	excludeList = flag.String("exclude", "",
		"comma-separated list of packages to exclude")

	// The user can specify a selection of packages to build on the command line.
	args []string
)

func exclude(pkg string) bool {
	if len(args) > 0 {
		return !contains(args, pkg)
	}
	return contains(strings.Split(*excludeList, ","), pkg)
}

// TODO:
// - Better version handling.
// - Generate tables for the core unicode package?
// - Add generation for encodings. This requires some retooling here and there.
// - Running repo-wide "long" tests.

var vprintf = fmt.Printf

func main() {
	gen.Init()
	args = flag.Args()
	if !*verbose {
		// Set vprintf to a no-op.
		vprintf = func(string, ...interface{}) (int, error) { return 0, nil }
	}

	if gen.UnicodeVersion() != unicode.Version {
		fmt.Printf("Requested Unicode version %s; core unicode version is %s.\n",
			gen.UnicodeVersion,
			unicode.Version)
		// TODO: use collate to compare. Simple comparison will work, though,
		// until Unicode reaches version 10. To avoid circular dependencies, we
		// could use the NumericWeighter without using package collate using a
		// trivial Weighter implementation.
		if gen.UnicodeVersion() < unicode.Version && !*force {
			os.Exit(2)
		}
	}
	var (
		cldr     = generate("cldr")
		language = generate("language", cldr)
		norm     = generate("unicode/norm")
		_        = generate("unicode/rangetable")
		_        = generate("width")
		_        = generate("display", cldr, language)
		_        = generate("cases", norm)
		_        = generate("collate", norm, cldr, language)
		_        = generate("search", norm, cldr, language)
	)
	all.Wait()

	if hasErrors {
		fmt.Println("FAIL")
		os.Exit(1)
	}
	vprintf("SUCCESS\n")
}

var (
	all       sync.WaitGroup
	hasErrors bool
)

type dependency struct {
	sync.WaitGroup
	hasErrors bool
}

func generate(pkg string, deps ...*dependency) *dependency {
	var wg dependency
	if exclude(pkg) {
		return &wg
	}
	wg.Add(1)
	all.Add(1)
	go func() {
		defer wg.Done()
		defer all.Done()
		// Wait for dependencies to finish.
		for _, d := range deps {
			d.Wait()
			if d.hasErrors && !*force {
				fmt.Printf("--- ABORT: %s\n", pkg)
				wg.hasErrors = true
				return
			}
		}
		vprintf("=== GENERATE %s\n", pkg)
		args := []string{"generate"}
		if *verbose {
			args = append(args, "-v")
		}
		args = append(args, "./"+pkg)
		cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), args...)
		w := &bytes.Buffer{}
		cmd.Stderr = w
		cmd.Stdout = w
		if err := cmd.Run(); err != nil {
			fmt.Printf("--- FAIL: %s:\n\t%v\n\tError: %v\n", pkg, indent(w), err)
			hasErrors = true
			wg.hasErrors = true
			return
		}
		vprintf("--- SUCCESS: %s\n\t%v\n", pkg, indent(w))
	}()
	return &wg
}

func contains(a []string, s string) bool {
	for _, e := range a {
		if s == e {
			return true
		}
	}
	return false
}

func indent(b *bytes.Buffer) string {
	return strings.Replace(strings.TrimSpace(b.String()), "\n", "\n\t", -1)
}

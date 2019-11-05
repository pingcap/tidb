// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Incomplete source tree on Android.

// +build !android

package buildutil_test

import (
	"go/build"
	"runtime"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/buildutil"
	"golang.org/x/tools/go/packages/packagestest"
)

func TestAllPackages(t *testing.T) {
	if runtime.Compiler == "gccgo" {
		t.Skip("gccgo has no standard packages")
	}

	exported := packagestest.Export(t, packagestest.GOPATH, []packagestest.Module{
		{Name: "golang.org/x/tools/go/buildutil", Files: packagestest.MustCopyFileTree(".")}})
	defer exported.Cleanup()

	var gopath string
	for _, env := range exported.Config.Env {
		if !strings.HasPrefix(env, "GOPATH=") {
			continue
		}
		gopath = strings.TrimPrefix(env, "GOPATH=")
	}
	if gopath == "" {
		t.Fatal("Failed to fish GOPATH out of env: ", exported.Config.Env)
	}

	var buildContext = build.Default
	buildContext.GOPATH = gopath
	all := buildutil.AllPackages(&buildContext)

	set := make(map[string]bool)
	for _, pkg := range all {
		set[pkg] = true
	}

	const wantAtLeast = 250
	if len(all) < wantAtLeast {
		t.Errorf("Found only %d packages, want at least %d", len(all), wantAtLeast)
	}

	for _, want := range []string{"fmt", "crypto/sha256", "golang.org/x/tools/go/buildutil"} {
		if !set[want] {
			t.Errorf("Package %q not found; got %s", want, all)
		}
	}
}

func TestExpandPatterns(t *testing.T) {
	tree := make(map[string]map[string]string)
	for _, pkg := range []string{
		"encoding",
		"encoding/xml",
		"encoding/hex",
		"encoding/json",
		"fmt",
	} {
		tree[pkg] = make(map[string]string)
	}
	ctxt := buildutil.FakeContext(tree)

	for _, test := range []struct {
		patterns string
		want     string
	}{
		{"", ""},
		{"fmt", "fmt"},
		{"nosuchpkg", "nosuchpkg"},
		{"nosuchdir/...", ""},
		{"...", "encoding encoding/hex encoding/json encoding/xml fmt"},
		{"encoding/... -encoding/xml", "encoding encoding/hex encoding/json"},
		{"... -encoding/...", "fmt"},
		{"encoding", "encoding"},
		{"encoding/", "encoding"},
	} {
		var pkgs []string
		for pkg := range buildutil.ExpandPatterns(ctxt, strings.Fields(test.patterns)) {
			pkgs = append(pkgs, pkg)
		}
		sort.Strings(pkgs)
		got := strings.Join(pkgs, " ")
		if got != test.want {
			t.Errorf("ExpandPatterns(%s) = %s, want %s",
				test.patterns, got, test.want)
		}
	}
}

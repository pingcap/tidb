// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build bazel
// +build bazel

package util

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	inner "github.com/bazelbuild/rules_go/go/tools/bazel"
)

// BuiltWithBazel returns true iff this library was built with Bazel.
func BuiltWithBazel() bool {
	return true
}

// Runfile is a convenience wrapper around the rules_go variant.
func Runfile(path string) (string, error) {
	return inner.Runfile(path)
}

// SetGoEnv will get go env from bazel
func SetGoEnv() {
	gobin, err := Runfile("bin/go")
	if err != nil {
		panic(err)
	}
	if err := os.Setenv("PATH", fmt.Sprintf("%s%c%s", filepath.Dir(gobin), os.PathListSeparator, os.Getenv("PATH"))); err != nil {
		panic(err)
	}
	// GOPATH has to be set to some value (not equal to GOROOT) in order for `go env` to work.
	// See https://github.com/golang/go/issues/43938 for the details.
	// Specify a name under the system TEMP/TMP directory in order to be platform agnostic.
	if err := os.Setenv("GOPATH", filepath.Join(os.TempDir(), "nonexist-gopath")); err != nil {
		panic(err)
	}
	if err := os.Setenv("GOROOT", filepath.Dir(filepath.Dir(gobin))); err != nil {
		panic(err)
	}
	if err := os.Setenv("GOCACHE", path.Join(inner.TestTmpDir(), ".gocache")); err != nil {
		panic(err)
	}
}

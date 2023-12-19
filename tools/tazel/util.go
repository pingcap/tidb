// Copyright 2023 PingCAP, Inc.
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

package main

import (
	"os"
	"strings"

	"github.com/bazelbuild/buildtools/build"
	"github.com/pingcap/tidb/pkg/util/set"
)

func write(path string, f *build.File) error {
	build.Rewrite(f)
	out := build.Format(f)
	return os.WriteFile(path, out, 0644)
}

func skipFlaky(path string) bool {
	var pmap = set.NewStringSet()
	pmap.Insert("tests/realtikvtest/addindextest/BUILD.bazel")
	return pmap.Exist(path)
}

func skipTazel(path string) bool {
	var pmap = set.NewStringSet()
	pmap.Insert("build/BUILD.bazel")
	return pmap.Exist(path)
}

func skipShardCount(path string) bool {
	return strings.HasPrefix(path, "tests") ||
		(strings.HasPrefix(path, "pkg/util") &&
			!strings.HasPrefix(path, "pkg/util/admin") &&
			!strings.HasPrefix(path, "pkg/util/chunk") &&
			!strings.HasPrefix(path, "pkg/util/topsql") &&
			!strings.HasPrefix(path, "pkg/util/stmtsummary"))
}

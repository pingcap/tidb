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

package main

import (
	_ "embed"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/ghemawat/stream"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

//go:embed testdata/marker_test_go.txt
var markerTest string

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestWalker(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "marker_test.go", markerTest, parser.ParseComments)
	require.NoError(t, err)

	mis := walkMarker(f, "marker_test.go")
	require.Equal(t, []*MarkInfo{{
		Features: []featureMarkInfo{{
			ID:          "FD-1",
			Description: []any{"FD-1 feature testing"},
		}},
		TestName: "TestMarkAsFeature",
		File:     "marker_test.go",
	}, {
		Issues: []issueMarkInfo{{
			ID:       12345,
			IssueURL: "https://github.com/pingcap/tidb/issues/12345",
		}},
		TestName: "TestMarkAsIssue",
		File:     "marker_test.go",
	}, {
		Features: []featureMarkInfo{
			{
				ID:          "FD-1",
				Description: []any{"FD-1 feature testing"},
			},
			{
				ID:          "FD-2",
				Description: []any{"FD-2 feature testing"},
			},
		},
		Issues: []issueMarkInfo{
			{
				ID:       12345,
				IssueURL: "https://github.com/pingcap/tidb/issues/12345",
			},
			{
				ID:       12346,
				IssueURL: "https://github.com/pingcap/tidb/issues/12346",
			},
		},
		TestName: "TestMarkMixes",
		File:     "marker_test.go",
	}}, mis)
}

func TestNeedCheckMarkInfo(t *testing.T) {
	type test struct {
		name     string
		filePath []string
		rules    string
		expect   []bool
	}
	tests := []test{
		{
			name:     "sample",
			filePath: []string{"tests/featuremarker/walker_test.go", "tests/other/some_test.go"},
			rules: `
- path: /tests/featuremarker
`,
			expect: []bool{true, false},
		},
		{
			name:     "refactor testcase dirs",
			filePath: []string{"tests/featuremarker/walker_test.go", "executor/admin_test.go", "executor/newdir/old_test.go"},
			rules: `
- path: /tests/featuremarker
  exclude:
  - /tests/featuremarker/walker_test.go
- path: /executor
  exclude:
  - /executor/newdir
`,
			expect: []bool{false, true, false},
		},
	}
	for _, test := range tests {
		var rules []*RuleSpec
		require.NoError(t, yaml.Unmarshal([]byte(test.rules), &rules))
		var got []bool
		for _, filePath := range test.filePath {
			got = append(got, shouldCheckMarker(filePath, rules))
		}
		require.Equal(t, test.expect, got, "test: %s", test.name)
	}
}

func TestIndexNewTest(t *testing.T) {
	type test struct {
		name string
		diff string
		want []string
	}

	tests := []test{
		{
			name: "new test",
			diff: `diff --git a/tests/featuremarker/walker_test.go b/tests/featuremarker/walker_test.go
index 519ee8ce1..9c51846ee 100644
--- a/tests/featuremarker/walker_test.go
+++ b/tests/featuremarker/walker_test.go
@@ -86,6 +86,6 @@ func TestExtractMappingsFromCommentMap(t *testing.T) {
		}
 }
 
-// Feature: mapping
-func TestDummy(t *testing.T) {
+// Feature: lint
+func TestIndexNewTest(t *testing.T) {
 }
 `,
			want: []string{"TestIndexNewTest"},
		},
		{
			name: "no new test",
			diff: `diff --git a/bindinfo/bind_test.go b/bindinfo/bind_test.go
index 07e29923a..76ab41992 100644
--- a/bindinfo/bind_test.go
+++ b/bindinfo/bind_test.go
@@ -32,6 +32,8 @@ import (
		"github.com/stretchr/testify/require"
 )
 
+// TestPrepareCacheWithBinding ...
+// Feature: prepare_cache
 func TestPrepareCacheWithBinding(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
`,
			want: nil,
		},
	}
	for _, test := range tests {
		got, err := findAddTest(stream.ReadLines(strings.NewReader(test.diff)))
		require.NoError(t, err)
		require.Equal(t, test.want, got, "test: %s", test.name)
	}
}

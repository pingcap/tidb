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
	"testing"

	markerpkg "github.com/pingcap/tidb/tests/testmarker/pkg"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/marker_test_go.txt
var markerTest string

func TestWalker(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "marker_test.go", markerTest, parser.ParseComments)
	require.NoError(t, err)

	mis := markerpkg.WalkTestFile(fset, f, "marker_test.go")
	require.Equal(t, []*markerpkg.MarkInfo{{
		Features: []markerpkg.FeatureMarkInfo{{
			ID:          "FD-1",
			Description: []string{"FD-1 feature testing"},
		}},
		TestName: "TestMarkAsFeature",
		File:     "marker_test.go",
		Pos:      680,
	}, {
		Issues: []markerpkg.IssueMarkInfo{{
			ID:       12345,
			IssueURL: "https://github.com/pingcap/tidb/issues/12345",
		}},
		TestName: "TestMarkAsIssue",
		File:     "marker_test.go",
		Pos:      784,
	}, {
		Features: []markerpkg.FeatureMarkInfo{
			{
				ID:          "FD-1",
				Description: []string{"FD-1 feature testing"},
			},
			{
				ID:          "FD-2",
				Description: []string{"FD-2 feature testing"},
			},
		},
		Issues: []markerpkg.IssueMarkInfo{
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
		Pos:      859,
	}, {
		Features: []markerpkg.FeatureMarkInfo{
			{
				ID: "marker.NoID",
			},
		},
		TestName: "TestMarkNoID",
		File:     "marker_test.go",
		Pos:      1329,
	}}, mis)
}

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
// See the License for the specific language governing permissions and
// limitations under the License.

package parser_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestKeywords(t *testing.T) {
	// Test for the first keyword
	require.Equal(t, "ADD", parser.Keywords[0].Word)
	require.Equal(t, true, parser.Keywords[0].Reserved)

	// Make sure TiDBKeywords are included and AUTO is unreserved.
	foundAdmin := false
	foundAuto := false
	for _, kw := range parser.Keywords {
		if kw.Word == "ADMIN" {
			foundAdmin = true
		}
		if kw.Word == "AUTO" {
			foundAuto = true
			require.False(t, kw.Reserved)
		}
	}
	require.True(t, foundAdmin, "TiDBKeyword ADMIN is part of the list")
	require.True(t, foundAuto, "AUTO is part of the keyword list")
}

func TestKeywordsLength(t *testing.T) {
	require.Equal(t, 685, len(parser.Keywords))

	reservedNr := 0
	for _, kw := range parser.Keywords {
		if kw.Reserved {
			reservedNr += 1
		}
	}
	require.Equal(t, 233, reservedNr)
}

func TestKeywordsSorting(t *testing.T) {
	for i, kw := range parser.Keywords {
		if i > 1 && parser.Keywords[i-1].Word > kw.Word && parser.Keywords[i-1].Section == kw.Section {
			t.Errorf("%s should come after %s, please update parser.y and re-generate keywords.go\n",
				parser.Keywords[i-1].Word, kw.Word)
		}
	}
}

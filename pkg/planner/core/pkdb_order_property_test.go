// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMatchStrictIndexOrder(t *testing.T) {
	cols := map[string]*expression.Column{
		"a": {UniqueID: 1},
		"b": {UniqueID: 2},
		"c": {UniqueID: 3},
		"d": {UniqueID: 4},
	}
	items := func(names []string, desc []bool) []property.SortItem {
		result := make([]property.SortItem, 0, len(names))
		for i, name := range names {
			result = append(result, property.SortItem{Col: cols[name], Desc: desc[i]})
		}
		return result
	}
	indexCols := func(names ...string) []*expression.Column {
		result := make([]*expression.Column, 0, len(names))
		for _, name := range names {
			result = append(result, cols[name])
		}
		return result
	}
	fullLens := func(count int) []int {
		result := make([]int, count)
		for i := range result {
			result[i] = types.UnspecifiedLength
		}
		return result
	}

	tests := []struct {
		name      string
		idxCols   []*expression.Column
		idxLens   []int
		constCols []bool
		order     []property.SortItem
		expected  bool
	}{
		{name: "ascending", idxCols: indexCols("a"), idxLens: fullLens(1), order: items([]string{"a"}, []bool{false}), expected: true},
		{name: "descending", idxCols: indexCols("a"), idxLens: fullLens(1), order: items([]string{"a"}, []bool{true}), expected: true},
		{name: "constant prefix", idxCols: indexCols("a", "b"), idxLens: fullLens(2), constCols: []bool{true, false}, order: items([]string{"b"}, []bool{true}), expected: true},
		{name: "missing constant prefix", idxCols: indexCols("a", "b"), idxLens: fullLens(2), order: items([]string{"b"}, []bool{false}), expected: false},
		{name: "multiple columns", idxCols: indexCols("a", "b", "c"), idxLens: fullLens(3), constCols: []bool{true, false, false}, order: items([]string{"b", "c"}, []bool{false, false}), expected: true},
		{name: "explicit constant sort column", idxCols: indexCols("d", "c", "b", "a"), idxLens: fullLens(4), constCols: []bool{true, false, true, false}, order: items([]string{"c", "b", "a"}, []bool{false, false, false}), expected: true},
		{name: "prefix index", idxCols: indexCols("a", "b"), idxLens: []int{3, types.UnspecifiedLength}, order: items([]string{"a"}, []bool{false}), expected: false},
		{name: "mixed direction", idxCols: indexCols("a", "b"), idxLens: fullLens(2), order: items([]string{"a", "b"}, []bool{false, true}), expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, matchStrictIndexOrder(tt.idxCols, tt.idxLens, tt.constCols, tt.order))
		})
	}
}

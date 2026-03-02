// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloneColAndIdxExistenceMap(t *testing.T) {
	m := NewColAndIndexExistenceMapWithoutSize()
	m.InsertCol(1, true)
	m.InsertIndex(1, true)

	m2 := m.Clone()
	require.Equal(t, m, m2)
}

type ShareMode uint8

const (
	Shared ShareMode = iota
	Cloned
)

func TestCopyAs(t *testing.T) {
	tests := []struct {
		name        string
		intent      CopyIntent
		expectCols  ShareMode
		expectIdxs  ShareMode
		expectExist ShareMode
	}{
		{"MetaOnly", MetaOnly, Shared, Shared, Shared},
		{"ColumnMapWritable", ColumnMapWritable, Cloned, Shared, Cloned},
		{"IndexMapWritable", IndexMapWritable, Shared, Cloned, Cloned},
		{"BothMapsWritable", BothMapsWritable, Cloned, Cloned, Cloned},
		{"AllDataWritable", AllDataWritable, Cloned, Cloned, Cloned},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := &Table{
				HistColl:              *NewHistColl(1, 1, 1, 1, 1),
				ColAndIdxExistenceMap: NewColAndIndexExistenceMap(1, 1),
			}

			copied := table.CopyAs(tt.intent)

			// Test columns map sharing/cloning
			copied.SetCol(1, &Column{PhysicalID: 123})
			if tt.expectCols == Shared {
				require.NotNil(t, table.GetCol(1), "shared columns: addition to copy should appear in original")
			} else {
				require.Nil(t, table.GetCol(1), "cloned columns: addition to copy should not appear in original")
			}

			// Test indices map sharing/cloning
			copied.SetIdx(1, &Index{PhysicalID: 123})
			if tt.expectIdxs == Shared {
				require.NotNil(t, table.GetIdx(1), "shared indices: addition to copy should appear in original")
			} else {
				require.Nil(t, table.GetIdx(1), "cloned indices: addition to copy should not appear in original")
			}

			// Test existence map sharing/cloning
			if tt.expectExist == Shared {
				require.Same(t, table.ColAndIdxExistenceMap, copied.ColAndIdxExistenceMap)
			} else {
				require.NotSame(t, table.ColAndIdxExistenceMap, copied.ColAndIdxExistenceMap)
			}
		})
	}
}

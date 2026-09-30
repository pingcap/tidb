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

package ddl

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type delRangeExecWrapperForTest struct {
	rewrite  map[int64]int64
	consumed int
	sql      string
	params   []any
}

func (*delRangeExecWrapperForTest) UpdateTSOForJob() error { return nil }

func (w *delRangeExecWrapperForTest) PrepareParamsList(_ int) {
	w.params = nil
}

func (w *delRangeExecWrapperForTest) RewriteTableID(tableID int64) (int64, bool) {
	newID, ok := w.rewrite[tableID]
	return newID, ok
}

func (w *delRangeExecWrapperForTest) AppendParamsList(jobID, elemID int64, startKey, endKey string) {
	w.params = append(w.params, jobID, elemID, startKey, endKey)
}

func (w *delRangeExecWrapperForTest) ConsumeDeleteRange(_ context.Context, sql string) error {
	w.consumed++
	w.sql = sql
	return nil
}

func TestDoBatchDeleteTablesRangeSkipsRewrittenIDs(t *testing.T) {
	tests := []struct {
		name          string
		tableIDs      []int64
		rewrite       map[int64]int64
		expectedRows  int
		expectedComma bool
	}{
		{
			name:          "all IDs skipped",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{},
			expectedRows:  0,
			expectedComma: false,
		},
		{
			name:          "first ID kept",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{1: 11},
			expectedRows:  1,
			expectedComma: false,
		},
		{
			name:          "second ID kept",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{2: 22},
			expectedRows:  1,
			expectedComma: false,
		},
		{
			name:          "both IDs kept",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{1: 11, 2: 22},
			expectedRows:  2,
			expectedComma: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper := &delRangeExecWrapperForTest{rewrite: tt.rewrite}
			err := doBatchDeleteTablesRange(context.Background(), wrapper, 100, tt.tableIDs, &elementIDAlloc{}, "test")
			require.NoError(t, err)
			require.Equal(t, bool(tt.expectedRows > 0), wrapper.consumed > 0)
			if tt.expectedRows == 0 {
				require.Empty(t, wrapper.params)
				return
			}
			require.Len(t, wrapper.params, tt.expectedRows*4)
			require.Equal(t, tt.expectedComma, strings.Contains(wrapper.sql, insertDeleteRangeSQLValue+","+insertDeleteRangeSQLValue))
			require.False(t, strings.HasSuffix(wrapper.sql, ","))
		})
	}
}

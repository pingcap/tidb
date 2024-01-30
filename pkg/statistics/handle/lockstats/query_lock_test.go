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

package lockstats

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func TestGetTablesLockedStatuses(t *testing.T) {
	tests := []struct {
		name        string
		tableLocked map[int64]struct{}
		tableIDs    []int64
		want        map[int64]struct{}
	}{
		{
			name:        "not locked",
			tableLocked: map[int64]struct{}{},
			tableIDs:    []int64{1, 2, 3},
			want:        map[int64]struct{}{},
		},
		{
			name:        "locked",
			tableLocked: map[int64]struct{}{1: {}, 2: {}},
			tableIDs:    []int64{1, 2, 3},
			want: map[int64]struct{}{
				1: {},
				2: {},
			},
		},
		{
			name:        "empty",
			tableLocked: map[int64]struct{}{},
			tableIDs:    []int64{},
			want:        map[int64]struct{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetLockedTables(tt.tableLocked, tt.tableIDs...)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestQueryLockedTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	tests := []struct {
		name      string
		numRows   int
		wantLen   int
		wantError bool
	}{
		{
			name:    "Empty result",
			numRows: 0,
			wantLen: 0,
		},
		{
			name:    "One table",
			numRows: 1,
			wantLen: 1,
		},
		{
			name:    "Two tables",
			numRows: 2,
			wantLen: 2,
		},
		{
			name:      "Error",
			numRows:   0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executeQueryLockedTables(exec, tt.numRows, tt.wantError)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

type ctxMatcher struct{}

func (c *ctxMatcher) Matches(x any) bool {
	ctx := x.(context.Context)
	s := util.RequestSourceFromCtx(ctx)
	return s == util.InternalRequest+"_"+kv.InternalTxnStats
}

func (c *ctxMatcher) String() string {
	return "all txns should be internal_stats source"
}

func executeQueryLockedTables(exec *mock.MockRestrictedSQLExecutor, numRows int, wantErr bool) (map[int64]struct{}, error) {
	if wantErr {
		exec.EXPECT().ExecRestrictedSQL(
			gomock.All(&ctxMatcher{}),
			statsutil.UseCurrentSessionOpt,
			selectSQL,
		).Return(nil, nil, errors.New("error"))
		return QueryLockedTables(wrapAsSCtx(exec))
	}

	c := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, numRows)
	for i := 0; i < numRows; i++ {
		c.AppendInt64(0, int64(i+1))
	}
	var rows []chunk.Row
	for i := 0; i < numRows; i++ {
		rows = append(rows, c.GetRow(i))
	}
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		statsutil.UseCurrentSessionOpt,
		selectSQL,
	).Return(rows, nil, nil)

	return QueryLockedTables(wrapAsSCtx(exec))
}

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

package copr

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestNewCopContextSingleIndex(t *testing.T) {
	var mockColInfos []*model.ColumnInfo
	colCnt := 6
	for i := 0; i < colCnt; i++ {
		mockColInfos = append(mockColInfos, &model.ColumnInfo{
			ID:        int64(i),
			Offset:    i,
			Name:      model.NewCIStr(fmt.Sprintf("c%d", i)),
			FieldType: *types.NewFieldType(1),
			State:     model.StatePublic,
		})
	}
	findColByName := func(name string) *model.ColumnInfo {
		for _, info := range mockColInfos {
			if info.Name.L == name {
				return info
			}
		}
		return nil
	}
	const (
		pkTypeRowID        = 0
		pkTypePKHandle     = 1
		pkTypeCommonHandle = 2
	)

	testCases := []struct {
		pkType       int
		cols         []string
		expectedCols []string
	}{
		{pkTypeRowID, []string{"c1"}, []string{"c1", "_tidb_rowid"}},
		{pkTypeRowID, []string{"c1", "c3"}, []string{"c1", "c3", "_tidb_rowid"}},
		{pkTypePKHandle, []string{"c1"}, []string{"c0", "c1"}},
		{pkTypeCommonHandle, []string{"c4", "c1"}, []string{"c1", "c2", "c4"}},
	}

	for i, tt := range testCases {
		var idxCols []*model.IndexColumn
		for _, cn := range tt.cols {
			idxCols = append(idxCols, &model.IndexColumn{
				Name:   model.NewCIStr(cn),
				Offset: findColByName(cn).Offset,
			})
		}
		mockIdxInfo := &model.IndexInfo{
			ID:      int64(i),
			Name:    model.NewCIStr(fmt.Sprintf("i%d", i)),
			Columns: idxCols,
			State:   model.StatePublic,
		}
		mockTableInfo := &model.TableInfo{
			Name:           model.NewCIStr("t"),
			Columns:        mockColInfos,
			Indices:        []*model.IndexInfo{mockIdxInfo},
			PKIsHandle:     tt.pkType == pkTypePKHandle,
			IsCommonHandle: tt.pkType == pkTypeCommonHandle,
		}
		if mockTableInfo.PKIsHandle {
			mockTableInfo.Columns[0].SetFlag(mysql.PriKeyFlag)
		}
		if mockTableInfo.IsCommonHandle {
			mockTableInfo.Indices = append(mockTableInfo.Indices, &model.IndexInfo{
				Columns: []*model.IndexColumn{
					{
						Name:   model.NewCIStr("c2"),
						Offset: 2,
					},
					{
						Name:   model.NewCIStr("c4"),
						Offset: 4,
					},
				},
				State:   model.StatePublic,
				Primary: true,
			})
		}

		sctx := mock.NewContext()
		copCtx, err := NewCopContextSingleIndex(
			sctx.GetExprCtx(),
			sctx.GetDistSQLCtx(),
			sctx.GetSessionVars().StmtCtx.PushDownFlags(),
			mockTableInfo, mockIdxInfo, "",
		)
		require.NoError(t, err)
		base := copCtx.GetBase()
		require.Equal(t, "t", base.TableInfo.Name.L)
		if tt.pkType != pkTypeCommonHandle {
			require.Nil(t, base.PrimaryKeyInfo)
		}
		expectedLen := len(tt.expectedCols)
		require.Equal(t, expectedLen, len(base.ColumnInfos))
		require.Equal(t, expectedLen, len(base.FieldTypes))
		require.Equal(t, expectedLen, len(base.ExprColumnInfos))
		for i, col := range base.ColumnInfos {
			require.Equal(t, tt.expectedCols[i], col.Name.L)
		}
	}
}

func TestResolveIndicesForHandle(t *testing.T) {
	type args struct {
		cols      []*expression.Column
		handleIDs []int64
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{
			name: "Basic 1",
			args: args{
				cols:      []*expression.Column{{ID: 1}, {ID: 2}, {ID: 3}},
				handleIDs: []int64{2},
			},
			want: []int{1},
		},
		{
			name: "Basic 2",
			args: args{
				cols:      []*expression.Column{{ID: 1}, {ID: 2}, {ID: 3}},
				handleIDs: []int64{3, 2, 1},
			},
			want: []int{2, 1, 0},
		},
		{
			name: "Basic 3",
			args: args{
				cols:      []*expression.Column{{ID: 1}, {ID: 2}, {ID: 3}},
				handleIDs: []int64{1, 3},
			},
			want: []int{0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveIndicesForHandle(tt.args.cols, tt.args.handleIDs)
			require.Equal(t, got, tt.want)
		})
	}
}

func TestCollectVirtualColumnOffsetsAndTypes(t *testing.T) {
	tests := []struct {
		name    string
		cols    []*expression.Column
		offsets []int
		fieldTp []int
	}{
		{
			name: "Basic 1",
			cols: []*expression.Column{
				{VirtualExpr: &expression.Constant{}, RetType: types.NewFieldType(1)},
				{VirtualExpr: nil},
				{VirtualExpr: &expression.Constant{}, RetType: types.NewFieldType(2)},
			},
			offsets: []int{0, 2},
			fieldTp: []int{1, 2},
		},
		{
			name: "Basic 2",
			cols: []*expression.Column{
				{VirtualExpr: nil},
				{VirtualExpr: &expression.Constant{}, RetType: types.NewFieldType(1)},
				{VirtualExpr: nil},
			},
			offsets: []int{1},
			fieldTp: []int{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := contextstatic.NewStaticEvalContext()
			gotOffsets, gotFt := collectVirtualColumnOffsetsAndTypes(ctx, tt.cols)
			require.Equal(t, gotOffsets, tt.offsets)
			require.Equal(t, len(gotFt), len(tt.fieldTp))
			for i, ft := range gotFt {
				require.Equal(t, int(ft.GetType()), tt.fieldTp[i])
			}
		})
	}
}

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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	stststypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	mockctx "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func wrapAsSCtx(exec *mock.MockRestrictedSQLExecutor) sessionctx.Context {
	sctx := mockctx.NewContext()
	sctx.SetValue(mock.RestrictedSQLExecutorKey{}, exec)
	return sctx
}

func TestGetStatsDeltaFromTableLocked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	tests := []struct {
		name                string
		expectedCount       int64
		expectedModifyCount int64
		expectedVersion     uint64
		execResult          []chunk.Row
		execError           error
	}{
		{
			name:                "No rows",
			expectedCount:       0,
			expectedModifyCount: 0,
			expectedVersion:     0,
			execResult:          nil,
			execError:           nil,
		},
		{
			name:                "One row",
			expectedCount:       1,
			expectedModifyCount: 1,
			expectedVersion:     1000,
			execResult: []chunk.Row{
				createStatsDeltaRow(1, 1, 1000),
			},
			execError: nil,
		},
		{
			name:                "Error",
			expectedCount:       0,
			expectedModifyCount: 0,
			expectedVersion:     0,
			execResult:          nil,
			execError:           errors.New("test error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec.EXPECT().ExecRestrictedSQL(
				util.StatsCtx,
				util.UseCurrentSessionOpt,
				selectDeltaSQL,
				gomock.Eq([]any{int64(1)}),
			).Return(tt.execResult, nil, tt.execError)

			count, modifyCount, version, err := getStatsDeltaFromTableLocked(wrapAsSCtx(exec), 1)
			if tt.execError != nil {
				require.Equal(t, tt.execError.Error(), err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedCount, count)
				require.Equal(t, tt.expectedModifyCount, modifyCount)
				require.Equal(t, tt.expectedVersion, version)
			}
		})
	}
}

func createStatsDeltaRow(count, modifyCount int64, version uint64) chunk.Row {
	c := chunk.NewChunkWithCapacity(
		[]*types.FieldType{
			types.NewFieldType(mysql.TypeLonglong),
			types.NewFieldType(mysql.TypeLonglong),
			types.NewFieldType(mysql.TypeLonglong),
		},
		1,
	)
	c.AppendInt64(0, count)
	c.AppendInt64(1, modifyCount)
	c.AppendUint64(2, version)
	return c.GetRow(0)
}

func TestUpdateStatsAndUnlockTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	tests := []struct {
		name      string
		tableID   int64
		execError error
	}{
		{
			name:      "Success",
			tableID:   1,
			execError: nil,
		},
		{
			name:      "Error",
			tableID:   1,
			execError: errors.New("test error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec.EXPECT().ExecRestrictedSQL(
				util.StatsCtx,
				util.UseCurrentSessionOpt,
				selectDeltaSQL,
				gomock.Eq([]any{tt.tableID}),
			).Return([]chunk.Row{createStatsDeltaRow(1, 1, 1000)}, nil, nil)

			if tt.execError == nil {
				exec.EXPECT().ExecRestrictedSQL(
					util.StatsCtx,
					util.UseCurrentSessionOpt,
					updateDeltaSQL,
					gomock.Eq([]any{uint64(1000), int64(1), int64(1), int64(1), int64(1)}),
				).Return(nil, nil, nil)
				exec.EXPECT().ExecRestrictedSQL(
					util.StatsCtx,
					util.UseCurrentSessionOpt,
					DeleteLockSQL,
					gomock.Eq([]any{tt.tableID}),
				).Return(nil, nil, nil)
			} else {
				exec.EXPECT().ExecRestrictedSQL(
					util.StatsCtx,
					util.UseCurrentSessionOpt,
					updateDeltaSQL,
					gomock.Eq([]any{uint64(1000), int64(1), int64(1), int64(1), int64(1)}),
				).Return(nil, nil, tt.execError)
			}

			err := updateStatsAndUnlockTable(wrapAsSCtx(exec), tt.tableID)
			if tt.execError != nil {
				require.Equal(t, tt.execError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoveLockedTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// Return table 1 and partition p1 are locked.
	table := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	table.AppendInt64(0, int64(1))
	partition := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	partition.AppendInt64(0, int64(4))
	rows := []chunk.Row{table.GetRow(0), partition.GetRow(0)}
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectSQL,
	).Return(rows, nil, nil)

	// No rows returned for table 1, because the delta is only stored in partition p1.
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectDeltaSQL,
		gomock.Eq([]any{int64(1)}),
	).Return([]chunk.Row{}, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		updateDeltaSQL,
		gomock.Eq([]any{uint64(0), int64(0), int64(0), int64(0), int64(1)}),
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		DeleteLockSQL,
		gomock.Eq([]any{int64(1)}),
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectDeltaSQL,
		gomock.Eq([]any{int64(4)}),
	).Return([]chunk.Row{createStatsDeltaRow(1, 1, 1000)}, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		updateDeltaSQL,
		gomock.Eq([]any{uint64(1000), int64(1), int64(1), int64(1), int64(4)}),
	).Return(nil, nil, nil)
	// Patch the delta to table 1 from partition p1.
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		updateDeltaSQL,
		gomock.Eq([]any{uint64(1000), int64(1), int64(1), int64(1), int64(1)}),
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		DeleteLockSQL,
		gomock.Eq([]any{int64(4)}),
	).Return(nil, nil, nil)

	tables := map[int64]*stststypes.StatsLockTable{
		1: {
			FullName: "test.t1",
			PartitionInfo: map[int64]string{
				4: "p1",
			},
		},
		2: {
			FullName: "test.t2",
		},
		3: {
			FullName: "test.t3",
		},
	}

	msg, err := RemoveLockedTables(
		wrapAsSCtx(exec),
		tables,
	)
	require.NoError(t, err)
	require.Equal(t, "skip unlocking unlocked tables: test.t2, test.t3, other tables unlocked successfully", msg)
}

func TestRemoveLockedPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// Return table 2 is locked.
	c := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	c.AppendInt64(0, int64(2))
	rows := []chunk.Row{c.GetRow(0)}
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectSQL,
	).Return(rows, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectDeltaSQL,
		gomock.Eq([]any{int64(2)}),
	).Return([]chunk.Row{createStatsDeltaRow(1, 1, 1000)}, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		updateDeltaSQL,
		gomock.Eq([]any{uint64(1000), int64(1), int64(1), int64(1), int64(2)}),
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		updateDeltaSQL,
		gomock.Eq([]any{uint64(1000), int64(1), int64(1), int64(1), int64(1)}),
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		DeleteLockSQL,
		gomock.Eq([]any{int64(2)}),
	).Return(nil, nil, nil)

	pidAndNames := map[int64]string{
		2: "p1",
	}

	msg, err := RemoveLockedPartitions(
		wrapAsSCtx(exec),
		1,
		"test.t1",
		pidAndNames,
	)
	require.NoError(t, err)
	require.Equal(t, "", msg)
}

func TestRemoveLockedPartitionsFailedIfTheWholeTableIsLocked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// Return table 2 is locked.
	c := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	c.AppendInt64(0, int64(1))
	rows := []chunk.Row{c.GetRow(0)}
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectSQL,
	).Return(rows, nil, nil)

	pidAndNames := map[int64]string{
		2: "p1",
	}

	msg, err := RemoveLockedPartitions(
		wrapAsSCtx(exec),
		1,
		"test.t1",
		pidAndNames,
	)
	require.NoError(t, err)
	require.Equal(t, "skip unlocking partitions of locked table: test.t1", msg)
}

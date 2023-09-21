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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

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
			ctx := context.Background()
			exec.EXPECT().ExecRestrictedSQL(
				ctx,
				useCurrentSession,
				selectDeltaSQL,
				gomock.Eq([]interface{}{int64(1)}),
			).Return(tt.execResult, nil, tt.execError)

			count, modifyCount, version, err := getStatsDeltaFromTableLocked(ctx, 1, exec)
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
			ctx := context.Background()
			exec.EXPECT().ExecRestrictedSQL(
				ctx,
				useCurrentSession,
				selectDeltaSQL,
				gomock.Eq([]interface{}{tt.tableID}),
			).Return([]chunk.Row{createStatsDeltaRow(1, 1, 1000)}, nil, nil)

			if tt.execError == nil {
				exec.EXPECT().ExecRestrictedSQL(
					ctx,
					useCurrentSession,
					updateDeltaSQL,
					gomock.Eq([]interface{}{uint64(1000), int64(1), int64(1), int64(1)}),
				).Return(nil, nil, nil)
				exec.EXPECT().ExecRestrictedSQL(
					ctx,
					useCurrentSession,
					DeleteLockSQL,
					gomock.Eq([]interface{}{tt.tableID}),
				).Return(nil, nil, nil)
			} else {
				exec.EXPECT().ExecRestrictedSQL(
					ctx,
					useCurrentSession,
					updateDeltaSQL,
					gomock.Eq([]interface{}{uint64(1000), int64(1), int64(1), int64(1)}),
				).Return(nil, nil, tt.execError)
			}

			err := updateStatsAndUnlockTable(ctx, exec, tt.tableID)
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

	// Executed SQL should be:
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		gomock.Eq("BEGIN PESSIMISTIC"),
	)

	// Return table 1 is locked.
	c := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	c.AppendInt64(0, int64(1))
	rows := []chunk.Row{c.GetRow(0)}
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		selectSQL,
	).Return(rows, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		selectDeltaSQL,
		gomock.Eq([]interface{}{int64(1)}),
	).Return([]chunk.Row{createStatsDeltaRow(1, 1, 1000)}, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		updateDeltaSQL,
		gomock.Eq([]interface{}{uint64(1000), int64(1), int64(1), int64(1)}),
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		DeleteLockSQL,
		gomock.Eq([]interface{}{int64(1)}),
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		"COMMIT",
	)

	tidsAndNames := map[int64]string{
		1: "test.t1",
		2: "test.t2",
		3: "test.t3",
	}
	pidAndNames := map[int64]string{
		4: "p1",
	}

	msg, err := RemoveLockedTables(
		exec,
		tidsAndNames,
		pidAndNames,
	)
	require.NoError(t, err)
	require.Equal(t, "skip unlocking unlocked tables: test.t2, test.t3, other tables unlocked successfully", msg)
}

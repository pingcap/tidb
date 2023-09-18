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
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGenerateSkippedMessage(t *testing.T) {
	tests := []struct {
		name          string
		totalTableIDs []int64
		tables        []string
		action        string
		status        string
		expectedMsg   string
	}{
		{
			name:          "no duplicate tables when locking",
			totalTableIDs: []int64{1, 2, 3},
			action:        lockAction,
			status:        lockedStatus,
			expectedMsg:   "",
		},
		{
			name:          "one duplicate table when locking",
			totalTableIDs: []int64{1},
			tables:        []string{"t1"},
			action:        lockAction,
			status:        lockedStatus,
			expectedMsg:   "skip locking locked table: t1",
		},
		{
			name:          "multiple duplicate tables when locking",
			totalTableIDs: []int64{1, 2, 3, 4},
			tables:        []string{"t1", "t2", "t3"},
			action:        lockAction,
			status:        lockedStatus,
			expectedMsg:   "skip locking locked tables: t1, t2, t3, other tables locked successfully",
		},
		{
			name:          "all tables are duplicate when locking",
			totalTableIDs: []int64{1, 2, 3, 4},
			tables:        []string{"t1", "t2", "t3", "t4"},
			action:        lockAction,
			status:        lockedStatus,
			expectedMsg:   "skip locking locked tables: t1, t2, t3, t4",
		},
		{
			name:          "all tables are duplicate when unlocking",
			totalTableIDs: []int64{1, 2, 3, 4},
			tables:        []string{"t1", "t2", "t3", "t4"},
			action:        unlockAction,
			status:        unlockedStatus,
			expectedMsg:   "skip unlocking unlocked tables: t1, t2, t3, t4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := generateSkippedMessage(tt.totalTableIDs, tt.tables, tt.action, tt.status)
			require.Equal(t, tt.expectedMsg, msg)
		})
	}
}

func TestInsertIntoStatsTableLocked(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// Executed SQL should be:
	exec.EXPECT().ExecRestrictedSQL(
		gomock.Eq(ctx),
		useCurrentSession,
		gomock.Eq(insertSQL),
		gomock.Eq([]interface{}{int64(1), int64(1)}),
	)
	err := insertIntoStatsTableLocked(ctx, exec, 1)
	require.NoError(t, err)

	// Error should be returned when ExecRestrictedSQL returns error.
	exec.EXPECT().ExecRestrictedSQL(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(nil, nil, errors.New("test error"))

	err = insertIntoStatsTableLocked(ctx, exec, 1)
	require.Equal(t, "test error", err.Error())
}

func TestAddLockedTables(t *testing.T) {
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
		insertSQL,
		gomock.Eq([]interface{}{int64(2), int64(2)}),
	)
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		insertSQL,
		gomock.Eq([]interface{}{int64(3), int64(3)}),
	)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		insertSQL,
		gomock.Eq([]interface{}{int64(4), int64(4)}),
	)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		useCurrentSession,
		"COMMIT",
	)

	msg, err := AddLockedTables(
		exec,
		[]int64{1, 2, 3},
		[]int64{4},
		[]*ast.TableName{
			{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t1")},
			{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t2")},
			{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t3")}},
	)
	require.NoError(t, err)
	require.Equal(t, "skip locking locked tables: test.t1, other tables locked successfully", msg)
}

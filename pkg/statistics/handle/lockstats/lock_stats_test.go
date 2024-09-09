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
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGenerateSkippedTablesMessage(t *testing.T) {
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
			msg := generateStableSkippedTablesMessage(len(tt.totalTableIDs), tt.tables, tt.action, tt.status)
			require.Equal(t, tt.expectedMsg, msg)
		})
	}
}

func TestGenerateSkippedPartitionsMessage(t *testing.T) {
	tests := []struct {
		name              string
		tableName         string
		totalPartitionIDs []int64
		partitions        []string
		action            string
		status            string
		expectedMsg       string
	}{
		{
			name:              "no duplicate partitions when locking",
			tableName:         "test.t",
			totalPartitionIDs: []int64{1, 2, 3},
			action:            lockAction,
			status:            lockedStatus,
			expectedMsg:       "",
		},
		{
			name:              "one duplicate table when locking",
			tableName:         "test.t",
			totalPartitionIDs: []int64{1},
			partitions:        []string{"t1"},
			action:            lockAction,
			status:            lockedStatus,
			expectedMsg:       "skip locking locked partition of table test.t: t1",
		},
		{
			name:              "multiple duplicate partitions when locking",
			tableName:         "test.t",
			totalPartitionIDs: []int64{1, 2, 3, 4},
			partitions:        []string{"t1", "t2", "t3"},
			action:            lockAction,
			status:            lockedStatus,
			expectedMsg:       "skip locking locked partitions of table test.t: t1, t2, t3, other partitions locked successfully",
		},
		{
			name:              "all partitions are duplicate when locking",
			tableName:         "test.t",
			totalPartitionIDs: []int64{1, 2, 3, 4},
			partitions:        []string{"t1", "t2", "t3", "t4"},
			action:            lockAction,
			status:            lockedStatus,
			expectedMsg:       "skip locking locked partitions of table test.t: t1, t2, t3, t4",
		},
		{
			name:              "all partitions are duplicate when unlocking",
			tableName:         "test.t",
			totalPartitionIDs: []int64{1, 2, 3, 4},
			partitions:        []string{"t1", "t2", "t3", "t4"},
			action:            unlockAction,
			status:            unlockedStatus,
			expectedMsg:       "skip unlocking unlocked partitions of table test.t: t1, t2, t3, t4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := generateStableSkippedPartitionsMessage(tt.totalPartitionIDs, tt.tableName, tt.partitions, tt.action, tt.status)
			require.Equal(t, tt.expectedMsg, msg)
		})
	}
}

func TestInsertIntoStatsTableLocked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// Executed SQL should be:
	exec.EXPECT().ExecRestrictedSQL(
		util.StatsCtx,
		util.UseCurrentSessionOpt,
		gomock.Eq(insertSQL),
		gomock.Eq([]any{int64(1), int64(1)}),
	)
	err := insertIntoStatsTableLocked(wrapAsSCtx(exec), 1)
	require.NoError(t, err)

	// Error should be returned when ExecRestrictedSQL returns error.
	exec.EXPECT().ExecRestrictedSQL(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(nil, nil, errors.New("test error"))

	err = insertIntoStatsTableLocked(wrapAsSCtx(exec), 1)
	require.Equal(t, "test error", err.Error())
}

func TestAddLockedTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// Return table 1 is locked.
	c := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	c.AppendInt64(0, int64(1))
	rows := []chunk.Row{c.GetRow(0)}
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectSQL,
	).Return(rows, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		insertSQL,
		gomock.Eq([]any{int64(2), int64(2)}),
	)
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		insertSQL,
		gomock.Eq([]any{int64(3), int64(3)}),
	)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		insertSQL,
		gomock.Eq([]any{int64(4), int64(4)}),
	)

	tables := map[int64]*statstypes.StatsLockTable{
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

	msg, err := AddLockedTables(
		wrapAsSCtx(exec),
		tables,
	)
	require.NoError(t, err)
	require.Equal(t, "skip locking locked tables: test.t1, other tables locked successfully", msg)
}

func TestAddLockedPartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// No table is locked.
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectSQL,
	).Return(nil, nil, nil)

	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		insertSQL,
		gomock.Eq([]any{int64(2), int64(2)}),
	)
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		insertSQL,
		gomock.Eq([]any{int64(3), int64(3)}),
	)

	msg, err := AddLockedPartitions(
		wrapAsSCtx(exec),
		1,
		"test.t1",
		map[int64]string{
			2: "p1",
			3: "p2",
		},
	)
	require.NoError(t, err)
	require.Equal(t, "", msg)
}

func TestAddLockedPartitionsFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	exec := mock.NewMockRestrictedSQLExecutor(ctrl)

	// Return table 1 is locked.
	c := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)
	c.AppendInt64(0, int64(1))
	rows := []chunk.Row{c.GetRow(0)}
	exec.EXPECT().ExecRestrictedSQL(
		gomock.All(&ctxMatcher{}),
		util.UseCurrentSessionOpt,
		selectSQL,
	).Return(rows, nil, nil)

	msg, err := AddLockedPartitions(
		wrapAsSCtx(exec),
		1,
		"test.t1",
		map[int64]string{
			2: "p1",
			3: "p2",
		},
	)
	require.NoError(t, err)
	require.Equal(t, "skip locking partitions of locked table: test.t1", msg)
}

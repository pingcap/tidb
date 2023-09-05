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
	"github.com/pingcap/tidb/util/sqlexec/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGenerateDuplicateTablesMessage(t *testing.T) {
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
			msg := generateSkippedTablesMessage(tt.totalTableIDs, tt.tables, tt.action, tt.status)
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
		gomock.Eq(useCurrentSession),
		gomock.Eq("INSERT INTO mysql.stats_table_locked (table_id) VALUES (%?) ON DUPLICATE KEY UPDATE table_id = %?"),
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

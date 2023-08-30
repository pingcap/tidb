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

package handle

import (
	"testing"

	"github.com/stretchr/testify/require"
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

func TestRemoveIfTableLocked(t *testing.T) {
	tests := []struct {
		name          string
		tableLocked   []int64
		removeTableID int64
		expectedExist bool
		expectedTable []int64
	}{
		{
			name:          "no table locked",
			tableLocked:   []int64{},
			removeTableID: 1,
			expectedExist: false,
			expectedTable: []int64{},
		},
		{
			name:          "table locked",
			tableLocked:   []int64{1, 2, 3},
			removeTableID: 1,
			expectedExist: true,
			expectedTable: []int64{2, 3},
		},
		{
			name:          "table not locked",
			tableLocked:   []int64{1, 2, 3},
			removeTableID: 4,
			expectedExist: false,
			expectedTable: []int64{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exist, tables := removeIfTableLocked(tt.tableLocked, tt.removeTableID)
			require.Equal(t, tt.expectedExist, exist)
			require.Equal(t, tt.expectedTable, tables)
		})
	}
}

func TestLockTableIndexOf(t *testing.T) {
	tests := []struct {
		name          string
		tableLocked   []int64
		tableID       int64
		expectedIndex int
	}{
		{
			name:          "no table locked",
			tableLocked:   []int64{},
			tableID:       1,
			expectedIndex: -1,
		},
		{
			name:          "table locked",
			tableLocked:   []int64{1, 2, 3},
			tableID:       1,
			expectedIndex: 0,
		},
		{
			name:          "table not locked",
			tableLocked:   []int64{1, 2, 3},
			tableID:       4,
			expectedIndex: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := lockTableIndexOf(tt.tableLocked, tt.tableID)
			require.Equal(t, tt.expectedIndex, index)
		})
	}
}

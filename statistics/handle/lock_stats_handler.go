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
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/statistics/handle/lockstats"
	"github.com/pingcap/tidb/util/sqlexec"
)

// AddLockedTables add locked tables id to store.
// - tids: table ids of which will be locked.
// - pids: partition ids of which will be locked.
// - tables: table names of which will be locked.
// Return the message of skipped tables and error.
func (h *Handle) AddLockedTables(tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return lockstats.AddLockedTables(h.mu.ctx.(sqlexec.SQLExecutor), tids, pids, tables)
}

// RemoveLockedTables remove tables from table locked array.
// - tids: table ids of which will be unlocked.
// - pids: partition ids of which will be unlocked.
// - tables: table names of which will be unlocked.
// Return the message of skipped tables and error.
func (h *Handle) RemoveLockedTables(tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return lockstats.RemoveLockedTables(h.mu.ctx.(sqlexec.SQLExecutor), tids, pids, tables)
}

// IsTableLocked check whether table is locked in handle with Handle.Mutex
func (h *Handle) IsTableLocked(tableID int64) (bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.isTableLockedWithoutLock(tableID)
}

// loadLockedTablesWithoutLock load locked tables from store without Handle.Mutex.
func (h *Handle) loadLockedTablesWithoutLock() ([]int64, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)

	return lockstats.LoadLockedTables(ctx, exec)
}

// isTableLockedWithoutLock check whether table is locked in handle without Handle.Mutex
func (h *Handle) isTableLockedWithoutLock(tableID int64) (bool, error) {
	tableLocked, err := h.loadLockedTablesWithoutLock()
	if err != nil {
		return false, err
	}
	return lockstats.IsTableLocked(tableLocked, tableID), nil
}

// GetTableLockedAndClearForTest for unit test only
func (h *Handle) GetTableLockedAndClearForTest() ([]int64, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.loadLockedTablesWithoutLock()
}

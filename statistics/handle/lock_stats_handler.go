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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/statistics/handle/lockstats"
	"github.com/pingcap/tidb/util/sqlexec"
)

// LockTables add locked tables id to store.
// - tidAndNames: table ids and names of which will be locked.
// - pidAndNames: partition ids and names of which will be locked.
// Return the message of skipped tables and error.
func (h *Handle) LockTables(tidAndNames map[int64]string, pidAndNames map[int64]string) (string, error) {
	se, err := h.pool.Get()
	if err != nil {
		return "", errors.Trace(err)
	}
	defer h.pool.Put(se)

	exec := se.(sqlexec.RestrictedSQLExecutor)

	return lockstats.AddLockedTables(exec, tidAndNames, pidAndNames)
}

// LockPartitions add locked partitions id to store.
// If the whole table is locked, then skip all partitions of the table.
// - tid: table id of which will be locked.
// - tableName: table name of which will be locked.
// - pidNames: partition ids of which will be locked.
// Return the message of skipped tables and error.
// Note: If the whole table is locked, then skip all partitions of the table.
func (h *Handle) LockPartitions(
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (string, error) {
	se, err := h.pool.Get()
	if err != nil {
		return "", errors.Trace(err)
	}
	defer h.pool.Put(se)

	exec := se.(sqlexec.RestrictedSQLExecutor)

	return lockstats.AddLockedPartitions(exec, tid, tableName, pidNames)
}

// RemoveLockedTables remove tables from table locked records.
// - tidAndNames:  table ids and names of which will be unlocked.
// - pidAndNames: partition ids and names of which will be unlocked.
// Return the message of skipped tables and error.
func (h *Handle) RemoveLockedTables(tidAndNames map[int64]string, pidAndNames map[int64]string) (string, error) {
	se, err := h.pool.Get()
	if err != nil {
		return "", errors.Trace(err)
	}
	defer h.pool.Put(se)

	exec := se.(sqlexec.RestrictedSQLExecutor)
	return lockstats.RemoveLockedTables(exec, tidAndNames, pidAndNames)
}

// RemoveLockedPartitions remove partitions from table locked records.
// - tid: table id of which will be unlocked.
// - tableName: table name of which will be unlocked.
// - pidNames: partition ids of which will be unlocked.
// Note: If the whole table is locked, then skip all partitions of the table.
func (h *Handle) RemoveLockedPartitions(
	tid int64,
	tableName string,
	pidNames map[int64]string,
) (string, error) {
	se, err := h.pool.Get()
	if err != nil {
		return "", errors.Trace(err)
	}
	defer h.pool.Put(se)

	exec := se.(sqlexec.RestrictedSQLExecutor)
	return lockstats.RemoveLockedPartitions(exec, tid, tableName, pidNames)
}

// GetLockedTables returns the locked status of the given tables.
// Note: This function query locked tables from store, so please try to batch the query.
func (h *Handle) GetLockedTables(tableIDs ...int64) (map[int64]struct{}, error) {
	tableLocked, err := h.queryLockedTables()
	if err != nil {
		return nil, err
	}

	return lockstats.GetLockedTables(tableLocked, tableIDs...), nil
}

// queryLockedTables query locked tables from store.
func (h *Handle) queryLockedTables() (map[int64]struct{}, error) {
	se, err := h.pool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer h.pool.Put(se)

	exec := se.(sqlexec.RestrictedSQLExecutor)
	return lockstats.QueryLockedTables(exec)
}

// GetTableLockedAndClearForTest for unit test only.
func (h *Handle) GetTableLockedAndClearForTest() (map[int64]struct{}, error) {
	return h.queryLockedTables()
}

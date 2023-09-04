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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/sqlexec"
)

// QueryLockedTables loads locked tables from mysql.stats_table_locked.
// Return it as a map for fast query.
func QueryLockedTables(ctx context.Context, exec sqlexec.SQLExecutor) (map[int64]struct{}, error) {
	recordSet, err := exec.ExecuteInternal(ctx, "SELECT table_id FROM mysql.stats_table_locked")
	if err != nil {
		return nil, err
	}
	rows, err := sqlexec.DrainRecordSet(ctx, recordSet, maxChunkSize)
	if err != nil {
		return nil, err
	}
	tableLocked := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		tableLocked[row.GetInt64(0)] = struct{}{}
	}
	return tableLocked, nil
}

// GetTablesLockedStatuses check whether table is locked.
func GetTablesLockedStatuses(tableLocked map[int64]struct{}, tableIDs ...int64) map[int64]bool {
	lockedTableStatus := make(map[int64]bool, len(tableIDs))

	for _, tid := range tableIDs {
		if _, ok := tableLocked[tid]; ok {
			lockedTableStatus[tid] = true
			continue
		}
		lockedTableStatus[tid] = false
	}

	return lockedTableStatus
}

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(ctx context.Context, exec sqlexec.SQLExecutor, err error) error {
	if err == nil {
		_, err = exec.ExecuteInternal(ctx, "commit")
	} else {
		_, err1 := exec.ExecuteInternal(ctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}

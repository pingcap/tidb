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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

const selectSQL = "SELECT table_id FROM mysql.stats_table_locked"

// QueryLockedTables loads locked tables from mysql.stats_table_locked.
// Return it as a map for fast query.
func QueryLockedTables(sctx sessionctx.Context) (map[int64]struct{}, error) {
	rows, _, err := util.ExecRows(sctx, selectSQL)
	if err != nil {
		return nil, err
	}
	tableLocked := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		tableLocked[row.GetInt64(0)] = struct{}{}
	}
	return tableLocked, nil
}

// GetLockedTables returns the locked status of the given tables.
func GetLockedTables(tableLocked map[int64]struct{}, tableIDs ...int64) map[int64]struct{} {
	lockedTables := make(map[int64]struct{}, len(tableLocked))
	if len(tableLocked) == 0 {
		return lockedTables
	}

	for _, tid := range tableIDs {
		if _, ok := tableLocked[tid]; ok {
			lockedTables[tid] = struct{}{}
			continue
		}
	}

	return lockedTables
}

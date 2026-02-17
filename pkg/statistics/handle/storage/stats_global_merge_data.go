// Copyright 2026 PingCAP, Inc.
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

package storage

import (
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// Merge data type constants for stats_global_merge_data.type column.
const (
	// MergeDataTypeFMSketchCol is the type for column FM sketch data.
	MergeDataTypeFMSketchCol = 0
	// MergeDataTypeFMSketchIdx is the type for index FM sketch data.
	MergeDataTypeFMSketchIdx = 1
)

// fmSketchMergeDataType converts is_index (0 or 1) to the corresponding merge data type.
func fmSketchMergeDataType(isIndex int64) int {
	if isIndex == 1 {
		return MergeDataTypeFMSketchIdx
	}
	return MergeDataTypeFMSketchCol
}

// SaveFMSketchToMergeData saves FM sketch data into stats_global_merge_data.
func SaveFMSketchToMergeData(sctx sessionctx.Context, tableID int64, isIndex int64, histID int64, value []byte) error {
	tp := fmSketchMergeDataType(isIndex)
	_, err := util.Exec(sctx, "replace into mysql.stats_global_merge_data (table_id, type, hist_id, value) values (%?, %?, %?, %?)",
		tableID, tp, histID, value)
	return err
}

// DeleteFMSketchFromMergeData deletes FM sketch data from stats_global_merge_data for a specific column/index.
func DeleteFMSketchFromMergeData(sctx sessionctx.Context, tableID int64, isIndex int64, histID int64) error {
	tp := fmSketchMergeDataType(isIndex)
	_, err := util.Exec(sctx, "delete from mysql.stats_global_merge_data where table_id = %? and type = %? and hist_id = %?",
		tableID, tp, histID)
	return err
}

// FMSketchFromMergeData reads FM sketch data from stats_global_merge_data.
// Returns nil, nil if no data is found.
func FMSketchFromMergeData(sctx sessionctx.Context, tableID int64, isIndex, histID int64) (*statistics.FMSketch, error) {
	tp := fmSketchMergeDataType(isIndex)
	rows, _, err := util.ExecRows(sctx, "select value from mysql.stats_global_merge_data where table_id = %? and type = %? and hist_id = %?",
		tableID, tp, histID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return statistics.DecodeFMSketch(rows[0].GetBytes(0))
}

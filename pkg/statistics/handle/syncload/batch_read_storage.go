// Copyright 2024 PingCAP, Inc.
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

package syncload

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func generateMetaPredict(conditions []*batchSyncLoadTask) string {
	var sqlParts []string
	template := "(table_id = %d and hist_id = %d and is_index = %d)"
	for _, cond := range conditions {
		var isIndex int
		if cond.task.Item.IsIndex {
			isIndex = 0
		}
		part := fmt.Sprintf(template, cond.task.Item.TableItemID.TableID, cond.task.Item.TableItemID.ID, isIndex)
		sqlParts = append(sqlParts, part)
	}

	return strings.Join(sqlParts, " or ")
}

func batchHistMetaFromStorageWithHighPriority(sctx sessionctx.Context, bc *batchContext, tasks []*batchSyncLoadTask) (ok bool, err error) {
	rows, _, err := util.ExecRows(sctx,
		"select high_priority distinct_count, version, null_count, tot_col_size, stats_ver, correlation, flag, last_analyze_pos, table_id, hist_id, is_index from mysql.stats_histograms where "+
			generateMetaPredict(tasks),
	)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	var tp *types.FieldType
	for _, row := range rows {
		id := row.GetInt64(8)
		hist_id := row.GetInt64(9)
		is_index := row.GetInt64(10)
		if is_index == 1 {
			tp = types.NewFieldType(mysql.TypeBlob)
		} else {
			tp = bc.tables[id].columns[hist_id].Tp
		}
		fullLoad := bc.GetFullLoad(id, hist_id)

		hist := statistics.NewHistogram(id, row.GetInt64(0), row.GetInt64(2), row.GetUint64(1), tp, chunk.InitialCapacity, row.GetInt64(3))
		hist.Correlation = row.GetFloat64(5)
		lastPos := row.GetDatum(7, types.NewFieldType(mysql.TypeBlob))

		lastAnalyzePos := &lastPos
		statsVer := row.GetInt64(4)
		flag := row.GetInt64(6)
		if is_index == 1 {
			idxHist := &statistics.Index{
				Histogram:  *hist,
				CMSketch:   nil,
				TopN:       nil,
				FMSketch:   nil,
				Info:       nil,
				StatsVer:   statsVer,
				Flag:       flag,
				PhysicalID: id,
			}
			if statsVer != statistics.Version0 {
				if fullLoad {
					idxHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
				} else {
					idxHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
				}
			}
			lastAnalyzePos.Copy(&idxHist.LastAnalyzePos)
			bc.tables[id].indexs[hist_id] = idxHist
		} else {
			isPkIsHandle := bc.tableInfo[id].IsPkIsHandle
			wrapper, ok := bc.GetStatsWrapper(id, hist_id)
			if !ok {
				continue
			}
			colHist := &statistics.Column{
				PhysicalID: id,
				Histogram:  *hist,
				Info:       nil,
				CMSketch:   nil,
				TopN:       nil,
				FMSketch:   nil,
				IsHandle:   isPkIsHandle && mysql.HasPriKeyFlag(wrapper.colInfo.GetFlag()),
				StatsVer:   statsVer,
			}
			if colHist.StatsAvailable() {
				if fullLoad {
					colHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
				} else {
					colHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
				}
			}
			bc.tables[id].columns[hist_id] = colHist
		}
	}
	return true, nil
}

// HistogramFromStorageWithPriority wraps the HistogramFromStorage with the given kv.Priority.
// Sync load and async load will use high priority to get data.
func HistogramFromStorageWithPriority(
	sctx sessionctx.Context,
	bc *batchContext, tasks []*batchSyncLoadTask,
) error {
	rows, fields, err := util.ExecRows(sctx, "select high_priority count, repeats, lower_bound, upper_bound, ndv, table_id, hist_id, is_index from mysql.stats_bucketswhere "+generateMetaPredict(tasks)+" order by table_id,is_index,bucket_id")
	if err != nil {
		return errors.Trace(err)
	}
	var table_id, hist_id int64
	for idx, row := range rows {
		tid := row.GetInt64(5)
		hid := row.GetInt64(6)
		row.GetInt64(7)
		if table_id != tid || hist_id != hid {
			table_id = tid
			hist_id = hid
		}
	}

}

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

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
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

func BatchHistMetaFromStorageWithHighPriority(sctx sessionctx.Context, bc batchContext, tasks []*batchSyncLoadTask) (ok bool, err error) {
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
}

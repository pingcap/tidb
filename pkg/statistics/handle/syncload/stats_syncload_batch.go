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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"go.uber.org/zap"
)

const batchSize int = 10

type batchSyncLoadTask struct {
	wrapper *statsWrapper
	task    *statstypes.NeededItemTask
}

func (s *statsSyncLoad) startBatchSyncLoad(sctx sessionctx.Context, tasks []*statstypes.NeededItemTask) {
	taskW := make([]*batchSyncLoadTask, 0, len(tasks))
	bctx := newBatchContext()
	for _, task := range tasks {
		item := task.Item.TableItemID
		tbl, ok := s.statsHandle.Get(item.TableID)
		if !ok {
			// send result
			result := stmtctx.StatsLoadResult{Item: task.Item.TableItemID}
			task.ResultCh <- result
			continue
		}
		bctx.SetFullLoad(item.TableID, item.ID, task.Item.FullLoad)
		taskW = append(taskW, &batchSyncLoadTask{
			wrapper: newStatsWrapper(tbl, item, task.Item.FullLoad, s.updateCachedItem),
			task:    task,
		})
	}
	ok, err := batchHistMetaFromStorageWithHighPriority(sctx, bctx, taskW)
	if !ok {
		if err != nil {
			statslogutil.StatsLogger().Warn("fail to batch load meta", zap.Error(err))
		}
	}

}

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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

func (h *ddlHandlerImpl) onTruncatePartitions(t *util.DDLEvent) error {
	globalTableInfo, addedPartInfo, droppedPartInfo := t.GetTruncatePartitionInfo()
	// First, add the new stats meta record for the new partitions.
	for _, def := range addedPartInfo.Definitions {
		if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
			return err
		}
	}

	// Second, clean up the old stats meta from global stats meta for the dropped partitions.
	// Do not forget to put those operations in one transaction.
	if err := util.CallWithSCtx(h.statsHandler.SPool(), func(sctx sessionctx.Context) error {
		count := int64(0)
		for _, def := range droppedPartInfo.Definitions {
			// Get the count and modify count of the partition.
			tableCount, _, _, err := storage.StatsMetaCountAndModifyCount(sctx, def.ID)
			if err != nil {
				return err
			}
			count += tableCount
		}

		if count != 0 {
			lockedTables, err := lockstats.QueryLockedTables(sctx)
			if err != nil {
				return errors.Trace(err)
			}
			isLocked := false
			if _, ok := lockedTables[globalTableInfo.ID]; ok {
				isLocked = true
			}
			startTS, err := util.GetStartTS(sctx)
			if err != nil {
				return errors.Trace(err)
			}

			// Because we drop the partition, we should subtract the count from the global stats.
			// Note: We don't need to subtract the modify count from the global stats.
			// For example:
			// 1. The partition has 100 rows.
			// 2. We deleted 100 rows from the partition.
			// 3. The global stats has `count - 100 rows` and 100 modify count.
			// 4. We drop the partition.
			// 5. The global stats should not be `count` and 0 modify count. We need to keep the modify count.
			delta := -count
			err = storage.UpdateStatsMeta(
				sctx,
				startTS,
				variable.TableDelta{Count: count, Delta: delta},
				globalTableInfo.ID,
				isLocked,
			)
			return err
		}

		return nil
	}, util.FlagWrapTxn); err != nil {
		return err
	}

	// Third, clean up the old stats meta from partition stats meta for the dropped partitions.
	// It's OK to put those operations in different transactions. Because it will not affect the correctness.
	for _, def := range droppedPartInfo.Definitions {
		if err := h.statsWriter.ResetTableStats2KVForDrop(def.ID); err != nil {
			return err
		}
	}

	return nil
}

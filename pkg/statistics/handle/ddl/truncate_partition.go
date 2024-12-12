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
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

func (h *ddlHandlerImpl) onTruncatePartitions(t *notifier.SchemaChangeEvent) error {
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
		return updateGlobalTableStats4TruncatePartition(
			util.StatsCtx,
			sctx,
			globalTableInfo,
			droppedPartInfo,
		)
	}, util.FlagWrapTxn); err != nil {
		return err
	}

	// Third, clean up the old stats meta from partition stats meta for the dropped partitions.
	// It's OK to put those operations in different transactions. Because it will not affect the correctness.
	for _, def := range droppedPartInfo.Definitions {
		if err := h.statsWriter.UpdateStatsMetaVersionForGC(def.ID); err != nil {
			return err
		}
	}

	return nil
}

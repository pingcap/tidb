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
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

func (h *ddlHandlerImpl) onReorganizePartitions(t *util.DDLEvent) error {
	globalTableInfo,
		addedPartInfo,
		droppedPartitionInfo := t.GetReorganizePartitionInfo()
	// Avoid updating global stats as the data remains unchanged.
	// For new partitions, it's crucial to correctly insert the count and modify count correctly.
	// However, this is challenging due to the need to know the count of the new partitions.
	// Given that a partition can be split into two, determining the count of the new partitions is so hard.
	// It's acceptable to not update it immediately,
	// as the new partitions will be analyzed shortly due to the absence of statistics for them.
	// Therefore, the auto-analyze worker will handle them in the near future.
	for _, def := range addedPartInfo.Definitions {
		if err := h.statsWriter.InsertTableStats2KV(globalTableInfo, def.ID); err != nil {
			return err
		}
	}

	// Reset the partition stats.
	// It's OK to put those operations in different transactions. Because it will not affect the correctness.
	for _, def := range droppedPartitionInfo.Definitions {
		if err := h.statsWriter.UpdateStatsMetaVersionForGC(def.ID); err != nil {
			return err
		}
	}

	return nil
}

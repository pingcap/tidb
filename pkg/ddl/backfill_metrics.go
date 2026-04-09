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

package ddl

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// getBackfillTotalByTableID returns the Counter showing the speed of backfilling for the given table ID and type label.
// It also tracks the label for later cleanup by tableID.
func getBackfillTotalByTableID(tableID int64, label, schemaName, tableName, optionalColOrIdxName string) prometheus.Counter {
	return metrics.GetBackfillTotalByTableID(tableID, label, schemaName, tableName, optionalColOrIdxName)
}

// getBackfillProgressByTableID returns the Gauge showing the percentage progress for the given table ID and type label.
// It also tracks the label for later cleanup by tableID.
func getBackfillProgressByTableID(tableID int64, label, schemaName, tableName, optionalColOrIdxName string) prometheus.Gauge {
	return metrics.GetBackfillProgressByTableID(tableID, label, schemaName, tableName, optionalColOrIdxName)
}

func backfillProgressLabel(jobType model.ActionType, mergingTmpIdx bool) string {
	switch jobType {
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		if mergingTmpIdx {
			return metrics.LblAddIndexMerge
		}
		return metrics.LblAddIndex
	case model.ActionModifyColumn:
		return metrics.LblModifyColumn
	case model.ActionReorganizePartition, model.ActionAlterTablePartitioning, model.ActionRemovePartitioning:
		return metrics.LblReorgPartition
	default:
		return ""
	}
}
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

package executor

import (
	"sort"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

func storageClassTransitionRows(sctx sessionctx.Context) [][]types.Datum {
	statuses := domain.GetDomain(sctx).DDL().StorageClassTransitionStatuses()
	sort.Slice(statuses, func(i, j int) bool {
		if !statuses[i].StartTime.Equal(statuses[j].StartTime) {
			return statuses[i].StartTime.Before(statuses[j].StartTime)
		}
		if statuses[i].TableID != statuses[j].TableID {
			return statuses[i].TableID < statuses[j].TableID
		}
		return statuses[i].PartitionID < statuses[j].PartitionID
	})

	checker := privilege.GetPrivilegeManager(sctx)
	rows := make([][]types.Datum, 0, len(statuses))
	for _, status := range statuses {
		if checker != nil && !checker.RequestVerification(
			sctx.GetSessionVars().ActiveRoles,
			strings.ToLower(status.TableSchema),
			strings.ToLower(status.TableName),
			"",
			mysql.AllPrivMask,
		) {
			continue
		}
		rows = append(rows, storageClassTransitionStatusToDatums(sctx.GetSessionVars().Location(), status))
	}
	return rows
}

func storageClassTransitionStatusToDatums(location *time.Location, status ddl.StorageClassTransitionStatus) []types.Datum {
	partitionName := any(nil)
	partitionID := any(nil)
	if status.PartitionID != 0 {
		partitionName = status.PartitionName
		partitionID = status.PartitionID
	}
	startTime := types.NewTime(types.FromGoTime(status.StartTime.In(location)), mysql.TypeDatetime, types.MaxFsp)
	totalReplicas := any(nil)
	completedReplicas := any(nil)
	progress := any(nil)
	lastUpdateTime := any(nil)
	if status.StatusValid {
		totalReplicas = status.TotalReplicas
		completedReplicas = status.CompletedReplicas
		if status.ProgressValid {
			progress = status.Progress
		}
		lastUpdateTime = types.NewTime(types.FromGoTime(status.LastUpdateTime.In(location)), mysql.TypeDatetime, types.MaxFsp)
	}
	return types.MakeDatums(
		status.TableSchema,
		status.TableName,
		status.TableID,
		partitionName,
		partitionID,
		status.Direction,
		totalReplicas,
		completedReplicas,
		progress,
		startTime,
		uint64(status.Duration/time.Second),
		lastUpdateTime,
	)
}

func (e *memtableRetriever) dataForStorageClassTransitions(sctx sessionctx.Context) {
	e.rows = storageClassTransitionRows(sctx)
	for _, row := range e.rows {
		e.recordMemoryConsume(row)
	}
}

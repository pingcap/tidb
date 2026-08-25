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
	transitions := domain.GetDomain(sctx).DDL().StorageClassTransitions()
	sort.Slice(transitions, func(i, j int) bool {
		if !transitions[i].StartTime.Equal(transitions[j].StartTime) {
			return transitions[i].StartTime.Before(transitions[j].StartTime)
		}
		if transitions[i].TableID != transitions[j].TableID {
			return transitions[i].TableID < transitions[j].TableID
		}
		return transitions[i].PartitionID < transitions[j].PartitionID
	})

	checker := privilege.GetPrivilegeManager(sctx)
	rows := make([][]types.Datum, 0, len(transitions))
	for _, transition := range transitions {
		if checker != nil && !checker.RequestVerification(
			sctx.GetSessionVars().ActiveRoles,
			strings.ToLower(transition.TableSchema),
			strings.ToLower(transition.TableName),
			"",
			mysql.AllPrivMask,
		) {
			continue
		}
		rows = append(rows, storageClassTransitionToDatums(sctx.GetSessionVars().Location(), transition))
	}
	return rows
}

func storageClassTransitionToDatums(location *time.Location, transition ddl.StorageClassTransition) []types.Datum {
	partitionName := any(nil)
	partitionID := any(nil)
	if transition.PartitionID != 0 {
		partitionName = transition.PartitionName
		partitionID = transition.PartitionID
	}
	startTime := types.NewTime(types.FromGoTime(transition.StartTime.In(location)), mysql.TypeDatetime, types.MaxFsp)
	totalReplicas := any(nil)
	completedReplicas := any(nil)
	progress := any(nil)
	lastUpdateTime := any(nil)
	if transition.StatusValid {
		totalReplicas = transition.TotalReplicas
		completedReplicas = transition.CompletedReplicas
		if transition.ProgressValid {
			progress = transition.Progress
		}
		lastUpdateTime = types.NewTime(types.FromGoTime(transition.LastUpdateTime.In(location)), mysql.TypeDatetime, types.MaxFsp)
	}
	return types.MakeDatums(
		transition.TableSchema,
		transition.TableName,
		transition.TableID,
		partitionName,
		partitionID,
		transition.Direction,
		totalReplicas,
		completedReplicas,
		progress,
		startTime,
		uint64(transition.Duration/time.Second),
		lastUpdateTime,
	)
}

func (e *memtableRetriever) dataForStorageClassTransitions(sctx sessionctx.Context) {
	e.rows = storageClassTransitionRows(sctx)
	for _, row := range e.rows {
		e.recordMemoryConsume(row)
	}
}

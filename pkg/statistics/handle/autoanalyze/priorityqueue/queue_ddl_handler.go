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

package priorityqueue

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// HandleDDLEvent handles DDL events for the priority queue.
func (pq *AnalysisPriorityQueue) HandleDDLEvent(_ context.Context, sctx sessionctx.Context, event *notifier.SchemaChangeEvent) (err error) {
	pq.syncFields.mu.Lock()
	defer pq.syncFields.mu.Unlock()
	// If the priority queue is not initialized, we should retry later.
	if !pq.syncFields.initialized {
		return notifier.ErrNotReadyRetryLater
	}

	defer func() {
		if err != nil {
			actionType := event.GetType().String()
			statslogutil.StatsLogger().Error(fmt.Sprintf("Failed to handle %s event", actionType),
				zap.Error(err),
				zap.String("event", event.String()),
			)
		}
	}()

	switch event.GetType() {
	case model.ActionAddIndex:
		err = pq.handleAddIndexEvent(sctx, event)
	case model.ActionTruncateTable:
		err = pq.handleTruncateTableEvent(sctx, event)
	case model.ActionDropTable:
		err = pq.handleDropTableEvent(sctx, event)
	case model.ActionTruncateTablePartition:
		err = pq.handleTruncateTablePartitionEvent(sctx, event)
	case model.ActionDropTablePartition:
		err = pq.handleDropTablePartitionEvent(sctx, event)
	case model.ActionExchangeTablePartition:
		err = pq.handleExchangeTablePartitionEvent(sctx, event)
	case model.ActionReorganizePartition:
		err = pq.handleReorganizePartitionEvent(sctx, event)
	case model.ActionAlterTablePartitioning:
		err = pq.handleAlterTablePartitioningEvent(sctx, event)
	case model.ActionRemovePartitioning:
		err = pq.handleRemovePartitioningEvent(sctx, event)
	case model.ActionDropSchema:
		err = pq.handleDropSchemaEvent(sctx, event)
	default:
		// Ignore other DDL events.
	}

	return err
}

// getAndDeleteJob tries to get a job from the priority queue and delete it if it exists.
func (pq *AnalysisPriorityQueue) getAndDeleteJob(tableID int64) error {
	job, ok, err := pq.syncFields.inner.getByKey(tableID)
	if err != nil {
		statslogutil.StatsLogger().Error(
			"Failed to get the job from priority queue",
			zap.Error(err),
			zap.Int64("tableID", tableID),
		)
		return errors.Trace(err)
	}
	if ok {
		err := pq.syncFields.inner.delete(job)
		if err != nil {
			statslogutil.StatsLogger().Error(
				"Failed to delete table from priority queue",
				zap.Error(err),
				zap.Int64("tableID", tableID),
				zap.String("job", job.String()),
			)
			return errors.Trace(err)
		}
	}
	return nil
}

// recreateAndPushJob is a helper function that recreates a job and pushes it to the queue.
func (pq *AnalysisPriorityQueue) recreateAndPushJob(
	sctx sessionctx.Context,
	lockedTables map[int64]struct{},
	pruneMode variable.PartitionPruneMode,
	stats *statistics.Table,
) error {
	parameters := exec.GetAutoAnalyzeParameters(sctx)
	autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[vardef.TiDBAutoAnalyzeRatio])
	currentTs, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	jobFactory := NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	job := pq.tryCreateJob(is, stats, pruneMode, jobFactory, lockedTables)
	return pq.pushWithoutLock(job)
}

// recreateAndPushJobForTable recreates a job for the given table and pushes it to the queue.
// For static partitioned tables, we need to recreate the job for each partition.
// So we need to call this function for each partition.
// For normal tables and dynamic partitioned tables, we only need to recreate the job for the whole table.
func (pq *AnalysisPriorityQueue) recreateAndPushJobForTable(sctx sessionctx.Context, tableInfo *model.TableInfo) error {
	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
	partitionInfo := tableInfo.GetPartitionInfo()
	lockedTables, err := lockstats.QueryLockedTables(statsutil.StatsCtx, sctx)
	if err != nil {
		return err
	}
	// For static partitioned tables, we need to recreate the job for each partition.
	if partitionInfo != nil && pruneMode == variable.Static {
		for _, def := range partitionInfo.Definitions {
			partitionStats := pq.statsHandle.GetPartitionStatsForAutoAnalyze(tableInfo, def.ID)
			err := pq.recreateAndPushJob(sctx, lockedTables, pruneMode, partitionStats)
			if err != nil {
				return err
			}
		}
		return nil
	}
	stats := pq.statsHandle.GetTableStatsForAutoAnalyze(tableInfo)
	return pq.recreateAndPushJob(sctx, lockedTables, pruneMode, stats)
}

func (pq *AnalysisPriorityQueue) handleAddIndexEvent(
	sctx sessionctx.Context,
	event *notifier.SchemaChangeEvent,
) error {
	tableInfo, idxes := event.GetAddIndexInfo()

	intest.AssertFunc(func() bool {
		// Vector index has a separate job type. We should not see vector index here.
		for _, idx := range idxes {
			if idx.VectorInfo != nil {
				return false
			}
		}
		return true
	})

	parameters := exec.GetAutoAnalyzeParameters(sctx)
	autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[vardef.TiDBAutoAnalyzeRatio])
	// Get current timestamp from the session context.
	currentTs, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	jobFactory := NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
	partitionInfo := tableInfo.GetPartitionInfo()
	lockedTables, err := lockstats.QueryLockedTables(statsutil.StatsCtx, sctx)
	if err != nil {
		return err
	}
	if pruneMode == variable.Static && partitionInfo != nil {
		// For static partitioned tables, we need to recreate the job for each partition.
		for _, def := range partitionInfo.Definitions {
			partitionID := def.ID
			partitionStats := pq.statsHandle.GetPartitionStatsForAutoAnalyze(tableInfo, partitionID)
			job := pq.tryCreateJob(is, partitionStats, pruneMode, jobFactory, lockedTables)
			return pq.pushWithoutLock(job)
		}
		return nil
	}

	// For normal tables and dynamic partitioned tables, we only need to recreate the job for the table.
	stats := pq.statsHandle.GetTableStatsForAutoAnalyze(tableInfo)
	// Directly create a new job for the newly added index.
	job := pq.tryCreateJob(is, stats, pruneMode, jobFactory, lockedTables)
	return pq.pushWithoutLock(job)
}

func (pq *AnalysisPriorityQueue) handleTruncateTableEvent(
	_ sessionctx.Context,
	event *notifier.SchemaChangeEvent,
) error {
	_, droppedTableInfo := event.GetTruncateTableInfo()

	// For non-partitioned tables or dynamic partitioned tables.
	err := pq.getAndDeleteJob(droppedTableInfo.ID)
	if err != nil {
		return err
	}

	// For static partitioned tables.
	partitionInfo := droppedTableInfo.GetPartitionInfo()
	if partitionInfo != nil {
		for _, def := range partitionInfo.Definitions {
			err := pq.getAndDeleteJob(def.ID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (pq *AnalysisPriorityQueue) handleDropTableEvent(
	_ sessionctx.Context,
	event *notifier.SchemaChangeEvent,
) error {
	droppedTableInfo := event.GetDropTableInfo()

	// For non-partitioned tables or dynamic partitioned tables.
	err := pq.getAndDeleteJob(droppedTableInfo.ID)
	if err != nil {
		return err
	}

	// For static partitioned tables.
	partitionInfo := droppedTableInfo.GetPartitionInfo()
	if partitionInfo != nil {
		for _, def := range partitionInfo.Definitions {
			err := pq.getAndDeleteJob(def.ID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (pq *AnalysisPriorityQueue) handleTruncateTablePartitionEvent(
	sctx sessionctx.Context,
	event *notifier.SchemaChangeEvent,
) error {
	globalTableInfo, _, droppedPartInfo := event.GetTruncatePartitionInfo()
	// For static partitioned tables.
	for _, def := range droppedPartInfo.Definitions {
		err := pq.getAndDeleteJob(def.ID)
		if err != nil {
			return err
		}
	}

	// For dynamic partitioned tables.
	err := pq.getAndDeleteJob(globalTableInfo.ID)
	if err != nil {
		return err
	}

	// Try to recreate the job for the partitioned table because it may contain other partitions.
	return pq.recreateAndPushJobForTable(sctx, globalTableInfo)
}

func (pq *AnalysisPriorityQueue) handleDropTablePartitionEvent(
	sctx sessionctx.Context,
	event *notifier.SchemaChangeEvent,
) error {
	globalTableInfo, droppedPartInfo := event.GetDropPartitionInfo()
	// For static partitioned tables.
	for _, def := range droppedPartInfo.Definitions {
		err := pq.getAndDeleteJob(def.ID)
		if err != nil {
			return err
		}
	}

	// For dynamic partitioned tables.
	err := pq.getAndDeleteJob(globalTableInfo.ID)
	if err != nil {
		return err
	}

	// Try to recreate the job for the partitioned table because it may contain other partitions.
	return pq.recreateAndPushJobForTable(sctx, globalTableInfo)
}

func (pq *AnalysisPriorityQueue) handleExchangeTablePartitionEvent(
	sctx sessionctx.Context,
	event *notifier.SchemaChangeEvent,
) error {
	globalTableInfo, partInfo, nonPartTableInfo := event.GetExchangePartitionInfo()

	// For static partitioned tables.
	err := pq.getAndDeleteJob(partInfo.Definitions[0].ID)
	if err != nil {
		return err
	}

	// For non-partitioned tables.
	err = pq.getAndDeleteJob(nonPartTableInfo.ID)
	if err != nil {
		return err
	}

	// For dynamic partitioned tables.
	err = pq.getAndDeleteJob(globalTableInfo.ID)
	if err != nil {
		return err
	}
	// Try to recreate the job for the partitioned table because it may contain other partitions.
	err = pq.recreateAndPushJobForTable(sctx, globalTableInfo)
	if err != nil {
		return err
	}

	// For non-partitioned tables.
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	// Get the new table info after the exchange.
	// Note: We should use the ID of the partitioned table to get the new table info after the exchange.
	tblInfo, ok := pq.statsHandle.TableInfoByID(is, partInfo.Definitions[0].ID)
	if ok {
		return pq.recreateAndPushJobForTable(sctx, tblInfo.Meta())
	}

	return nil
}

func (pq *AnalysisPriorityQueue) handleReorganizePartitionEvent(
	sctx sessionctx.Context,
	event *notifier.SchemaChangeEvent,
) error {
	globalTableInfo, _, droppedPartitionInfo := event.GetReorganizePartitionInfo()

	// For static partitioned tables.
	for _, def := range droppedPartitionInfo.Definitions {
		err := pq.getAndDeleteJob(def.ID)
		if err != nil {
			return err
		}
	}

	// For dynamic partitioned tables.
	err := pq.getAndDeleteJob(globalTableInfo.ID)
	if err != nil {
		return err
	}

	// Try to recreate the job for the partitioned table because the new partition has been added.
	// Currently, the stats meta for the reorganized partitions is not updated.
	// This might be improved in the future.
	return pq.recreateAndPushJobForTable(sctx, globalTableInfo)
}

func (pq *AnalysisPriorityQueue) handleAlterTablePartitioningEvent(sctx sessionctx.Context, event *notifier.SchemaChangeEvent) error {
	oldSingleTableID, newGlobalTableInfo, _ := event.GetAddPartitioningInfo()

	// For non-partitioned tables.
	err := pq.getAndDeleteJob(oldSingleTableID)
	if err != nil {
		return err
	}

	// For dynamic partitioned tables.
	err = pq.getAndDeleteJob(newGlobalTableInfo.ID)
	if err != nil {
		return err
	}

	// Try to recreate the job for the partitioned table because the new partition has been added.
	// Currently, the stats meta for the new partition is not updated.
	// This might be improved in the future.
	return pq.recreateAndPushJobForTable(sctx, newGlobalTableInfo)
}

func (pq *AnalysisPriorityQueue) handleRemovePartitioningEvent(sctx sessionctx.Context, event *notifier.SchemaChangeEvent) error {
	oldTblID, newSingleTableInfo, droppedPartInfo := event.GetRemovePartitioningInfo()

	// For static partitioned tables.
	for _, def := range droppedPartInfo.Definitions {
		err := pq.getAndDeleteJob(def.ID)
		if err != nil {
			return err
		}
	}

	// For dynamic partitioned tables.
	err := pq.getAndDeleteJob(oldTblID)
	if err != nil {
		return err
	}

	// Recreate the job for the new single table.
	// Currently, the stats meta for the new single table is not updated.
	// This might be improved in the future.
	return pq.recreateAndPushJobForTable(sctx, newSingleTableInfo)
}

func (pq *AnalysisPriorityQueue) handleDropSchemaEvent(_ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
	miniDBInfo := event.GetDropSchemaInfo()
	for _, tbl := range miniDBInfo.Tables {
		// For static partitioned tables.
		for _, partition := range tbl.Partitions {
			if err := pq.getAndDeleteJob(partition.ID); err != nil {
				// Try best to delete as many tables as possible.
				statslogutil.StatsLogger().Error(
					"Failed to delete table from priority queue",
					zap.Error(err),
					zap.String("db", miniDBInfo.Name.O),
					zap.Int64("tableID", tbl.ID),
					zap.String("tableName", tbl.Name.O),
					zap.Int64("partitionID", partition.ID),
					zap.String("partitionName", partition.Name.O),
				)
			}
		}
		// For non-partitioned tables or dynamic partitioned tables.
		if err := pq.getAndDeleteJob(tbl.ID); err != nil {
			// Try best to delete as many tables as possible.
			statslogutil.StatsLogger().Error(
				"Failed to delete table from priority queue",
				zap.Error(err),
				zap.String("db", miniDBInfo.Name.O),
				zap.Int64("tableID", tbl.ID),
				zap.String("tableName", tbl.Name.O),
			)
		}
	}
	return nil
}

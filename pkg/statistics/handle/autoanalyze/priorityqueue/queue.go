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
	"container/heap"
	"context"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

// PushJobFunc is a function that pushes an AnalysisJob to a queue.
type PushJobFunc func(job AnalysisJob) error

// FetchAllTablesAndBuildAnalysisJobs builds analysis jobs for all eligible tables and partitions.
func FetchAllTablesAndBuildAnalysisJobs(
	sctx sessionctx.Context,
	parameters map[string]string,
	statsHandle statstypes.StatsHandle,
	jobFunc PushJobFunc,
) error {
	autoAnalyzeRatio := exec.ParseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	pruneMode := variable.PartitionPruneMode(sctx.GetSessionVars().PartitionPruneMode.Load())
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	// Query locked tables once to minimize overhead.
	// Outdated lock info is acceptable as we verify table lock status pre-analysis.
	lockedTables, err := lockstats.QueryLockedTables(sctx)
	if err != nil {
		return err
	}
	// Get current timestamp from the session context.
	currentTs, err := getStartTs(sctx)
	if err != nil {
		return err
	}

	jobFactory := NewAnalysisJobFactory(sctx, autoAnalyzeRatio, currentTs)
	calculator := NewPriorityCalculator()

	dbs := is.AllSchemaNames()
	for _, db := range dbs {
		// Ignore the memory and system database.
		if util.IsMemOrSysDB(db.L) {
			continue
		}

		tbls, err := is.SchemaTableInfos(context.Background(), db)
		if err != nil {
			return err
		}

		// We need to check every partition of every table to see if it needs to be analyzed.
		for _, tblInfo := range tbls {
			// If table locked, skip analyze all partitions of the table.
			if _, ok := lockedTables[tblInfo.ID]; ok {
				continue
			}

			if tblInfo.IsView() {
				continue
			}

			pi := tblInfo.GetPartitionInfo()
			if pi == nil {
				job := jobFactory.CreateNonPartitionedTableAnalysisJob(
					db.O,
					tblInfo,
					statsHandle.GetTableStatsForAutoAnalyze(tblInfo),
				)
				err := setWeightAndPushJob(jobFunc, job, calculator)
				if err != nil {
					return err
				}
				continue
			}

			// Only analyze the partition that has not been locked.
			partitionDefs := make([]model.PartitionDefinition, 0, len(pi.Definitions))
			for _, def := range pi.Definitions {
				if _, ok := lockedTables[def.ID]; !ok {
					partitionDefs = append(partitionDefs, def)
				}
			}
			partitionStats := GetPartitionStats(statsHandle, tblInfo, partitionDefs)
			// If the prune mode is static, we need to analyze every partition as a separate table.
			if pruneMode == variable.Static {
				for pIDAndName, stats := range partitionStats {
					job := jobFactory.CreateStaticPartitionAnalysisJob(
						db.O,
						tblInfo,
						pIDAndName.ID,
						pIDAndName.Name,
						stats,
					)
					err := setWeightAndPushJob(jobFunc, job, calculator)
					if err != nil {
						return err
					}
				}
			} else {
				job := jobFactory.CreateDynamicPartitionedTableAnalysisJob(
					db.O,
					tblInfo,
					statsHandle.GetPartitionStatsForAutoAnalyze(tblInfo, tblInfo.ID),
					partitionStats,
				)
				err := setWeightAndPushJob(jobFunc, job, calculator)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func setWeightAndPushJob(pushFunc PushJobFunc, job AnalysisJob, calculator *PriorityCalculator) error {
	if job == nil {
		return nil
	}
	// We apply a penalty to larger tables, which can potentially result in a negative weight.
	// To prevent this, we filter out any negative weights. Under normal circumstances, table sizes should not be negative.
	weight := calculator.CalculateWeight(job)
	if weight <= 0 {
		statslogutil.SingletonStatsSamplerLogger().Warn(
			"Table gets a negative weight",
			zap.Float64("weight", weight),
			zap.Stringer("job", job),
		)
	}
	job.SetWeight(weight)
	// Push the job onto the queue.
	return pushFunc(job)
}

func getStartTs(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}

// AnalysisPriorityQueue is a priority queue for TableAnalysisJobs.
type AnalysisPriorityQueue struct {
	inner *AnalysisInnerQueue
}

// NewAnalysisPriorityQueue creates a new AnalysisPriorityQueue.
func NewAnalysisPriorityQueue() *AnalysisPriorityQueue {
	q := &AnalysisPriorityQueue{
		inner: &AnalysisInnerQueue{},
	}
	heap.Init(q.inner)
	return q
}

// Push adds a job to the priority queue with the given weight.
func (apq *AnalysisPriorityQueue) Push(job AnalysisJob) error {
	heap.Push(apq.inner, job)
	return nil
}

// Pop removes the highest priority job from the queue.
func (apq *AnalysisPriorityQueue) Pop() AnalysisJob {
	return heap.Pop(apq.inner).(AnalysisJob)
}

// Len returns the number of jobs in the queue.
func (apq *AnalysisPriorityQueue) Len() int {
	return apq.inner.Len()
}

// An AnalysisInnerQueue implements heap.Interface and holds TableAnalysisJobs.
// Exported for testing purposes. You should not use this directly.
type AnalysisInnerQueue []AnalysisJob

// Implement the sort.Interface methods for the priority queue.

func (aq AnalysisInnerQueue) Len() int { return len(aq) }
func (aq AnalysisInnerQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority, so we use greater than here.
	return aq[i].GetWeight() > aq[j].GetWeight()
}
func (aq AnalysisInnerQueue) Swap(i, j int) {
	aq[i], aq[j] = aq[j], aq[i]
}

// Push adds an item to the priority queue.
func (aq *AnalysisInnerQueue) Push(x any) {
	item := x.(AnalysisJob)
	*aq = append(*aq, item)
}

// Pop removes the highest priority item from the queue.
func (aq *AnalysisInnerQueue) Pop() any {
	old := *aq
	n := len(old)
	item := old[n-1]
	*aq = old[0 : n-1]
	return item
}

// Copyright 2025 PingCAP, Inc.
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
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type mergeTempIndexExecutor struct {
	taskexecutor.BaseStepExecutor
	job           *model.Job
	store         kv.Storage
	parentTable   table.PhysicalTable // The parent table for partition table.
	physicalTable table.PhysicalTable // The physical partition to merge temp index.
	batchCnt      int

	mergeCounter    prometheus.Counter
	conflictCounter prometheus.Counter
	idxInfo         *model.IndexInfo

	*execute.SubtaskSummary
	totalRows int64
	buffers   *tempIdxBuffers
}

func newMergeTempIndexExecutor(job *model.Job, store kv.Storage, tbl table.PhysicalTable) (*mergeTempIndexExecutor, error) {
	batchCnt := job.ReorgMeta.GetBatchSize()
	return &mergeTempIndexExecutor{
		job:            job,
		store:          store,
		batchCnt:       batchCnt,
		parentTable:    tbl,
		SubtaskSummary: &execute.SubtaskSummary{},
		buffers:        newTempIdxBuffers(batchCnt),
	}, nil
}

func (*mergeTempIndexExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge temp index executor init subtask exec env")
	return nil
}

func (e *mergeTempIndexExecutor) initializeByMeta(subtask *proto.Subtask, meta *BackfillSubTaskMeta) error {
	physicalID := tablecodec.DecodeTableID(meta.StartKey)
	idxInfo, err := findIndexInfoByDecodingKey(e.parentTable.Indices(), meta.StartKey)
	if err != nil {
		return errors.Trace(err)
	}
	e.idxInfo = idxInfo
	if e.parentTable.GetPartitionedTable() != nil && !idxInfo.Global {
		e.physicalTable = e.parentTable.GetPartitionedTable().GetPartition(physicalID)
		if e.physicalTable == nil {
			return errors.Errorf("partitioned table %d not found for index %s", physicalID, idxInfo.Name.L)
		}
	} else {
		e.physicalTable = e.parentTable
	}
	logutil.BgLogger().Info("initialize merge temp index executor by meta",
		zap.Int64("jobID", e.job.ID),
		zap.Int64("subtaskID", subtask.ID),
		zap.String("index", e.idxInfo.Name.O),
		zap.Int64("physicalID", e.physicalTable.GetPhysicalID()),
		zap.Int64("taskPhysicalID", meta.PhysicalTableID),
		zap.Bool("isPartition", e.parentTable.GetPartitionedTable() != nil),
		zap.Bool("isGlobal", e.idxInfo.Global),
	)

	// init metrics
	idxName := getChangingIndexOriginName(e.idxInfo)
	e.mergeCounter = metrics.GetBackfillTotalByLabel(e.job.ID,
		metrics.LblMergeTmpIdxRate,
		e.job.SchemaName, e.physicalTable.Meta().Name.String(), idxName)
	e.conflictCounter = metrics.GetBackfillTotalByLabel(e.job.ID,
		fmt.Sprintf("%s-conflict", metrics.LblMergeTmpIdxRate),
		e.job.SchemaName, e.physicalTable.Meta().Name.String(), idxName)
	return nil
}

func (e *mergeTempIndexExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge temp index executor run subtask")

	var meta BackfillSubTaskMeta
	err := json.Unmarshal(subtask.Meta, &meta)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.initializeByMeta(subtask, &meta)
	if err != nil {
		return errors.Trace(err)
	}

	wctx := workerpool.NewContext(ctx)
	collector := &mergeTempIndexCollector{}

	srcOp := NewTempIndexScanTaskSource(wctx, e.store, e.physicalTable, meta.StartKey, meta.EndKey)
	mergeOp := NewMergeTempIndexOperator(wctx, e.store, e.physicalTable, e.idxInfo, e.job.ID, subtask.Concurrency, e.batchCnt, e.job.ReorgMeta)
	sinkOp := newTempIndexResultSink(wctx, e.physicalTable, collector)

	operator.Compose(srcOp, mergeOp)
	operator.Compose(mergeOp, sinkOp)

	pipe := operator.NewAsyncPipeline(srcOp, mergeOp, sinkOp)
	err = pipe.Execute()
	if err != nil {
		return err
	}
	err = pipe.Close()
	if opErr := wctx.OperatorErr(); opErr != nil {
		return opErr
	}
	e.mergeCounter.Add(float64(collector.addCount))
	e.RowCnt.Add(int64(collector.addCount))
	e.totalRows += int64(collector.scanCount)
	logutil.Logger(ctx).Info("merge temp index executor finish subtask", zap.Int("added", collector.addCount), zap.Int("scanned", collector.scanCount))
	return err
}

type mergeTempIndexCollector struct {
	execute.NoopCollector
	addCount  int
	scanCount int
}

func (m *mergeTempIndexCollector) Processed(_, rows int64) {
	m.addCount += int(rows)
	m.scanCount += int(rows)
}

func (e *mergeTempIndexExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return e.SubtaskSummary
}

func (e *mergeTempIndexExecutor) ResetSummary() {
	e.SubtaskSummary.Reset()
}

func (e *mergeTempIndexExecutor) Cleanup(ctx context.Context) error {
	metrics.CleanupAllMetricsForJob(e.job.ID)
	logutil.Logger(ctx).Info("merge temp index executor clean up subtask env", zap.Int64("rows", e.totalRows))
	return nil
}

// TaskMetaModified changes the concurrency for merging temp index.
func (*mergeTempIndexExecutor) TaskMetaModified(_ context.Context, _ []byte) error {
	// Will be added in the future PR
	return nil
}

// ResourceModified change the concurrency for merging temp index.
func (*mergeTempIndexExecutor) ResourceModified(_ context.Context, _ *proto.StepResource) error {
	// Will be added in the future PR
	return nil
}

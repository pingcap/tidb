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
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type readIndexToLocalStage struct {
	d     *ddl
	job   *model.Job
	index *model.IndexInfo
	ptbl  table.PhysicalTable
	jc    *JobContext

	bc      ingest.BackendCtx
	summary *scheduler.Summary
}

func newReadIndexToLocalStage(
	d *ddl,
	job *model.Job,
	index *model.IndexInfo,
	ptbl table.PhysicalTable,
	jc *JobContext,
	bc ingest.BackendCtx,
	summary *scheduler.Summary,
) *readIndexToLocalStage {
	return &readIndexToLocalStage{
		d:       d,
		job:     job,
		index:   index,
		ptbl:    ptbl,
		jc:      jc,
		bc:      bc,
		summary: summary,
	}
}

func (*readIndexToLocalStage) InitSubtaskExecEnv(_ context.Context) error {
	logutil.BgLogger().Info("read index stage init subtask exec env",
		zap.String("category", "ddl"))
	return nil
}

func (r *readIndexToLocalStage) SplitSubtask(ctx context.Context, subtask *proto.Subtask) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("read index stage run subtask",
		zap.String("category", "ddl"))

	d := r.d
	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error",
			zap.String("category", "ddl"),
			zap.Error(err))
		return nil, err
	}

	var startKey, endKey kv.Key
	var tbl table.PhysicalTable

	currentVer, err1 := getValidCurrentVersion(d.store)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}

	if parTbl, ok := r.ptbl.(table.PartitionedTable); ok {
		pid := sm.PhysicalTableID
		startKey, endKey, err = getTableRange(r.jc, d.ddlCtx, parTbl.GetPartition(pid), currentVer.Ver, r.job.Priority)
		if err != nil {
			logutil.BgLogger().Error("get table range error",
				zap.String("category", "ddl"),
				zap.Error(err))
			return nil, err
		}
		tbl = parTbl.GetPartition(pid)
	} else {
		startKey, endKey = sm.StartKey, sm.EndKey
		tbl = r.ptbl
	}

	ei, err := r.bc.Register(r.job.ID, r.index.ID, r.job.SchemaName, r.job.TableName)
	if err != nil {
		logutil.Logger(ctx).Warn("cannot register new engine", zap.Error(err),
			zap.Int64("job ID", r.job.ID), zap.Int64("index ID", r.index.ID))
		return nil, err
	}

	sessCtx, err := newSessCtx(d.store, r.job.ReorgMeta.SQLMode, r.job.ReorgMeta.Location)
	if err != nil {
		return nil, err
	}

	totalRowCount := &atomic.Int64{}
	counter := metrics.BackfillTotalCounter.WithLabelValues(
		metrics.GenerateReorgLabel("add_idx_rate", r.job.SchemaName, tbl.Meta().Name.O))

	pipe, err := NewAddIndexIngestPipeline(
		ctx, d.store, d.sessPool, r.bc, ei, sessCtx, tbl, r.index, startKey, endKey, totalRowCount, counter)
	if err != nil {
		return nil, err
	}
	err = pipe.Execute()
	if err != nil {
		return nil, err
	}
	err = pipe.Close()
	if err != nil {
		return nil, err
	}

	r.summary.UpdateRowCount(subtask.ID, totalRowCount.Load())
	return nil, nil
}

func (r *readIndexToLocalStage) CleanupSubtaskExecEnv(_ context.Context) error {
	logutil.BgLogger().Info("read index stage cleanup subtask exec env",
		zap.String("category", "ddl"))
	if _, ok := r.ptbl.(table.PartitionedTable); ok {
		ingest.LitBackCtxMgr.Unregister(r.job.ID)
	}
	return nil
}

// MockDMLExecutionAddIndexSubTaskFinish is used to mock DML execution during distributed add index.
var MockDMLExecutionAddIndexSubTaskFinish func()

func (*readIndexToLocalStage) OnSubtaskFinished(_ context.Context, subtask []byte) ([]byte, error) {
	failpoint.Inject("mockDMLExecutionAddIndexSubTaskFinish", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecutionAddIndexSubTaskFinish != nil {
			MockDMLExecutionAddIndexSubTaskFinish()
		}
	})
	return subtask, nil
}

func (r *readIndexToLocalStage) Rollback(_ context.Context) error {
	logutil.BgLogger().Info("read index stage rollback backfill add index task",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))
	ingest.LitBackCtxMgr.Unregister(r.job.ID)
	return nil
}

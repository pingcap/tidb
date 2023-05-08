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
	"encoding/hex"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type backfillSchedulerHandle struct {
	d           *ddl
	db          *model.DBInfo
	index       *model.IndexInfo
	job         *model.Job
	bc          ingest.BackendCtx
	ptbl        table.PhysicalTable
	jc          *JobContext
	eleTypeKey  []byte
	totalRowCnt int64
}

// BackfillGlobalMeta is the global task meta for backfilling index.
type BackfillGlobalMeta struct {
	Job        model.Job `json:"job"`
	EleID      int64     `json:"ele_id"`
	EleTypeKey []byte    `json:"ele_type_key"`
}

// BackfillSubTaskMeta is the sub-task meta for backfilling index.
type BackfillSubTaskMeta struct {
	PhysicalTableID int64 `json:"physical_table_id"`
}

// BackfillMinimalTask is the minimal-task for backfilling index.
type BackfillMinimalTask struct {
}

// IsMinimalTask implements the MinimalTask interface.
func (b *BackfillMinimalTask) IsMinimalTask() {
}

// NewBackfillSchedulerHandle creates a new backfill scheduler.
func NewBackfillSchedulerHandle(taskMeta []byte, d *ddl) (scheduler.Scheduler, error) {
	bh := &backfillSchedulerHandle{d: d}

	bgm := &BackfillGlobalMeta{}
	err := json.Unmarshal(taskMeta, bgm)
	if err != nil {
		return nil, err
	}

	bh.eleTypeKey = bgm.EleTypeKey
	jobMeta := &bgm.Job
	bh.job = jobMeta

	db, tbl, err := d.getTableByTxn(d.store, jobMeta.SchemaID, jobMeta.TableID)
	if err != nil {
		return nil, err
	}
	bh.db = db

	physicalTable := tbl.(table.PhysicalTable)
	bh.ptbl = physicalTable

	d.setDDLLabelForTopSQL(jobMeta.ID, jobMeta.Query)
	d.setDDLSourceForDiagnosis(jobMeta.ID, jobMeta.Type)
	jobCtx := d.jobContext(jobMeta.ID)
	bh.jc = jobCtx

	// Build reader.
	indexInfo := model.FindIndexInfoByID(tbl.Meta().Indices, bgm.EleID)
	if indexInfo == nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender",
			zap.Int64("table ID", tbl.Meta().ID), zap.Int64("index ID", bgm.EleID))
		return nil, errors.New("cannot find index info")
	}
	bh.index = indexInfo

	return bh, nil
}

// InitSubtaskExecEnv implements the Scheduler interface.
func (b *backfillSchedulerHandle) InitSubtaskExecEnv(context.Context) error {
	logutil.BgLogger().Info("[ddl] lightning init subtask exec env")
	d := b.d

	bc, err := ingest.LitBackCtxMgr.Register(d.ctx, b.index.Unique, b.job.ID)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] lightning register error", zap.Error(err))
		return err
	}
	b.bc = bc
	return nil
}

// SplitSubtask implements the Scheduler interface.
func (b *backfillSchedulerHandle) SplitSubtask(_ context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("[ddl] lightning split subtask")

	fnCtx, fnCancel := context.WithCancel(context.Background())
	defer fnCancel()

	go func() {
		select {
		case <-ctx.Done():
			b.d.notifyReorgCancel(b.job)
		case <-fnCtx.Done():
		}
	}()

	d := b.d
	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask, sm)
	if err != nil {
		logutil.BgLogger().Error("[ddl] unmarshal error", zap.Error(err))
		return nil, err
	}

	pid := sm.PhysicalTableID
	parTbl := b.ptbl.(table.PartitionedTable)

	currentVer, err1 := getValidCurrentVersion(d.store)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}
	startKey, endKey, err := getTableRange(b.jc, d.ddlCtx, parTbl.GetPartition(pid), currentVer.Ver, b.job.Priority)
	if err != nil {
		logutil.BgLogger().Error("[ddl] get table range error", zap.Error(err))
		return nil, err
	}

	mockReorgInfo := &reorgInfo{Job: b.job, d: d.ddlCtx}
	elements := make([]*meta.Element, 0)
	elements = append(elements, &meta.Element{ID: b.index.ID, TypeKey: meta.IndexElementKey})
	mockReorgInfo.elements = elements
	mockReorgInfo.currElement = mockReorgInfo.elements[0]

	ingestScheduler := newIngestBackfillScheduler(d.ctx, mockReorgInfo, d.sessPool, parTbl.GetPartition(pid), true)
	defer ingestScheduler.close(true)

	consumer := newResultConsumer(d.ddlCtx, mockReorgInfo, nil, true)
	consumer.run(ingestScheduler, startKey, &b.totalRowCnt)

	err = ingestScheduler.setupWorkers()
	if err != nil {
		logutil.BgLogger().Error("[ddl] setup workers error", zap.Error(err))
		return nil, err
	}

	taskIDAlloc := newTaskIDAllocator()
	for {
		kvRanges, err := splitTableRanges(b.ptbl, d.store, startKey, endKey, backfillTaskChanSize)
		if err != nil {
			return nil, err
		}
		if len(kvRanges) == 0 {
			break
		}

		logutil.BgLogger().Info("[ddl] start backfill workers to reorg record",
			zap.Int("workerCnt", ingestScheduler.currentWorkerSize()),
			zap.Int("regionCnt", len(kvRanges)),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		sendTasks(ingestScheduler, consumer, parTbl.GetPartition(pid), kvRanges, mockReorgInfo, taskIDAlloc)
		if consumer.shouldAbort() {
			break
		}
		rangeEndKey := kvRanges[len(kvRanges)-1].EndKey
		startKey = rangeEndKey.Next()
		if startKey.Cmp(endKey) >= 0 {
			break
		}
	}
	ingestScheduler.close(false)

	_, _, err = b.bc.Flush(b.index.ID, true)
	if err != nil {
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = convertToKeyExistsErr(err, b.index, b.ptbl.Meta())
		}
		logutil.BgLogger().Error("[ddl] flush error", zap.Error(err))
		return nil, err
	}
	return nil, consumer.getResult()
}

// OnSubtaskFinished implements the Scheduler interface.
func (*backfillSchedulerHandle) OnSubtaskFinished(context.Context, []byte) error {
	return nil
}

// CleanupSubtaskExecEnv implements the Scheduler interface.
func (b *backfillSchedulerHandle) CleanupSubtaskExecEnv(context.Context) error {
	logutil.BgLogger().Info("[ddl] lightning cleanup subtask exec env")

	b.bc.Unregister(b.job.ID, b.index.ID)
	return nil
}

// Rollback implements the Scheduler interface.
func (b *backfillSchedulerHandle) Rollback(context.Context) error {
	return nil
}

// BackFillSubtaskExecutor is the executor for backfill subtask.
type BackFillSubtaskExecutor struct {
	Task proto.MinimalTask
}

// Run implements the Executor interface.
func (b *BackFillSubtaskExecutor) Run(ctx context.Context) error {
	return nil
}

// BackfillTaskType is the type of backfill task.
const BackfillTaskType = "backfill"

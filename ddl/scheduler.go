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
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/ddl/ingest"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// MockDMLExecutionAddIndexSubTaskFinish is used to mock DML execution during distributed add index.
var MockDMLExecutionAddIndexSubTaskFinish func()

type backfillSchedulerHandle struct {
	d             *ddl
	db            *model.DBInfo
	index         *model.IndexInfo
	job           *model.Job
	bc            ingest.BackendCtx
	ptbl          table.PhysicalTable
	jc            *JobContext
	eleTypeKey    []byte
	totalRowCnt   int64
	isPartition   bool
	stepForImport bool
	done          chan struct{}
	ctx           context.Context
}

// BackfillGlobalMeta is the global task meta for backfilling index.
type BackfillGlobalMeta struct {
	Job        model.Job `json:"job"`
	EleID      int64     `json:"ele_id"`
	EleTypeKey []byte    `json:"ele_type_key"`
}

// BackfillSubTaskMeta is the sub-task meta for backfilling index.
type BackfillSubTaskMeta struct {
	PhysicalTableID int64  `json:"physical_table_id"`
	StartKey        []byte `json:"start_key"`
	EndKey          []byte `json:"end_key"`
}

// NewBackfillSchedulerHandle creates a new backfill scheduler.
func NewBackfillSchedulerHandle(taskMeta []byte, d *ddl, stepForImport bool) (scheduler.Scheduler, error) {
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
	bh.isPartition = tbl.Meta().GetPartitionInfo() != nil
	bh.db = db

	physicalTable := tbl.(table.PhysicalTable)
	bh.ptbl = physicalTable

	indexInfo := model.FindIndexInfoByID(tbl.Meta().Indices, bgm.EleID)
	if indexInfo == nil {
		logutil.BgLogger().Warn("index info not found", zap.String("category", "ddl-ingest"),
			zap.Int64("table ID", tbl.Meta().ID), zap.Int64("index ID", bgm.EleID))
		return nil, errors.New("index info not found")
	}
	bh.index = indexInfo

	if stepForImport {
		bh.stepForImport = true
		return bh, nil
	}

	d.setDDLLabelForTopSQL(jobMeta.ID, jobMeta.Query)
	d.setDDLSourceForDiagnosis(jobMeta.ID, jobMeta.Type)
	jobCtx := d.jobContext(jobMeta.ID)
	bh.jc = jobCtx
	d.newReorgCtx(jobMeta.ID, 0)

	return bh, nil
}

// UpdateStatLoop updates the row count of adding index.
func (b *backfillSchedulerHandle) UpdateStatLoop() {
	tk := time.Tick(time.Second * 5)
	ser, err := infosync.GetServerInfo()
	if err != nil {
		logutil.BgLogger().Warn("get server info failed", zap.String("category", "ddl"), zap.Error(err))
		return
	}
	path := fmt.Sprintf("%s/%d/%s:%d", rowCountEtcdPath, b.job.ID, ser.IP, ser.Port)
	writeToEtcd := func() {
		err := ddlutil.PutKVToEtcd(context.TODO(), b.d.etcdCli, 3, path, strconv.Itoa(int(b.totalRowCnt)))
		if err != nil {
			logutil.BgLogger().Warn("update row count for distributed add index failed", zap.String("category", "ddl"), zap.Error(err))
		}
	}
	for {
		select {
		case <-b.done:
			writeToEtcd()
			return
		case <-tk:
			writeToEtcd()
		}
	}
}

// InitSubtaskExecEnv implements the Scheduler interface.
func (b *backfillSchedulerHandle) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("lightning init subtask exec env", zap.String("category", "ddl"))
	d := b.d

	dCtx := logutil.WithCategory(d.ctx, "ddl-ingest")
	bc, err := ingest.LitBackCtxMgr.Register(dCtx, b.index.Unique, b.job.ID, d.etcdCli)
	if err != nil {
		logutil.BgLogger().Warn("lightning register error", zap.String("category", "ddl"), zap.Error(err))
		return err
	}
	b.bc = bc
	if b.stepForImport {
		return b.doFlushAndHandleError(ingest.FlushModeForceGlobal)
	}
	b.ctx = ctx

	ser, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	path := fmt.Sprintf("distAddIndex/%d/%s:%d", b.job.ID, ser.IP, ser.Port)
	response, err := d.etcdCli.Get(ctx, path)
	if err != nil {
		return err
	}
	if len(response.Kvs) > 0 {
		cnt, err := strconv.Atoi(string(response.Kvs[0].Value))
		if err != nil {
			return err
		}
		b.totalRowCnt = int64(cnt)
	}

	b.done = make(chan struct{})
	go b.UpdateStatLoop()
	return nil
}

func (b *backfillSchedulerHandle) doFlushAndHandleError(mode ingest.FlushMode) error {
	_, _, err := b.bc.Flush(b.index.ID, mode)
	if err != nil {
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = convertToKeyExistsErr(err, b.index, b.ptbl.Meta())
		}
		logutil.BgLogger().Error("flush error", zap.String("category", "ddl"), zap.Error(err))
		return err
	}
	return nil
}

// SplitSubtask implements the Scheduler interface.
func (b *backfillSchedulerHandle) SplitSubtask(ctx context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("lightning split subtask", zap.String("category", "ddl"))

	if b.stepForImport {
		return nil, nil
	}

	fnCtx, fnCancel := context.WithCancel(context.Background())
	defer fnCancel()

	go func() {
		select {
		case <-ctx.Done():
			b.d.notifyReorgWorkerJobStateChange(b.job)
		case <-fnCtx.Done():
		}
	}()

	d := b.d
	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask, sm)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error", zap.String("category", "ddl"), zap.Error(err))
		return nil, err
	}

	var startKey, endKey kv.Key
	var tbl table.PhysicalTable

	currentVer, err1 := getValidCurrentVersion(d.store)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}

	if !b.isPartition {
		startKey, endKey = sm.StartKey, sm.EndKey
		tbl = b.ptbl
	} else {
		pid := sm.PhysicalTableID
		parTbl := b.ptbl.(table.PartitionedTable)
		startKey, endKey, err = getTableRange(b.jc, d.ddlCtx, parTbl.GetPartition(pid), currentVer.Ver, b.job.Priority)
		if err != nil {
			logutil.BgLogger().Error("get table range error", zap.String("category", "ddl"), zap.Error(err))
			return nil, err
		}
		tbl = parTbl.GetPartition(pid)
	}

	mockReorgInfo := &reorgInfo{Job: b.job, d: d.ddlCtx}
	elements := make([]*meta.Element, 0)
	elements = append(elements, &meta.Element{ID: b.index.ID, TypeKey: meta.IndexElementKey})
	mockReorgInfo.elements = elements
	mockReorgInfo.currElement = mockReorgInfo.elements[0]

	ctx = logutil.WithCategory(ctx, "ddl-ingest")
	ingestScheduler := newIngestBackfillScheduler(ctx, mockReorgInfo, d.sessPool, tbl, true)
	defer ingestScheduler.close(true)

	consumer := newResultConsumer(d.ddlCtx, mockReorgInfo, nil, true)
	consumer.run(ingestScheduler, startKey, &b.totalRowCnt)

	err = ingestScheduler.setupWorkers()
	if err != nil {
		logutil.BgLogger().Error("setup workers error", zap.String("category", "ddl"), zap.Error(err))
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

		logutil.BgLogger().Info("start backfill workers to reorg record", zap.String("category", "ddl"),
			zap.Int("workerCnt", ingestScheduler.currentWorkerSize()),
			zap.Int("regionCnt", len(kvRanges)),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		sendTasks(ingestScheduler, consumer, tbl, kvRanges, mockReorgInfo, taskIDAlloc)
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

	if err := consumer.getResult(); err != nil {
		return nil, err
	}

	if b.isPartition {
		return nil, b.doFlushAndHandleError(ingest.FlushModeForceGlobal)
	}
	return nil, b.doFlushAndHandleError(ingest.FlushModeForceLocalAndCheckDiskQuota)
}

// OnSubtaskFinished implements the Scheduler interface.
func (*backfillSchedulerHandle) OnSubtaskFinished(_ context.Context, meta []byte) ([]byte, error) {
	failpoint.Inject("mockDMLExecutionAddIndexSubTaskFinish", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecutionAddIndexSubTaskFinish != nil {
			MockDMLExecutionAddIndexSubTaskFinish()
		}
	})
	return meta, nil
}

// CleanupSubtaskExecEnv implements the Scheduler interface.
func (b *backfillSchedulerHandle) CleanupSubtaskExecEnv(context.Context) error {
	logutil.BgLogger().Info("lightning cleanup subtask exec env", zap.String("category", "ddl"))

	if b.isPartition || b.stepForImport {
		ingest.LitBackCtxMgr.Unregister(b.job.ID)
	}

	if !b.stepForImport {
		close(b.done)
		b.d.removeReorgCtx(b.job.ID)
	}
	return nil
}

// Rollback implements the Scheduler interface.
func (b *backfillSchedulerHandle) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback backfill add index task", zap.String("category", "ddl"), zap.Int64("jobID", b.job.ID))
	ingest.LitBackCtxMgr.Unregister(b.job.ID)
	if !b.d.OwnerManager().IsOwner() {
		// For owner, reorg ctx will be removed after the reorg job is done.
		b.d.removeReorgCtx(b.job.ID)
	}
	return nil
}

// BackfillTaskType is the type of backfill task.
const BackfillTaskType = "backfill"

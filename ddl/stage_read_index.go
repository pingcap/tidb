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
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
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

	bc ingest.BackendCtx

	done        chan struct{}
	totalRowCnt int64
}

func newReadIndexToLocalStage(
	d *ddl,
	job *model.Job,
	index *model.IndexInfo,
	ptbl table.PhysicalTable,
	jc *JobContext,
	bc ingest.BackendCtx,
) *readIndexToLocalStage {
	return &readIndexToLocalStage{
		d:           d,
		job:         job,
		index:       index,
		ptbl:        ptbl,
		jc:          jc,
		bc:          bc,
		done:        make(chan struct{}),
		totalRowCnt: 0,
	}
}

func (r *readIndexToLocalStage) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("read index stage init subtask exec env",
		zap.String("category", "ddl"))
	d := r.d

	ser, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	path := fmt.Sprintf("distAddIndex/%d/%s:%d", r.job.ID, ser.IP, ser.Port)
	response, err := d.etcdCli.Get(ctx, path)
	if err != nil {
		return err
	}
	if len(response.Kvs) > 0 {
		cnt, err := strconv.Atoi(string(response.Kvs[0].Value))
		if err != nil {
			return err
		}
		r.totalRowCnt = int64(cnt)
	}

	r.done = make(chan struct{})
	go r.UpdateStatLoop()
	return nil
}

func (r *readIndexToLocalStage) SplitSubtask(ctx context.Context, subtask []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("read index stage run subtask",
		zap.String("category", "ddl"))

	d := r.d
	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask, sm)
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

	bc, ok := ingest.LitBackCtxMgr.Load(r.job.ID)
	if !ok {
		logutil.Logger(ctx).Error(ingest.LitErrGetBackendFail, zap.Int64("job ID", r.job.ID))
		return nil, errors.Trace(errors.New("cannot get lightning backend"))
	}
	ei, err := bc.Register(r.job.ID, r.index.ID, r.job.SchemaName, r.job.TableName)
	if err != nil {
		logutil.Logger(ctx).Warn("cannot register new engine", zap.Error(err),
			zap.Int64("job ID", r.job.ID), zap.Int64("index ID", r.index.ID))
		return nil, err
	}

	sessCtx, err := newSessCtx(d.store, r.job.ReorgMeta.SQLMode, r.job.ReorgMeta.Location)
	if err != nil {
		return nil, err
	}

	pipe, err := NewAddIndexIngestPipeline(
		ctx, d.store, d.sessPool, ei, sessCtx, tbl, r.index, startKey, endKey)
	if err != nil {
		return nil, err
	}
	err = pipe.Execute()
	if err != nil {
		return nil, err
	}
	return nil, pipe.Close()
}

func (r *readIndexToLocalStage) CleanupSubtaskExecEnv(_ context.Context) error {
	logutil.BgLogger().Info("read index stage cleanup subtask exec env",
		zap.String("category", "ddl"))
	if _, ok := r.ptbl.(table.PartitionedTable); ok {
		ingest.LitBackCtxMgr.Unregister(r.job.ID)
	}
	close(r.done)
	if !r.d.OwnerManager().IsOwner() {
		// For owner, reorg ctx will be removed after the reorg job is done.
		r.d.removeReorgCtx(r.job.ID)
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
	if !r.d.OwnerManager().IsOwner() {
		// For owner, reorg ctx will be removed after the reorg job is done.
		r.d.removeReorgCtx(r.job.ID)
	}
	return nil
}

// UpdateStatLoop updates the row count of adding index.
func (r *readIndexToLocalStage) UpdateStatLoop() {
	tk := time.Tick(time.Second * 5)
	ser, err := infosync.GetServerInfo()
	if err != nil {
		logutil.BgLogger().Warn("get server info failed",
			zap.String("category", "ddl"), zap.Error(err))
		return
	}
	path := fmt.Sprintf("%s/%d/%s:%d", rowCountEtcdPath, r.job.ID, ser.IP, ser.Port)
	writeToEtcd := func() {
		err := ddlutil.PutKVToEtcd(context.TODO(), r.d.etcdCli, 3, path, strconv.Itoa(int(r.totalRowCnt)))
		if err != nil {
			logutil.BgLogger().Warn("update row count for distributed add index failed",
				zap.String("category", "ddl"),
				zap.Error(err))
		}
	}
	for {
		select {
		case <-r.done:
			writeToEtcd()
			return
		case <-tk:
			writeToEtcd()
		}
	}
}

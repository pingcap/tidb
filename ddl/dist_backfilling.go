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
	"encoding/hex"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/resourcemanager/pooltask"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/gpool/spmc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type backfillWorkerContext struct {
	currID          int
	mu              sync.Mutex
	sessCtxs        []sessionctx.Context
	backfillWorkers []*backfillWorker
}

type newBackfillerFunc func(bfCtx *backfillCtx) (bf backfiller, err error)

func newBackfillWorkerContext(d *ddl, schemaName string, tbl table.Table, workerCnt int, bfMeta *model.BackfillMeta,
	bfFunc newBackfillerFunc) (*backfillWorkerContext, error) {
	if workerCnt <= 0 {
		return nil, nil
	}

	bCtxs, err := d.backfillCtxPool.batchGet(workerCnt)
	if err != nil || len(bCtxs) == 0 {
		logutil.BgLogger().Debug("[ddl] no backfill context available now", zap.Int("backfillCtx", len(bCtxs)), zap.Error(err))
		return nil, errors.Trace(err)
	}
	bwCtx := &backfillWorkerContext{backfillWorkers: bCtxs, sessCtxs: make([]sessionctx.Context, 0, len(bCtxs))}
	defer func() {
		if err != nil {
			bwCtx.close(d)
		}
	}()

	for i := 0; i < len(bCtxs); i++ {
		var se sessionctx.Context
		se, err = d.sessPool.get()
		if err != nil {
			logutil.BgLogger().Error("[ddl] new backfill worker context, get a session failed", zap.Error(err))
			return nil, errors.Trace(err)
		}
		err = initSessCtx(se, bfMeta.SQLMode, bfMeta.Location)
		if err != nil {
			logutil.BgLogger().Error("[ddl] new backfill worker context, init the session ctx failed", zap.Error(err))
			return nil, errors.Trace(err)
		}
		bfCtx := newBackfillCtx(d.ddlCtx, 0, se, bfMeta.ReorgTp, schemaName, tbl)
		var bf backfiller
		bf, err = bfFunc(bfCtx)
		if err != nil {
			logutil.BgLogger().Error("[ddl] new backfill worker context, do bfFunc failed", zap.Error(err))
			return nil, errors.Trace(err)
		}
		bwCtx.sessCtxs = append(bwCtx.sessCtxs, se)
		bCtxs[i].backfiller = bf
	}
	return bwCtx, nil
}

func (bwCtx *backfillWorkerContext) GetContext() *backfillWorker {
	bwCtx.mu.Lock()
	// TODO: Special considerations are required if the number of consumers we get from the backfillWorkerPool is increased.
	offset := bwCtx.currID % len(bwCtx.backfillWorkers)
	// To prevent different workers from using the same session.
	bw := bwCtx.backfillWorkers[offset]
	logutil.BgLogger().Info("[ddl] backfill worker get context", zap.Int("workerCount", len(bwCtx.backfillWorkers)),
		zap.Int("currID", bwCtx.currID), zap.Int("offset", offset), zap.Stringer("backfill worker", bw))
	bwCtx.currID++
	bwCtx.mu.Unlock()
	return bw
}

func runBackfillJobs(d *ddl, ingestBackendCtx *ingest.BackendContext, sess *session, bJob *BackfillJob, jobCtx *JobContext) (table.Table, error) {
	dbInfo, tbl, err := d.getTableByTxn(d.store, bJob.Meta.SchemaID, bJob.Meta.TableID)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] runBackfillJobs gets table failed", zap.String("bfJob", bJob.AbbrStr()), zap.Error(err))
		return nil, err
	}

	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	// TODO: Different worker using different newBackfillerFunc.
	workerCtx, err := newAddIndexWorkerContext(d, dbInfo.Name, tbl, workerCnt, bJob, jobCtx)
	if err != nil || workerCtx == nil {
		logutil.BgLogger().Info("[ddl] new adding index worker context failed", zap.Reflect("workerCtx", workerCtx), zap.Error(err))
		return nil, errors.Trace(err)
	}
	bwMgr := newBackfilWorkerManager(workerCtx)
	d.backfillWorkerPool.SetConsumerFunc(func(task *reorgBackfillTask, _ int, bfWorker *backfillWorker) *backfillResult {
		return bfWorker.runTask(task)
	})
	proFunc := func() ([]*reorgBackfillTask, error) {
		// TODO: After BackfillJob replaces reorgBackfillTask, use backfiller's GetTasks instead of it.
		return GetTasks(d.ddlCtx, sess, tbl, bJob.JobID, workerCnt+5)
	}
	// add new task
	resultCh, control := d.backfillWorkerPool.AddProduceBySlice(proFunc, 0, workerCtx, spmc.WithConcurrency(workerCnt))
	bwMgr.waitFinalResult(resultCh, ingestBackendCtx, workerCnt, bJob.EleID, control)

	// waiting task finishing
	control.Wait()
	err = bwMgr.close(d)

	return tbl, err
}

func (bwCtx *backfillWorkerContext) close(d *ddl) {
	for _, s := range bwCtx.sessCtxs {
		d.sessPool.put(s)
	}
	for _, w := range bwCtx.backfillWorkers {
		d.backfillCtxPool.put(w)
	}
}

type backfilWorkerManager struct {
	bwCtx     *backfillWorkerContext
	wg        util.WaitGroupWrapper
	unsyncErr error
	exitCh    chan struct{}
}

func newBackfilWorkerManager(bwCtx *backfillWorkerContext) *backfilWorkerManager {
	return &backfilWorkerManager{
		bwCtx:  bwCtx,
		exitCh: make(chan struct{}),
	}
}

func (bwm *backfilWorkerManager) waitFinalResult(resultCh <-chan *backfillResult, ingestBackendCtx *ingest.BackendContext, workerCnt int, eleID int64,
	tControl pooltask.TaskController[*reorgBackfillTask, *backfillResult, int, *backfillWorker, *backfillWorkerContext]) {
	bwm.wg.Run(func() {
		i := 0
		for {
			select {
			case result, ok := <-resultCh:
				if !ok {
					return
				}
				if result.err != nil {
					logutil.BgLogger().Warn("handle backfill task failed", zap.Error(result.err))
					bwm.unsyncErr = result.err
					tControl.Stop()
					return
				}

				if ingestBackendCtx != nil && i%workerCnt == 0 {
					err := ingestBackendCtx.Flush(eleID)
					if err != nil {
						bwm.unsyncErr = err
						return
					}
				}
				i++
			case <-bwm.exitCh:
				return
			}
		}
	})
}

func (bwm *backfilWorkerManager) close(d *ddl) error {
	close(bwm.exitCh)
	bwm.wg.Wait()

	bwm.bwCtx.close(d)

	return bwm.unsyncErr
}

// backfillJob2Task builds reorg task.
func (dc *ddlCtx) backfillJob2Task(t table.Table, bfJob *BackfillJob) (*reorgBackfillTask, error) {
	pt := t.(table.PhysicalTable)
	if tbl, ok := t.(table.PartitionedTable); ok {
		pt = tbl.GetPartition(bfJob.Meta.PhysicalTableID)
		if pt == nil {
			return nil, dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", bfJob.Meta.PhysicalTableID, t.Meta().ID)
		}
	}
	endKey := bfJob.EndKey
	// TODO: Check reorgInfo.mergingTmpIdx
	endK, err := getRangeEndKey(dc.jobContext(bfJob.JobID), dc.store, bfJob.Meta.Priority, pt.RecordPrefix(), bfJob.StartKey, endKey)
	if err != nil {
		logutil.BgLogger().Info("[ddl] convert backfill job to task, get reverse key failed", zap.String("backfill job", bfJob.AbbrStr()), zap.Error(err))
	} else {
		logutil.BgLogger().Info("[ddl] convert backfill job to task, change end key", zap.String("backfill job",
			bfJob.AbbrStr()), zap.String("current key", hex.EncodeToString(bfJob.StartKey)), zap.Bool("end include", bfJob.Meta.EndInclude),
			zap.String("end key", hex.EncodeToString(endKey)), zap.String("current end key", hex.EncodeToString(endK)))
		endKey = endK
	}

	return &reorgBackfillTask{
		bfJob:         bfJob,
		physicalTable: pt,
		// TODO: Remove these fields after remove the old logic.
		sqlQuery:   bfJob.Meta.Query,
		startKey:   bfJob.StartKey,
		endKey:     endKey,
		endInclude: bfJob.Meta.EndInclude,
		priority:   bfJob.Meta.Priority}, nil
}

// GetTasks gets the backfill tasks associated with the non-runningJobID.
func GetTasks(d *ddlCtx, sess *session, tbl table.Table, runningJobID int64, concurrency int) ([]*reorgBackfillTask, error) {
	// TODO: At present, only add index is processed. In the future, different elements need to be distinguished.
	var err error
	var bJobs []*BackfillJob
	for i := 0; i < retrySQLTimes; i++ {
		bJobs, err = GetAndMarkBackfillJobsForOneEle(sess, concurrency, runningJobID, d.uuid, InstanceLease)
		if err != nil {
			// TODO: add test: if all tidbs can't get the unmark backfill job(a tidb mark a backfill job, other tidbs returned, then the tidb can't handle this job.)
			if dbterror.ErrDDLJobNotFound.Equal(err) {
				logutil.BgLogger().Info("no backfill job, handle backfill task finished")
				return nil, err
			}
			if kv.ErrWriteConflict.Equal(err) {
				logutil.BgLogger().Info("GetAndMarkBackfillJobsForOneEle failed", zap.Error(err))
				time.Sleep(retrySQLInterval)
				continue
			}
		}

		tasks := make([]*reorgBackfillTask, 0, len(bJobs))
		for _, bJ := range bJobs {
			task, err := d.backfillJob2Task(tbl, bJ)
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, task)
		}
		return tasks, nil
	}

	return nil, err
}

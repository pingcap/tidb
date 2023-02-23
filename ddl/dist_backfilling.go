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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/resourcemanager/pooltask"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/gpool"
	"github.com/pingcap/tidb/util/gpool/spmc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	getJobWithoutPartition = -1
	backfillJobPrefixKey   = "%d_%s_%d_%%"

	// InstanceLease is the instance lease.
	InstanceLease                = 1 * time.Minute
	updateInstanceLease          = 25 * time.Second
	genTaskBatch                 = 4096
	genPhysicalTableTaskBatch    = 256
	minGenTaskBatch              = 1024
	minGenPhysicalTableTaskBatch = 64
	minDistTaskCnt               = 64
	retrySQLTimes                = 10
)

// RetrySQLInterval is export for test.
var RetrySQLInterval = 300 * time.Millisecond

func backfillJobPrefixKeyString(ddlJobID int64, eleKey kv.Key, eleID int64) string {
	return fmt.Sprintf(backfillJobPrefixKey, ddlJobID, hex.EncodeToString(eleKey), eleID)
}

// BackfillJob is for a tidb_background_subtask table's record.
type BackfillJob struct {
	ID              int64
	JobID           int64
	EleID           int64
	EleKey          []byte
	PhysicalTableID int64
	Tp              backfillerType
	State           model.JobState
	InstanceID      string
	InstanceLease   types.Time
	StartTS         uint64
	StateUpdateTS   uint64
	Meta            *model.BackfillMeta
}

// PrefixKeyString returns the BackfillJob's prefix key.
func (bj *BackfillJob) PrefixKeyString() string {
	return backfillJobPrefixKeyString(bj.JobID, bj.EleKey, bj.EleID)
}

func (bj *BackfillJob) keyString() string {
	return fmt.Sprintf("%d_%s_%d_%d", bj.JobID, hex.EncodeToString(bj.EleKey), bj.EleID, bj.ID)
}

// AbbrStr returns the BackfillJob's info without the Meta info.
func (bj *BackfillJob) AbbrStr() string {
	return fmt.Sprintf("ID:%d, JobID:%d, EleID:%d, Type:%s, State:%s, InstanceID:%s, InstanceLease:%s",
		bj.ID, bj.JobID, bj.EleID, bj.Tp, bj.State, bj.InstanceID, bj.InstanceLease)
}

// GetOracleTimeWithStartTS returns the current time with txn's startTS.
func GetOracleTimeWithStartTS(se *session) (time.Time, error) {
	txn, err := se.Txn(true)
	if err != nil {
		return time.Time{}, err
	}
	return oracle.GetTimeFromTS(txn.StartTS()).UTC(), nil
}

// GetOracleTime returns the current time from TS without txn.
func GetOracleTime(store kv.Storage) (time.Time, error) {
	currentVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	return oracle.GetTimeFromTS(currentVer.Ver).UTC(), nil
}

// GetLeaseGoTime returns a types.Time by adding a lease.
func GetLeaseGoTime(currTime time.Time, lease time.Duration) types.Time {
	leaseTime := currTime.Add(lease)
	return types.NewTime(types.FromGoTime(leaseTime.In(time.UTC)), mysql.TypeTimestamp, types.MaxFsp)
}

type backfillWorkerContext struct {
	currID          int
	mu              sync.Mutex
	sessCtxs        []sessionctx.Context
	backfillWorkers []*backfillWorker
}

type newBackfillerFunc func(bfCtx *backfillCtx) (bf backfiller, err error)

func newBackfillWorkerContext(d *ddl, schemaName string, tbl table.Table, workerCnt int, jobID int64, bfMeta *model.BackfillMeta,
	bfFunc newBackfillerFunc) (*backfillWorkerContext, error) {
	if workerCnt <= 0 {
		return nil, nil
	}

	bwCtx := &backfillWorkerContext{backfillWorkers: make([]*backfillWorker, 0, workerCnt), sessCtxs: make([]sessionctx.Context, 0, workerCnt)}
	var err error
	defer func() {
		if err != nil {
			bwCtx.close(d)
		}
	}()

	for i := 0; i < workerCnt; i++ {
		var se sessionctx.Context
		se, err = d.sessPool.get()
		if err != nil {
			logutil.BgLogger().Error("[ddl] new backfill worker context, get a session failed", zap.Int64("jobID", jobID), zap.Error(err))
			return nil, errors.Trace(err)
		}
		bwCtx.sessCtxs = append(bwCtx.sessCtxs, se)
		err = initSessCtx(se, bfMeta.SQLMode, bfMeta.Location)
		if err != nil {
			logutil.BgLogger().Error("[ddl] new backfill worker context, init the session ctx failed", zap.Int64("jobID", jobID), zap.Error(err))
			return nil, errors.Trace(err)
		}

		var bf backfiller
		bf, err = bfFunc(newBackfillCtx(d.ddlCtx, 0, se, bfMeta.ReorgTp, schemaName, tbl, true))
		if err != nil {
			if canSkipError(jobID, len(bwCtx.backfillWorkers), err) {
				err = nil
				continue
			}
			logutil.BgLogger().Error("[ddl] new backfill worker context, do bfFunc failed", zap.Int64("jobID", jobID), zap.Error(err))
			return nil, errors.Trace(err)
		}
		var bCtx *backfillWorker
		bCtx, err = d.backfillCtxPool.get()
		if err != nil || bCtx == nil {
			logutil.BgLogger().Info("[ddl] new backfill worker context, get backfill context failed", zap.Int64("jobID", jobID), zap.Error(err))
			err = nil
			break
		}
		bCtx.backfiller = bf
		bwCtx.backfillWorkers = append(bwCtx.backfillWorkers, bCtx)
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

func runBackfillJobs(d *ddl, sess *session, ingestBackendCtx *ingest.BackendContext, bJob *BackfillJob, jobCtx *JobContext) (table.Table, error) {
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
	workerCnt = len(workerCtx.backfillWorkers)
	bwMgr := newBackfilWorkerManager(workerCtx)
	d.backfillWorkerPool.SetConsumerFunc(func(task *reorgBackfillTask, _ int, bfWorker *backfillWorker) *backfillResult {
		return bfWorker.runTask(task)
	})

	runningPID := int64(0)
	// If txn-merge we needn't to claim the backfill job through the partition table
	if ingestBackendCtx == nil {
		runningPID = getJobWithoutPartition
	}
	proFunc := func() ([]*reorgBackfillTask, error) {
		// TODO: After BackfillJob replaces reorgBackfillTask, use backfiller's GetTasks instead of it.
		return GetTasks(d.ddlCtx, sess, tbl, bJob.JobID, &runningPID, workerCnt+5)
	}
	// add new task
	resultCh, control := d.backfillWorkerPool.AddProduceBySlice(proFunc, 0, workerCtx, spmc.WithConcurrency(workerCnt))
	bwMgr.waitFinalResult(resultCh, ingestBackendCtx, bJob.EleID, control)

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

func (bwm *backfilWorkerManager) waitFinalResult(resultCh <-chan *backfillResult, ingestBackendCtx *ingest.BackendContext, eleID int64,
	tControl pooltask.TaskController[*reorgBackfillTask, *backfillResult, int, *backfillWorker, *backfillWorkerContext]) {
	bwm.wg.Run(func() {
		i := 0
		workerCnt := len(bwm.bwCtx.backfillWorkers)

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
		pt = tbl.GetPartition(bfJob.PhysicalTableID)
		if pt == nil {
			return nil, dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", bfJob.PhysicalTableID, t.Meta().ID)
		}
	}
	return &reorgBackfillTask{
		bfJob:         bfJob,
		physicalTable: pt,
		// TODO: Remove these fields after remove the old logic.
		sqlQuery:   bfJob.Meta.Query,
		startKey:   bfJob.Meta.StartKey,
		endKey:     bfJob.Meta.EndKey,
		endInclude: bfJob.Meta.EndInclude,
		priority:   bfJob.Meta.Priority}, nil
}

// GetTasks gets the backfill tasks associated with the non-runningJobID.
func GetTasks(d *ddlCtx, sess *session, tbl table.Table, runningJobID int64, runningPID *int64, concurrency int) ([]*reorgBackfillTask, error) {
	// TODO: At present, only add index is processed. In the future, different elements need to be distinguished.
	var err error
	for i := 0; i < retrySQLTimes; i++ {
		bJobs, err := GetAndMarkBackfillJobsForOneEle(sess, concurrency, runningJobID, d.uuid, *runningPID, InstanceLease)
		if err != nil {
			// TODO: add test: if all tidbs can't get the unmark backfill job(a tidb mark a backfill job, other tidbs returned, then the tidb can't handle this job.)
			if dbterror.ErrDDLJobNotFound.Equal(err) {
				logutil.BgLogger().Info("no backfill job, handle backfill task finished")
				return nil, gpool.ErrProducerClosed
			}
			if kv.ErrWriteConflict.Equal(err) {
				logutil.BgLogger().Info("GetAndMarkBackfillJobsForOneEle failed", zap.Error(err))
				time.Sleep(RetrySQLInterval)
				continue
			}
		}

		if *runningPID != getJobWithoutPartition {
			*runningPID = bJobs[0].PhysicalTableID
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

// Copyright 2022 PingCAP, Inc.
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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	lit "github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	BackfillProgressPercent float64 = 0.6
)

// Whether Fast DDl is allowed.
func IsAllowFastDDL() bool {
	// Only when both TiDBFastDDL is set to on and Lightning env is inited successful,
	// the add index could choose lightning path to do backfill procedure.
	// ToDo: need check PiTR is off currently.
	if variable.FastDDL.Load() && lit.GlobalLightningEnv.IsInited {
		return true
	} else {
		return false
	}
}

// Check if PiTR is enable in cluster.
func isPiTREnable(w *worker) bool {
	var (
		ctx sessionctx.Context
		valStr string = "show config where name = 'log-backup.enable'"
		err error
		retVal bool = false
	)
	ctx, err = w.sessPool.get()
	if err != nil  {
		return true
	}
	defer w.sessPool.put(ctx)
	rows, fields, errSQL := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(context.Background(), nil, valStr)
	if errSQL != nil {
		return true 
	}
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		d := row.GetDatum(3, &fields[3].Column.FieldType)
		value, errField := d.ToString()
		if errField != nil {
			return true
		}
		if value == "true" {
			retVal = true
			break
		}
	}
	return retVal
}

func isLightningEnabled(id int64) bool {
	return lit.IsEngineLightningBackfill(id)
}

func setLightningEnabled(id int64, value bool) {
	lit.SetEnable(id, value)
}

func needRestoreJob(id int64) bool {
	return lit.NeedRestore(id)
}

func setNeedRestoreJob(id int64, value bool) {
	lit.SetNeedRestore(id, value)
}

func prepareBackend(ctx context.Context, unique bool, job *model.Job, sqlMode mysql.SQLMode) (err error) {
	bcKey := lit.GenBackendContextKey(job.ID)
	// Create and regist backend of lightning
	err = lit.GlobalLightningEnv.LitMemRoot.RegistBackendContext(ctx, unique, bcKey, sqlMode)
	if err != nil {
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
		return err
	}

	return err
}

func prepareLightningEngine(job *model.Job, indexId int64, workerCnt int) (wCnt int, err error) {
	bcKey := lit.GenBackendContextKey(job.ID)
	enginKey := lit.GenEngineInfoKey(job.ID, indexId)
	wCnt, err = lit.GlobalLightningEnv.LitMemRoot.RegistEngineInfo(job, bcKey, enginKey, int32(indexId), workerCnt)
	if err != nil {
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	}
	return wCnt, err
}

// Import local index sst file into TiKV.
func importIndexDataToStore(ctx context.Context, reorg *reorgInfo, indexId int64, unique bool, tbl table.Table) error {
	if isLightningEnabled(reorg.ID) && needRestoreJob(reorg.ID) {
		engineInfoKey := lit.GenEngineInfoKey(reorg.ID, indexId)
		// just log info.
		err := lit.FinishIndexOp(ctx, engineInfoKey, tbl, unique)
		if err != nil {
			err = errors.Trace(err)
			return err
		}
		// After import local data into TiKV, then the progress set to 100.
		metrics.GetBackfillProgressByLabel(metrics.LblAddIndex).Set(100)
	}
	return nil
}

// Used to clean backend,
func cleanUpLightningEnv(reorg *reorgInfo, isCanceled bool, indexIds ...int64) {
	if isLightningEnabled(reorg.ID) {
		bcKey := lit.GenBackendContextKey(reorg.ID)
		// If reorg is cancled, need do clean up engine.
		if isCanceled {
			lit.GlobalLightningEnv.LitMemRoot.ClearEngines(reorg.ID, indexIds...)
		}
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	}
}

// Disk quota checking and ingest data.
func importPartialDataToTiKV(jobId int64, indexIds int64) error {
	return lit.UnsafeImportEngineData(jobId, indexIds)
}

// Check if this reorg is a restore reorg task
// Check if current lightning reorg task can be executed continuely.
// Otherwise, restart the reorg task.
func canRestoreReorgTask(job *model.Job, indexId int64) bool {
	// The reorg just start, do nothing
	if job.SnapshotVer == 0 {
		return false
	}

	// Check if backend and engine are cached.
	if !lit.CanRestoreReorgTask(job.ID, indexId) {
		job.SnapshotVer = 0
		return false
	}
	return true
}

// Below is lightning worker implement
type addIndexWorkerLit struct {
	addIndexWorker

	// Lightning related variable.
	writerContex *lit.WorkerContext
}

func newAddIndexWorkerLit(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jobId int64) (*addIndexWorkerLit, error) {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap, sessCtx)
	// ToDo: Bear Currently, all the lightning worker share one openengine.
	engineInfoKey := lit.GenEngineInfoKey(jobId, indexInfo.ID)

	lwCtx, err := lit.GlobalLightningEnv.LitMemRoot.RegistWorkerContext(engineInfoKey, id)
	if err != nil {
		return nil, err
	}
	// Add build openengine process.
	return &addIndexWorkerLit{
		addIndexWorker: addIndexWorker{
			baseIndexWorker: baseIndexWorker{
				backfillWorker: newBackfillWorker(sessCtx, id, t, reorgInfo),
				indexes:        []table.Index{index},
				rowDecoder:     rowDecoder,
				defaultVals:    make([]types.Datum, len(t.WritableCols())),
				rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
				metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("add_idx_rate"),
				sqlMode:        reorgInfo.ReorgMeta.SQLMode,
			},
			index: index,
		},
		writerContex: lwCtx,
	}, err
}

// BackfillDataInTxn will backfill table index in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed,
// Note that index columns values may change, and an index is not allowed to be added, so the txn will rollback and retry.
// BackfillDataInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
func (w *addIndexWorkerLit) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	fetchTag := "AddIndexLightningFetchdata" + strconv.Itoa(w.id)
	writeTag := "AddIndexLightningWritedata" + strconv.Itoa(w.id)
	txnTag := "AddIndexLightningBackfillDataInTxn" + strconv.Itoa(w.id)

	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.reorgInfo.d.getResourceGroupTaggerForTopSQL(w.reorgInfo.Job); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		logSlowOperations(time.Since(oprStartTime), fetchTag, 1000)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		for _, idxRecord := range idxRecords {
			taskCtx.scanCount++

			// Create the index.
			key, idxVal, _, err := w.index.Create4SST(w.sessCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData, table.WithIgnoreAssertion)
			if err != nil {
				return errors.Trace(err)
			}
			// Currently, only use one kVCache, later may use multi kvCache to parallel compute/io performance.
			w.writerContex.WriteRow(key, idxVal)

			taskCtx.addedCount++
		}
		logSlowOperations(time.Since(oprStartTime), writeTag, 1000)
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), txnTag, 3000)
	return
}

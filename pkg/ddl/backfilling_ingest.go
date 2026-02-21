// Copyright 2020 PingCAP, Inc.
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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/operator"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)
func (dc *ddlCtx) addIndexWithLocalIngest(
	ctx context.Context,
	sessPool *sess.Pool,
	t table.PhysicalTable,
	reorgInfo *reorgInfo,
) error {
	if err := dc.isReorgRunnable(ctx, false); err != nil {
		return errors.Trace(err)
	}
	job := reorgInfo.Job
	wctx := NewLocalWorkerCtx(ctx, job.ID)
	defer wctx.Cancel()

	idxCnt := len(reorgInfo.elements)
	indexIDs := make([]int64, 0, idxCnt)
	indexInfos := make([]*model.IndexInfo, 0, idxCnt)
	var indexNames strings.Builder
	uniques := make([]bool, 0, idxCnt)
	hasUnique := false
	for _, e := range reorgInfo.elements {
		indexIDs = append(indexIDs, e.ID)
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, e.ID)
		if indexInfo == nil {
			logutil.DDLIngestLogger().Warn("index info not found",
				zap.Int64("jobID", job.ID),
				zap.Int64("tableID", t.Meta().ID),
				zap.Int64("indexID", e.ID))
			return errors.Errorf("index info not found: %d", e.ID)
		}
		indexInfos = append(indexInfos, indexInfo)
		if indexNames.Len() > 0 {
			indexNames.WriteString("+")
		}
		indexNames.WriteString(indexInfo.Name.O)
		uniques = append(uniques, indexInfo.Unique)
		hasUnique = hasUnique || indexInfo.Unique
	}

	var (
		cfg *local.BackendConfig
		bd  *local.Backend
		err error
	)
	if config.GetGlobalConfig().Store == config.StoreTypeTiKV {
		cfg, bd, err = ingest.CreateLocalBackend(ctx, dc.store, job, hasUnique, false, 0)
		if err != nil {
			return errors.Trace(err)
		}
		defer bd.Close()
	}
	bcCtx, err := ingest.NewBackendCtxBuilder(ctx, dc.store, job).
		WithCheckpointManagerParam(sessPool, reorgInfo.PhysicalTableID).
		Build(cfg, bd)
	if err != nil {
		return errors.Trace(err)
	}
	defer bcCtx.Close()

	reorgCtx := dc.getReorgCtx(job.ID)
	rowCntListener := &localRowCntCollector{
		prevPhysicalRowCnt: reorgCtx.getRowCount(),
		reorgCtx:           reorgCtx,
		counter:            metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, job.SchemaName, job.TableName, indexNames.String()),
	}

	sctx, err := sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer sessPool.Put(sctx)
	avgRowSize := estimateTableRowSize(ctx, dc.store, sctx.GetRestrictedSQLExecutor(), t)

	engines, err := bcCtx.Register(indexIDs, uniques, t)
	if err != nil {
		logutil.DDLIngestLogger().Error("cannot register new engine",
			zap.Int64("jobID", job.ID),
			zap.Error(err),
			zap.Int64s("index IDs", indexIDs))
		return errors.Trace(err)
	}
	importConc := job.ReorgMeta.GetConcurrency()
	pipe, err := NewAddIndexIngestPipeline(
		wctx,
		dc.store,
		sessPool,
		bcCtx,
		engines,
		job.ID,
		t,
		indexInfos,
		reorgInfo.StartKey,
		reorgInfo.EndKey,
		job.ReorgMeta,
		avgRowSize,
		importConc,
		rowCntListener,
	)
	if err != nil {
		return err
	}
	err = executeAndClosePipeline(wctx, pipe, reorgInfo, bcCtx, avgRowSize)
	if err != nil {
		err1 := bcCtx.FinishAndUnregisterEngines(ingest.OptCloseEngines)
		if err1 != nil {
			logutil.DDLIngestLogger().Error("unregister engine failed",
				zap.Int64("jobID", job.ID),
				zap.Error(err1),
				zap.Int64s("index IDs", indexIDs))
		}
		return err
	}
	return bcCtx.FinishAndUnregisterEngines(ingest.OptCleanData | ingest.OptCheckDup)
}

func adjustWorkerCntAndMaxWriteSpeed(ctx context.Context, pipe *operator.AsyncPipeline, bcCtx ingest.BackendCtx, avgRowSize int, reorgInfo *reorgInfo) {
	reader, writer := pipe.GetReaderAndWriter()
	if reader == nil || writer == nil {
		logutil.DDLIngestLogger().Error("failed to get local ingest mode reader or writer", zap.Int64("jobID", reorgInfo.ID))
		return
	}
	ticker := time.NewTicker(UpdateDDLJobReorgCfgInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			failpoint.InjectCall("onUpdateJobParam")
			reorgInfo.UpdateConfigFromSysTbl(ctx)
			maxWriteSpeed := reorgInfo.ReorgMeta.GetMaxWriteSpeed()
			if maxWriteSpeed != bcCtx.GetLocalBackend().GetWriteSpeedLimit() {
				bcCtx.GetLocalBackend().UpdateWriteSpeedLimit(maxWriteSpeed)
				logutil.DDLIngestLogger().Info("adjust ddl job config success",
					zap.Int64("jobID", reorgInfo.ID),
					zap.Int("max write speed", bcCtx.GetLocalBackend().GetWriteSpeedLimit()))
			}

			concurrency := reorgInfo.ReorgMeta.GetConcurrency()
			targetReaderCnt, targetWriterCnt := expectedIngestWorkerCnt(concurrency, avgRowSize, reorgInfo.ReorgMeta.UseCloudStorage)
			currentReaderCnt, currentWriterCnt := reader.GetWorkerPoolSize(), writer.GetWorkerPoolSize()
			if int32(targetReaderCnt) != currentReaderCnt || int32(targetWriterCnt) != currentWriterCnt {
				reader.TuneWorkerPoolSize(int32(targetReaderCnt), false)
				writer.TuneWorkerPoolSize(int32(targetWriterCnt), false)
				logutil.DDLIngestLogger().Info("adjust ddl job config success",
					zap.Int64("jobID", reorgInfo.ID),
					zap.Int32("table scan operator count", reader.GetWorkerPoolSize()),
					zap.Int32("index ingest operator count", writer.GetWorkerPoolSize()))
			}
			failpoint.InjectCall("checkReorgConcurrency", reorgInfo.Job)
		}
	}
}

func executeAndClosePipeline(ctx *workerpool.Context, pipe *operator.AsyncPipeline, reorgInfo *reorgInfo, bcCtx ingest.BackendCtx, avgRowSize int) error {
	err := pipe.Execute()
	if err != nil {
		return err
	}

	// Adjust worker pool size and max write speed dynamically.
	var wg util.WaitGroupWrapper
	adjustCtx, cancel := context.WithCancel(ctx)
	if reorgInfo != nil {
		wg.RunWithLog(func() {
			adjustWorkerCntAndMaxWriteSpeed(adjustCtx, pipe, bcCtx, avgRowSize, reorgInfo)
		})
	}

	err = pipe.Close()
	failpoint.InjectCall("afterPipeLineClose", pipe)
	cancel()
	wg.Wait() // wait for adjustWorkerCntAndMaxWriteSpeed to exit
	if opErr := ctx.OperatorErr(); opErr != nil {
		return opErr
	}
	return err
}

type localRowCntCollector struct {
	execute.NoopCollector
	reorgCtx *reorgCtx
	counter  prometheus.Counter

	// prevPhysicalRowCnt records the row count from previous physical tables (partitions).
	prevPhysicalRowCnt int64
	// curPhysicalRowCnt records the row count of current physical table.
	curPhysicalRowCnt struct {
		cnt int64
		mu  sync.Mutex
	}
}

func (s *localRowCntCollector) Processed(_, rowCnt int64) {
	s.curPhysicalRowCnt.mu.Lock()
	s.curPhysicalRowCnt.cnt += rowCnt
	s.reorgCtx.setRowCount(s.prevPhysicalRowCnt + s.curPhysicalRowCnt.cnt)
	s.curPhysicalRowCnt.mu.Unlock()
	s.counter.Add(float64(rowCnt))
}

func (s *localRowCntCollector) SetTotal(total int) {
	s.reorgCtx.setRowCount(s.prevPhysicalRowCnt + int64(total))
}

// UpdateDDLJobReorgCfgInterval is the interval to check and update reorg configuration.
var UpdateDDLJobReorgCfgInterval = 2 * time.Second


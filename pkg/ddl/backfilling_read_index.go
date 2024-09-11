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
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/table"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type readIndexExecutor struct {
	execute.StepExecFrameworkInfo
	d       *ddl
	job     *model.Job
	indexes []*model.IndexInfo
	ptbl    table.PhysicalTable
	jc      *ReorgContext

	avgRowSize      int
	cloudStorageURI string

	bc          ingest.BackendCtx
	curRowCount *atomic.Int64

	subtaskSummary sync.Map // subtaskID => readIndexSummary
}

type readIndexSummary struct {
	metaGroups []*external.SortedKVMeta
	mu         sync.Mutex
}

func newReadIndexExecutor(
	d *ddl,
	job *model.Job,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	jc *ReorgContext,
	bcGetter func() (ingest.BackendCtx, error),
	cloudStorageURI string,
	avgRowSize int,
) (*readIndexExecutor, error) {
	bc, err := bcGetter()
	if err != nil {
		return nil, err
	}
	return &readIndexExecutor{
		d:               d,
		job:             job,
		indexes:         indexes,
		ptbl:            ptbl,
		jc:              jc,
		bc:              bc,
		cloudStorageURI: cloudStorageURI,
		avgRowSize:      avgRowSize,
		curRowCount:     &atomic.Int64{},
	}, nil
}

func (*readIndexExecutor) Init(_ context.Context) error {
	logutil.DDLLogger().Info("read index executor init subtask exec env")
	return nil
}

func (r *readIndexExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.DDLLogger().Info("read index executor run subtask",
		zap.Bool("use cloud", len(r.cloudStorageURI) > 0))

	r.subtaskSummary.Store(subtask.ID, &readIndexSummary{
		metaGroups: make([]*external.SortedKVMeta, len(r.indexes)),
	})

	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}

	opCtx := NewDistTaskOperatorCtx(ctx, subtask.TaskID, subtask.ID)
	defer opCtx.Cancel()
	r.curRowCount.Store(0)

	if len(r.cloudStorageURI) > 0 {
		pipe, err := r.buildExternalStorePipeline(opCtx, subtask.ID, sm, subtask.Concurrency)
		if err != nil {
			return err
		}
		return executeAndClosePipeline(opCtx, pipe)
	}

	pipe, err := r.buildLocalStorePipeline(opCtx, sm, subtask.Concurrency)
	if err != nil {
		return err
	}
	err = executeAndClosePipeline(opCtx, pipe)
	if err != nil {
		// For dist task local based ingest, checkpoint is unsupported.
		// If there is an error we should keep local sort dir clean.
		err1 := r.bc.FinishAndUnregisterEngines(ingest.OptCleanData)
		if err1 != nil {
			logutil.DDLLogger().Warn("read index executor unregister engine failed", zap.Error(err1))
		}
		return err
	}
	return r.bc.FinishAndUnregisterEngines(ingest.OptCleanData | ingest.OptCheckDup)
}

func (r *readIndexExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return &execute.SubtaskSummary{
		RowCount: r.curRowCount.Load(),
	}
}

func (r *readIndexExecutor) Cleanup(ctx context.Context) error {
	tidblogutil.Logger(ctx).Info("read index executor cleanup subtask exec env")
	// cleanup backend context
	ingest.LitBackCtxMgr.Unregister(r.job.ID)
	return nil
}

func (r *readIndexExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	failpoint.InjectCall("mockDMLExecutionAddIndexSubTaskFinish")
	if len(r.cloudStorageURI) == 0 {
		return nil
	}
	// Rewrite the subtask meta to record statistics.
	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}
	sum, _ := r.subtaskSummary.LoadAndDelete(subtask.ID)
	s := sum.(*readIndexSummary)
	sm.MetaGroups = s.metaGroups
	sm.EleIDs = make([]int64, 0, len(r.indexes))
	for _, index := range r.indexes {
		sm.EleIDs = append(sm.EleIDs, index.ID)
	}

	all := external.SortedKVMeta{}
	for _, g := range s.metaGroups {
		all.Merge(g)
	}
	tidblogutil.Logger(ctx).Info("get key boundary on subtask finished",
		zap.String("start", hex.EncodeToString(all.StartKey)),
		zap.String("end", hex.EncodeToString(all.EndKey)),
		zap.Int("fileCount", len(all.MultipleFilesStats)),
		zap.Uint64("totalKVSize", all.TotalKVSize))

	meta, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	subtask.Meta = meta
	return nil
}

func (r *readIndexExecutor) getTableStartEndKey(sm *BackfillSubTaskMeta) (
	start, end kv.Key, tbl table.PhysicalTable, err error) {
	currentVer, err1 := getValidCurrentVersion(r.d.store)
	if err1 != nil {
		return nil, nil, nil, errors.Trace(err1)
	}
	if parTbl, ok := r.ptbl.(table.PartitionedTable); ok {
		pid := sm.PhysicalTableID
		start, end, err = getTableRange(r.jc, r.d.store, parTbl.GetPartition(pid), currentVer.Ver, r.job.Priority)
		if err != nil {
			logutil.DDLLogger().Error("get table range error",
				zap.Error(err))
			return nil, nil, nil, err
		}
		tbl = parTbl.GetPartition(pid)
	} else {
		start, end = sm.RowStart, sm.RowEnd
		tbl = r.ptbl
	}
	return start, end, tbl, nil
}

func (r *readIndexExecutor) buildLocalStorePipeline(
	opCtx *OperatorCtx,
	sm *BackfillSubTaskMeta,
	concurrency int,
) (*operator.AsyncPipeline, error) {
	start, end, tbl, err := r.getTableStartEndKey(sm)
	if err != nil {
		return nil, err
	}
	d := r.d
	indexIDs := make([]int64, 0, len(r.indexes))
	uniques := make([]bool, 0, len(r.indexes))
	for _, index := range r.indexes {
		indexIDs = append(indexIDs, index.ID)
		uniques = append(uniques, index.Unique)
	}
	engines, err := r.bc.Register(indexIDs, uniques, r.ptbl)
	if err != nil {
		tidblogutil.Logger(opCtx).Error("cannot register new engine",
			zap.Error(err),
			zap.Int64("job ID", r.job.ID),
			zap.Int64s("index IDs", indexIDs))
		return nil, err
	}
	rowCntListener := newDistTaskRowCntListener(r.curRowCount, r.job.SchemaName, tbl.Meta().Name.O)
	return NewAddIndexIngestPipeline(
		opCtx,
		d.store,
		d.sessPool,
		r.bc,
		engines,
		r.job.ID,
		tbl,
		r.indexes,
		start,
		end,
		r.job.ReorgMeta,
		r.avgRowSize,
		concurrency,
		nil,
		rowCntListener,
	)
}

func (r *readIndexExecutor) buildExternalStorePipeline(
	opCtx *OperatorCtx,
	subtaskID int64,
	sm *BackfillSubTaskMeta,
	concurrency int,
) (*operator.AsyncPipeline, error) {
	start, end, tbl, err := r.getTableStartEndKey(sm)
	if err != nil {
		return nil, err
	}

	d := r.d
	onClose := func(summary *external.WriterSummary) {
		sum, _ := r.subtaskSummary.Load(subtaskID)
		s := sum.(*readIndexSummary)
		s.mu.Lock()
		kvMeta := s.metaGroups[summary.GroupOffset]
		if kvMeta == nil {
			kvMeta = &external.SortedKVMeta{}
			s.metaGroups[summary.GroupOffset] = kvMeta
		}
		kvMeta.MergeSummary(summary)
		s.mu.Unlock()
	}
	rowCntListener := newDistTaskRowCntListener(r.curRowCount, r.job.SchemaName, tbl.Meta().Name.O)
	return NewWriteIndexToExternalStoragePipeline(
		opCtx,
		d.store,
		r.cloudStorageURI,
		r.d.sessPool,
		r.job.ID,
		subtaskID,
		tbl,
		r.indexes,
		start,
		end,
		onClose,
		r.job.ReorgMeta,
		r.avgRowSize,
		concurrency,
		r.GetResource(),
		rowCntListener,
	)
}

type distTaskRowCntListener struct {
	EmptyRowCntListener
	totalRowCount *atomic.Int64
	counter       prometheus.Counter
}

func newDistTaskRowCntListener(totalRowCnt *atomic.Int64, dbName, tblName string) *distTaskRowCntListener {
	counter := metrics.BackfillTotalCounter.WithLabelValues(
		metrics.GenerateReorgLabel("add_idx_rate", dbName, tblName))
	return &distTaskRowCntListener{
		totalRowCount: totalRowCnt,
		counter:       counter,
	}
}

func (d *distTaskRowCntListener) Written(rowCnt int) {
	d.totalRowCount.Add(int64(rowCnt))
	d.counter.Add(float64(rowCnt))
}

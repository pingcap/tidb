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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/metering"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	lightningmetric "github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

type cloudImportExecutor struct {
	taskexecutor.BaseStepExecutor
	job             *model.Job
	store           kv.Storage
	indexes         []*model.IndexInfo
	ptbl            table.PhysicalTable
	cloudStoreURI   string
	backendCtx      ingest.BackendCtx
	backend         *local.Backend
	taskConcurrency int
	metric          *lightningmetric.Common
	summary         *execute.SubtaskSummary
}

func newCloudImportExecutor(
	job *model.Job,
	store kv.Storage,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	cloudStoreURI string,
	taskConcurrency int,
) (*cloudImportExecutor, error) {
	return &cloudImportExecutor{
		job:             job,
		store:           store,
		indexes:         indexes,
		ptbl:            ptbl,
		cloudStoreURI:   cloudStoreURI,
		taskConcurrency: taskConcurrency,
		summary:         &execute.SubtaskSummary{},
	}, nil
}

func (e *cloudImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor init subtask exec env")
	e.metric = metrics.RegisterLightningCommonMetricsForDDL(e.job.ID)
	ctx = lightningmetric.WithCommonMetric(ctx, e.metric)
	cfg, bd, err := ingest.CreateLocalBackend(ctx, e.store, e.job, hasUniqueIndex(e.indexes), false, e.taskConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	bCtx, err := ingest.NewBackendCtxBuilder(ctx, e.store, e.job).Build(cfg, bd)
	if err != nil {
		bd.Close()
		return err
	}
	e.backend = bd
	e.backendCtx = bCtx
	for _, idx := range e.indexes {
		if idx.IsTiCIIndex() {
			taskID := strconv.FormatInt(e.job.ID, 10)
			if err := bd.InitTiCIWriterGroup(ctx, e.ptbl.Meta(), e.job.SchemaName, taskID); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func (e *cloudImportExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor run subtask")

	accessRec, objStore, err := handle.NewObjStoreWithRecording(ctx, e.cloudStoreURI)
	if err != nil {
		return err
	}
	meterRec := e.GetMeterRecorder()
	defer func() {
		objStore.Close()
		e.summary.MergeObjStoreRequests(&accessRec.Requests)
		meterRec.MergeObjStoreAccess(accessRec)
	}()

	sm, err := decodeBackfillSubTaskMeta(ctx, objStore, subtask.Meta)
	if err != nil {
		return err
	}
	localBackend := e.backendCtx.GetLocalBackend()
	if localBackend == nil {
		return errors.Errorf("local backend not found")
	}

	localBackend.SetCollector(&ingestCollector{
		meterRec: meterRec,
	})

	currentIdx, idxID, err := getIndexInfoAndID(sm.EleIDs, e.indexes)
	if err != nil {
		return err
	}

	_, engineUUID := backend.MakeUUID(e.ptbl.Meta().Name.L, idxID)
	engineID := int32(common.IndexEngineID)

	ticiHeaderCommitTS := uint64(0)
	if currentIdx != nil && currentIdx.GetColumnarIndexType() == model.ColumnarIndexTypeHybrid {
		ticiHeaderCommitTS = sm.ScanSnapshotTS
	}

	all := external.SortedKVMeta{}
	for _, g := range sm.MetaGroups {
		all.Merge(g)
	}

	// compatible with old version task meta
	jobKeys := sm.RangeJobKeys
	if jobKeys == nil {
		jobKeys = sm.RangeSplitKeys
	}
	err = localBackend.CloseEngine(ctx, &backend.EngineConfig{
		TiCIWriteEnabled:   currentIdx != nil && currentIdx.IsTiCIIndex(),
		TiCIHeaderCommitTS: ticiHeaderCommitTS,
		External: &backend.ExternalEngineConfig{
			ExtStore:      objStore,
			DataFiles:     sm.DataFiles,
			StatFiles:     sm.StatFiles,
			StartKey:      all.StartKey,
			EndKey:        all.EndKey,
			JobKeys:       jobKeys,
			SplitKeys:     sm.RangeSplitKeys,
			TotalFileSize: int64(all.TotalKVSize),
			TotalKVCount:  0,
			CheckHotspot:  true,
			MemCapacity:   e.GetResource().Mem.Capacity(),
			OnDup:         engineapi.OnDuplicateKeyError,
		},
		TS: sm.TS,
	}, engineUUID)
	if err != nil {
		return err
	}
	err = localBackend.ImportEngine(ctx, engineUUID, engineID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	failpoint.Inject("mockCloudImportRunSubtaskError", func(_ failpoint.Value) {
		err = context.DeadlineExceeded
	})
	if err == nil {
		return nil
	}

	if currentIdx != nil {
		return ingest.TryConvertToKeyExistsErr(err, currentIdx, e.ptbl.Meta())
	}

	// cannot fill the index name for subtask generated from an old version TiDB
	tErr, ok := errors.Cause(err).(*terror.Error)
	if !ok {
		return err
	}
	if tErr.ID() != common.ErrFoundDuplicateKeys.ID() {
		return err
	}
	return kv.ErrKeyExists
}

func hasUniqueIndex(idxs []*model.IndexInfo) bool {
	for _, idx := range idxs {
		if idx.Unique {
			return true
		}
	}
	return false
}

func (e *cloudImportExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor clean up subtask env")
	if e.backendCtx != nil {
		e.backendCtx.Close()
	}
	e.backend.Close()
	metrics.UnregisterLightningCommonMetricsForDDL(e.job.ID, e.metric)
	return nil
}

// RealtimeSummary returns the summary of the subtask execution.
func (e *cloudImportExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return e.summary
}

// ResetSummary resets the summary stored in the executor.
func (e *cloudImportExecutor) ResetSummary() {
	e.summary.Reset()
}

// TaskMetaModified changes the max write speed for ingest
func (*cloudImportExecutor) TaskMetaModified(context.Context, []byte) error {
	// Will be added in the future PR
	return nil
}

// ResourceModified change the concurrency for ingest
func (*cloudImportExecutor) ResourceModified(context.Context, *proto.StepResource) error {
	// Will be added in the future PR
	return nil
}

type ingestCollector struct {
	execute.NoopCollector
	meterRec *metering.Recorder
}

func (c *ingestCollector) Processed(bytes, _ int64) {
	// since the region job might be retried, this value might be larger than
	// the total KV size.
	c.meterRec.IncClusterWriteBytes(uint64(bytes))
}

func getIndexInfoAndID(eleIDs []int64, indexes []*model.IndexInfo) (currentIdx *model.IndexInfo, idxID int64, err error) {
	switch len(eleIDs) {
	case 1:
		for _, idx := range indexes {
			if idx.ID == eleIDs[0] {
				currentIdx = idx
				idxID = idx.ID
				break
			}
		}
	case 0:
		// maybe this subtask is generated from an old version TiDB
		if len(indexes) == 1 {
			currentIdx = indexes[0]
		}
		idxID = indexes[0].ID
	default:
		return nil, 0, errors.Errorf("unexpected EleIDs count %v", eleIDs)
	}
	return
}

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
	goerrors "errors"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
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
	job           *model.Job
	store         kv.Storage
	indexes       []*model.IndexInfo
	ptbl          table.PhysicalTable
	cloudStoreURI string
	metric        *lightningmetric.Common
	backendCtx    ingest.BackendCtx
	backend       *local.Backend

	taskConcurrency int
	engine          atomic.Pointer[external.Engine]
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
		metric:          metrics.RegisterLightningCommonMetricsForDDL(job.ID),
	}, nil
}

func (m *cloudImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor init subtask exec env")
	ctx = lightningmetric.WithCommonMetric(ctx, m.metric)
	cfg, bd, err := ingest.CreateLocalBackend(ctx, m.store, m.job, hasUniqueIndex(m.indexes), false, m.taskConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	bCtx, err := ingest.NewBackendCtxBuilder(ctx, m.store, m.job).Build(cfg, bd)
	if err != nil {
		bd.Close()
		return err
	}
	m.backend = bd
	m.backendCtx = bCtx
	return nil
}

func (m *cloudImportExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor run subtask")

	sm, err := decodeBackfillSubTaskMeta(ctx, m.cloudStoreURI, subtask.Meta)
	if err != nil {
		return err
	}

	local := m.backendCtx.GetLocalBackend()
	if local == nil {
		return errors.Errorf("local backend not found")
	}

	currentIdx, idxID, err := getIndexInfoAndID(sm.EleIDs, m.indexes)
	if err != nil {
		return err
	}

	_, engineUUID := backend.MakeUUID(m.ptbl.Meta().Name.L, idxID)

	all := external.SortedKVMeta{}
	for _, g := range sm.MetaGroups {
		all.Merge(g)
	}

	// compatible with old version task meta
	jobKeys := sm.RangeJobKeys
	if jobKeys == nil {
		jobKeys = sm.RangeSplitKeys
	}
	err = local.CloseEngine(ctx, &backend.EngineConfig{
		External: &backend.ExternalEngineConfig{
			StorageURI:    m.cloudStoreURI,
			DataFiles:     sm.DataFiles,
			StatFiles:     sm.StatFiles,
			StartKey:      all.StartKey,
			EndKey:        all.EndKey,
			JobKeys:       jobKeys,
			SplitKeys:     sm.RangeSplitKeys,
			TotalFileSize: int64(all.TotalKVSize),
			TotalKVCount:  0,
			CheckHotspot:  true,
			MemCapacity:   m.GetResource().Mem.Capacity(),
			OnDup:         common.OnDuplicateKeyError,
		},
		TS: sm.TS,
	}, engineUUID)
	if err != nil {
		return err
	}
	eng := local.GetExternalEngine(engineUUID)
	if eng == nil {
		return errors.Errorf("external engine %s not found", engineUUID)
	}
	m.engine.Store(eng)
	defer m.engine.Store(nil)

	err = local.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	failpoint.Inject("mockCloudImportRunSubtaskError", func(_ failpoint.Value) {
		err = context.DeadlineExceeded
	})
	if err == nil {
		return nil
	}

	if currentIdx != nil {
		return ingest.TryConvertToKeyExistsErr(err, currentIdx, m.ptbl.Meta())
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

func (m *cloudImportExecutor) Cleanup(ctx context.Context) error {
	failpoint.InjectCall("cloudImportExecutorCleanup", m.backend)
	logutil.Logger(ctx).Info("cloud import executor clean up subtask env")
	if m.backendCtx != nil {
		m.backendCtx.Close()
	}
	m.backend.Close()
	return nil
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

// TaskMetaModified changes the max write speed for ingest
func (m *cloudImportExecutor) TaskMetaModified(ctx context.Context, newMeta []byte) error {
	logutil.Logger(ctx).Info("cloud import executor update task meta")
	newTaskMeta := &BackfillTaskMeta{}
	if err := json.Unmarshal(newMeta, newTaskMeta); err != nil {
		return errors.Trace(err)
	}

	newMaxWriteSpeed := newTaskMeta.Job.ReorgMeta.GetMaxWriteSpeedOrDefault()
	if newMaxWriteSpeed != m.job.ReorgMeta.GetMaxWriteSpeedOrDefault() {
		m.job.ReorgMeta.SetMaxWriteSpeed(newMaxWriteSpeed)
		if m.backend != nil {
			m.backend.UpdateWriteSpeedLimit(newMaxWriteSpeed)
		}
	}
	return nil
}

// ResourceModified change the concurrency for ingest
func (m *cloudImportExecutor) ResourceModified(ctx context.Context, newResource *proto.StepResource) error {
	logutil.Logger(ctx).Info("cloud import executor update resource")
	newConcurrency := int(newResource.CPU.Capacity())
	if newConcurrency == m.backend.GetWorkerConcurrency() {
		return nil
	}

	eng := m.engine.Load()
	if eng == nil {
		// let framework retry
		return goerrors.New("engine not started")
	}

	if err := eng.UpdateResource(ctx, newConcurrency, newResource.Mem.Capacity()); err != nil {
		return err
	}

	m.backend.SetWorkerConcurrency(newConcurrency)
	return nil
}

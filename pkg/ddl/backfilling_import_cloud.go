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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
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
<<<<<<< HEAD
	taskexecutor.EmptyStepExecutor
	job           *model.Job
	indexes       []*model.IndexInfo
	ptbl          table.PhysicalTable
	bc            ingest.BackendCtx
	cloudStoreURI string
=======
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
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
}

func newCloudImportExecutor(
	job *model.Job,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	bcGetter func() (ingest.BackendCtx, error),
	cloudStoreURI string,
) (*cloudImportExecutor, error) {
	bc, err := bcGetter()
	if err != nil {
		return nil, err
	}
	return &cloudImportExecutor{
		job:           job,
		indexes:       indexes,
		ptbl:          ptbl,
		bc:            bc,
		cloudStoreURI: cloudStoreURI,
	}, nil
}

<<<<<<< HEAD
func (*cloudImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor init subtask exec env")
=======
func (e *cloudImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor init subtask exec env")
	e.metric = metrics.RegisterLightningCommonMetricsForDDL(e.job.ID)
	ctx = lightningmetric.WithCommonMetric(ctx, e.metric)
	cfg, bd, err := ingest.CreateLocalBackend(ctx, e.store, e.job, false, e.taskConcurrency)
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
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
	return nil
}

func (e *cloudImportExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor run subtask")

<<<<<<< HEAD
	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}

	local := m.bc.GetLocalBackend()
=======
	sm, err := decodeBackfillSubTaskMeta(ctx, e.cloudStoreURI, subtask.Meta)
	if err != nil {
		return err
	}
	local := e.backendCtx.GetLocalBackend()
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
	if local == nil {
		return errors.Errorf("local backend not found")
	}

<<<<<<< HEAD
	currentIdx, idxID, err := getIndexInfoAndID(sm.EleIDs, m.indexes)
	if err != nil {
		return err
=======
	var (
		currentIdx *model.IndexInfo
		idxID      int64
	)
	switch len(sm.EleIDs) {
	case 1:
		for _, idx := range e.indexes {
			if idx.ID == sm.EleIDs[0] {
				currentIdx = idx
				idxID = idx.ID
				break
			}
		}
	case 0:
		// maybe this subtask is generated from an old version TiDB
		if len(e.indexes) == 1 {
			currentIdx = e.indexes[0]
		}
		idxID = e.indexes[0].ID
	default:
		return errors.Errorf("unexpected EleIDs count %v", sm.EleIDs)
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
	}

	_, engineUUID := backend.MakeUUID(e.ptbl.Meta().Name.L, idxID)

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
			StorageURI:    e.cloudStoreURI,
			DataFiles:     sm.DataFiles,
			StatFiles:     sm.StatFiles,
			StartKey:      all.StartKey,
			EndKey:        all.EndKey,
			JobKeys:       jobKeys,
			SplitKeys:     sm.RangeSplitKeys,
			TotalFileSize: int64(all.TotalKVSize),
			TotalKVCount:  0,
			CheckHotspot:  true,
<<<<<<< HEAD
			MemCapacity:   m.GetResource().Mem.Capacity(),
			OnDup:         common.OnDuplicateKeyError,
=======
			MemCapacity:   e.GetResource().Mem.Capacity(),
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
		},
		TS: sm.TS,
	}, engineUUID)
	if err != nil {
		return err
	}
	err = local.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
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

func (e *cloudImportExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor clean up subtask env")
<<<<<<< HEAD
	// cleanup backend context
	ingest.LitBackCtxMgr.Unregister(m.job.ID)
	return nil
}

func (*cloudImportExecutor) OnFinished(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor finish subtask")
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
=======
	if e.backendCtx != nil {
		e.backendCtx.Close()
	}
	e.backend.Close()
	metrics.UnregisterLightningCommonMetricsForDDL(e.job.ID, e.metric)
	return nil
}

// TaskMetaModified changes the max write speed for ingest
func (*cloudImportExecutor) TaskMetaModified(_ context.Context, _ []byte) error {
	// Will be added in the future PR
	return nil
}

// ResourceModified change the concurrency for ingest
func (*cloudImportExecutor) ResourceModified(_ context.Context, _ *proto.StepResource) error {
	// Will be added in the future PR
	return nil
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
}

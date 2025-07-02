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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

type cloudImportExecutor struct {
	taskexecutor.BaseStepExecutor
	job           *model.Job
	index         *model.IndexInfo
	ptbl          table.PhysicalTable
	bc            ingest.BackendCtx
	cloudStoreURI string
	engine        atomic.Pointer[external.Engine]
}

func newCloudImportExecutor(
	job *model.Job,
	index *model.IndexInfo,
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
		index:         index,
		ptbl:          ptbl,
		bc:            bc,
		cloudStoreURI: cloudStoreURI,
	}, nil
}

func (*cloudImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor init subtask exec env")
	return nil
}

func (m *cloudImportExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor run subtask")

	sm, err := decodeBackfillSubTaskMeta(ctx, m.cloudStoreURI, subtask.Meta)
	if err != nil {
		return err
	}

	local := m.bc.GetLocalBackend()
	if local == nil {
		return errors.Errorf("local backend not found")
	}
	_, engineUUID := backend.MakeUUID(m.ptbl.Meta().Name.L, m.index.ID)

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
	if common.ErrFoundDuplicateKeys.Equal(err) {
		err = convertToKeyExistsErr(err, m.index, m.ptbl.Meta())
	}
	return err
}

func (m *cloudImportExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor clean up subtask env")
	// cleanup backend context
	ingest.LitBackCtxMgr.Unregister(m.job.ID)
	return nil
}

// TaskMetaModified changes the max write speed for ingest
func (e *cloudImportExecutor) TaskMetaModified(ctx context.Context, newMeta []byte) error {
	logutil.Logger(ctx).Info("cloud import executor update task meta")
	// TODO(joechenrh): need to cp necessary PR
	// newTaskMeta := &BackfillTaskMeta{}
	// if err := json.Unmarshal(newMeta, newTaskMeta); err != nil {
	// 	return errors.Trace(err)
	// }

	// local := e.bc.GetLocalBackend()
	// newMaxWriteSpeed := newTaskMeta.Job.ReorgMeta.GetMaxWriteSpeed()
	// if newMaxWriteSpeed != e.job.ReorgMeta.GetMaxWriteSpeed() {
	// 	e.job.ReorgMeta.SetMaxWriteSpeed(newMaxWriteSpeed)
	// 	if local != nil {
	// 		local.UpdateWriteSpeedLimit(newMaxWriteSpeed)
	// 	}
	// }
	return nil
}

// ResourceModified change the concurrency for ingest
func (e *cloudImportExecutor) ResourceModified(ctx context.Context, newResource *proto.StepResource) error {
	logutil.Logger(ctx).Info("cloud import executor update resource")
	local := e.bc.GetLocalBackend()
	newConcurrency := int(newResource.CPU.Capacity())
	if newConcurrency == local.Concurrency() {
		return nil
	}

	eng := e.engine.Load()
	if eng != nil {
		if err := eng.UpdateResource(ctx, newConcurrency, newResource.Mem.Capacity()); err != nil {
			return err
		}
	}

	local.SetConcurrency(newConcurrency)
	return nil
}

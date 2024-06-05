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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

type cloudImportExecutor struct {
	taskexecutor.EmptyStepExecutor
	job           *model.Job
	indexes       []*model.IndexInfo
	ptbl          table.PhysicalTable
	bc            ingest.BackendCtx
	cloudStoreURI string
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

func (*cloudImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor init subtask exec env")
	return nil
}

func (m *cloudImportExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor run subtask")

	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}

	local := m.bc.GetLocalBackend()
	if local == nil {
		return errors.Errorf("local backend not found")
	}
	// TODO(lance6716): find the correct index, so "duplicate entry" error can have index name
	currentIdx := m.indexes[0]

	_, engineUUID := backend.MakeUUID(m.ptbl.Meta().Name.L, currentIdx.ID)

	all := external.SortedKVMeta{}
	for _, g := range sm.MetaGroups {
		all.Merge(g)
	}

	err = local.CloseEngine(ctx, &backend.EngineConfig{
		External: &backend.ExternalEngineConfig{
			StorageURI:    m.cloudStoreURI,
			DataFiles:     sm.DataFiles,
			StatFiles:     sm.StatFiles,
			StartKey:      all.StartKey,
			EndKey:        all.EndKey,
			SplitKeys:     sm.RangeSplitKeys,
			TotalFileSize: int64(all.TotalKVSize),
			TotalKVCount:  0,
			CheckHotspot:  true,
		},
		TS: sm.TS,
	}, engineUUID)
	if err != nil {
		return err
	}
	local.WorkerConcurrency = subtask.Concurrency * 2
	err = local.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err == nil {
		return nil
	}

	if len(m.indexes) == 1 {
		return ingest.TryConvertToKeyExistsErr(err, currentIdx, m.ptbl.Meta())
	}

	// TODO(lance6716): after we can find the correct index, we can use the index
	// name here. And fix TestGlobalSortMultiSchemaChange/ingest_dist_gs_backfill
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
	logutil.Logger(ctx).Info("cloud import executor clean up subtask env")
	// cleanup backend context
	ingest.LitBackCtxMgr.Unregister(m.job.ID)
	return nil
}

func (*cloudImportExecutor) OnFinished(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor finish subtask")
	return nil
}

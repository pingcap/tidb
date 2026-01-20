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
	taskexecutor.EmptyStepExecutor
	job           *model.Job
	indexes       []*model.IndexInfo
	ptbl          table.PhysicalTable
	bc            ingest.BackendCtx
	cloudStoreURI string
	metric        *lightningmetric.Common
}

func newCloudImportExecutor(
	ctx context.Context,
	job *model.Job,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	bcGetter func(context.Context, bool) (ingest.BackendCtx, error),
	cloudStoreURI string,
) (c *cloudImportExecutor, err error) {
	c = &cloudImportExecutor{
		job:           job,
		indexes:       indexes,
		ptbl:          ptbl,
		cloudStoreURI: cloudStoreURI,
		metric:        metrics.RegisterLightningCommonMetricsForDDL(job.ID),
	}
	ctx = lightningmetric.WithCommonMetric(ctx, c.metric)
	c.bc, err = bcGetter(ctx, hasUniqueIndex(indexes))
	if err != nil {
		return nil, err
	}
	return c, nil
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
	err = local.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
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
	logutil.Logger(ctx).Info("cloud import executor clean up subtask env")
	// cleanup backend context
	ingest.LitBackCtxMgr.Unregister(m.job.ID)
	metrics.UnregisterLightningCommonMetricsForDDL(m.job.ID, m.metric)
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
}

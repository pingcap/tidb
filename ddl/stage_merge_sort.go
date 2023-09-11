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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type mergeSortStage struct {
	jobID         int64
	index         *model.IndexInfo
	ptbl          table.PhysicalTable
	bc            ingest.BackendCtx
	cloudStoreURI string
}

func newMergeSortStage(
	jobID int64,
	index *model.IndexInfo,
	ptbl table.PhysicalTable,
	bc ingest.BackendCtx,
	cloudStoreURI string,
) (*mergeSortStage, error) {
	return &mergeSortStage{
		jobID:         jobID,
		index:         index,
		ptbl:          ptbl,
		bc:            bc,
		cloudStoreURI: cloudStoreURI,
	}, nil
}

func (*mergeSortStage) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort stage init subtask exec env")
	return nil
}

func (m *mergeSortStage) SplitSubtask(ctx context.Context, subtask *proto.Subtask) ([]proto.MinimalTask, error) {
	logutil.Logger(ctx).Info("merge sort stage split subtask")

	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error",
			zap.String("category", "ddl"),
			zap.Error(err))
		return nil, err
	}

	local := m.bc.GetLocalBackend()
	if local == nil {
		return nil,
			errors.Errorf("local backend not found")
	}
	_, engineUUID := backend.MakeUUID(m.ptbl.Meta().Name.L, int32(m.index.ID))
	err = local.CloseEngine(ctx, &backend.EngineConfig{
		External: &backend.ExternalEngineConfig{
			StorageURI:    m.cloudStoreURI,
			DataFiles:     sm.DataFiles,
			StatFiles:     sm.StatFiles,
			MinKey:        sm.MinKey,
			MaxKey:        sm.MaxKey,
			SplitKeys:     sm.RangeSplitKeys,
			TotalFileSize: int64(sm.TotalKVSize),
			TotalKVCount:  0,
		},
	}, engineUUID)
	if err != nil {
		return nil, err
	}
	err = local.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	return nil, err
}

func (m *mergeSortStage) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort stage clean up subtask env")
	ingest.LitBackCtxMgr.Unregister(m.jobID)
	return nil
}

func (*mergeSortStage) OnFinished(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("merge sort stage finish subtask")
	return nil
}

func (*mergeSortStage) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("merge sort stage rollback subtask")
	return nil
}

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
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type cloudImportExecutor struct {
	d             *ddl
	job           *model.Job
	jobID         int64
	index         *model.IndexInfo
	ptbl          table.PhysicalTable
	bc            ingest.BackendCtx
	unique        bool
	pdLeaderAddr  string
	cloudStoreURI string
}

func newCloudImportExecutor(
	d *ddl,
	job *model.Job,
	jobID int64,
	index *model.IndexInfo,
	ptbl table.PhysicalTable,
	cloudStoreURI string,
) (*cloudImportExecutor, error) {
	return &cloudImportExecutor{
		d:             d,
		job:           job,
		jobID:         jobID,
		index:         index,
		ptbl:          ptbl,
		cloudStoreURI: cloudStoreURI,
	}, nil
}

func (ci *cloudImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor init subtask exec env")
	bc, err := ingest.LitBackCtxMgr.Register(ctx, ci.unique, ci.job.ID, ci.d.etcdCli, ci.pdLeaderAddr, ci.job.ReorgMeta.ResourceGroupName)
	if err != nil {
		return errors.Trace(err)
	}
	ci.bc = bc
	return nil
}

func (ci *cloudImportExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor run subtask")

	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error",
			zap.String("category", "ddl"),
			zap.Error(err))
		return err
	}

	local := ci.bc.GetLocalBackend()
	if local == nil {
		return errors.Errorf("local backend not found")
	}
	_, engineUUID := backend.MakeUUID(ci.ptbl.Meta().Name.L, ci.index.ID)
	err = local.CloseEngine(ctx, &backend.EngineConfig{
		External: &backend.ExternalEngineConfig{
			StorageURI:    ci.cloudStoreURI,
			DataFiles:     sm.DataFiles,
			StatFiles:     sm.StatFiles,
			StartKey:      sm.StartKey,
			EndKey:        sm.EndKey,
			SplitKeys:     sm.RangeSplitKeys,
			TotalFileSize: int64(sm.TotalKVSize),
			TotalKVCount:  0,
			CheckHotspot:  true,
		},
	}, engineUUID)
	if err != nil {
		return err
	}
	err = local.ImportEngine(ctx, engineUUID, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	return err
}

func (*cloudImportExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("cloud import executor clean up subtask env")
	return nil
}

func (*cloudImportExecutor) OnFinished(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("cloud import executor finish subtask")
	return nil
}

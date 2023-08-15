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

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type ingestIndexStage struct {
	d     *ddl
	jobID int64
	index *model.IndexInfo
	ptbl  table.PhysicalTable
	bc    ingest.BackendCtx
}

func newIngestIndexStage(
	jobID int64,
	index *model.IndexInfo,
	ptbl table.PhysicalTable,
	bc ingest.BackendCtx,
) *ingestIndexStage {
	return &ingestIndexStage{
		jobID: jobID,
		index: index,
		ptbl:  ptbl,
		bc:    bc,
	}
}

func (i *ingestIndexStage) InitSubtaskExecEnv(_ context.Context) error {
	logutil.BgLogger().Info("ingest index stage init subtask exec env", zap.String("category", "ddl"))
	_, _, err := i.bc.Flush(i.index.ID, ingest.FlushModeForceGlobal)
	if err != nil {
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = convertToKeyExistsErr(err, i.index, i.ptbl.Meta())
		}
		logutil.BgLogger().Error("flush error", zap.String("category", "ddl"), zap.Error(err))
		return err
	}
	return err
}

func (*ingestIndexStage) SplitSubtask(_ context.Context, _ []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("ingest index stage split subtask", zap.String("category", "ddl"))
	return nil, nil
}

func (i *ingestIndexStage) CleanupSubtaskExecEnv(_ context.Context) error {
	logutil.BgLogger().Info("ingest index stage cleanup subtask exec env", zap.String("category", "ddl"))
	ingest.LitBackCtxMgr.Unregister(i.jobID)
	return nil
}

func (*ingestIndexStage) OnSubtaskFinished(_ context.Context, subtask []byte) ([]byte, error) {
	return subtask, nil
}

func (i *ingestIndexStage) Rollback(_ context.Context) error {
	logutil.BgLogger().Info("ingest index stage rollback backfill add index task",
		zap.String("category", "ddl"), zap.Int64("jobID", i.jobID))
	ingest.LitBackCtxMgr.Unregister(i.jobID)
	return nil
}

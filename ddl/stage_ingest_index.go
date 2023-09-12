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

func (i *ingestIndexStage) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("ingest index stage init subtask exec env")
	_, _, err := i.bc.Flush(i.index.ID, ingest.FlushModeForceGlobal)
	if err != nil {
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = convertToKeyExistsErr(err, i.index, i.ptbl.Meta())
			return err
		}
		logutil.Logger(ctx).Error("flush error", zap.Error(err))
		return err
	}
	return err
}

func (*ingestIndexStage) RunSubtask(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("ingest index stage split subtask")
	return nil
}

func (i *ingestIndexStage) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("ingest index stage cleanup subtask exec env")
	ingest.LitBackCtxMgr.Unregister(i.jobID)
	return nil
}

func (*ingestIndexStage) OnFinished(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("ingest index stage finish subtask")
	return nil
}

func (i *ingestIndexStage) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("ingest index stage rollback backfill add index task")
	ingest.LitBackCtxMgr.Unregister(i.jobID)
	return nil
}

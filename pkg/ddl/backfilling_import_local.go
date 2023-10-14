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
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

type localImportExecutor struct {
	jobID   int64
	indexes []*model.IndexInfo
	ptbl    table.PhysicalTable
	bc      ingest.BackendCtx
}

func newImportFromLocalStepExecutor(
	jobID int64,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	bc ingest.BackendCtx,
) *localImportExecutor {
	return &localImportExecutor{
		jobID:   jobID,
		indexes: indexes,
		ptbl:    ptbl,
		bc:      bc,
	}
}

func (i *localImportExecutor) Init(ctx context.Context) error {
	logutil.Logger(ctx).Info("local import executor init subtask exec env")
	for _, index := range i.indexes {
		_, _, err := i.bc.Flush(index.ID, ingest.FlushModeForceGlobal)
		if err != nil {
			if common.ErrFoundDuplicateKeys.Equal(err) {
				err = convertToKeyExistsErr(err, index, i.ptbl.Meta())
				return err
			}
		}
	}
	return nil
}

func (*localImportExecutor) RunSubtask(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("local import executor run subtask")
	return nil
}

func (*localImportExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("local import executor cleanup subtask exec env")
	return nil
}

func (*localImportExecutor) OnFinished(ctx context.Context, _ *proto.Subtask) error {
	logutil.Logger(ctx).Info("local import executor finish subtask")
	return nil
}

func (*localImportExecutor) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("local import executor rollback subtask")
	return nil
}

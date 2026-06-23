// Copyright 2026 PingCAP, Inc.
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

package tablemode

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// ToImportModeOnSubmit alter the table mode to import mode when the import-into
// job is submitted.
func ToImportModeOnSubmit(
	ctx context.Context,
	taskManager *storage.TaskManager,
	plan *importer.Plan,
) error {
	return alterWithCurrentRuntime(ctx, taskManager, plan, model.TableModeImport)
}

// ResetAfterSubmitFailure reset the table mode to normal when the import-into
// job submission fails.
func ResetAfterSubmitFailure(
	ctx context.Context,
	taskMgr *storage.TaskManager,
	plan *importer.Plan,
	jobID int64,
) {
	err := alterWithCurrentRuntime(ctx, taskMgr, plan, model.TableModeNormal)
	if err != nil {
		logutil.BgLogger().Warn("failed to reset table mode after task submit failure",
			zap.String("db-name", plan.DBName),
			zap.String("table-name", plan.TableInfo.Name.O),
			zap.Int64("db-id", plan.DBID),
			zap.Int64("table-id", plan.TableInfo.ID),
			zap.Int64("job-id", jobID),
			zap.Error(err),
		)
	}
}

func alterWithCurrentRuntime(
	ctx context.Context,
	taskManager *storage.TaskManager,
	plan *importer.Plan,
	mode model.TableMode,
) error {
	var runtime sqlsvrapi.Runtime
	if err := taskManager.WithNewTxn(ctx, func(se sessionctx.Context) error {
		runtime = se.GetSQLServer().GetRuntime()
		return nil
	}); err != nil {
		return errors.Trace(err)
	}
	return runtime.AlterTableMode(ctx, BuildRequest(plan, mode))
}

// ResetWithTaskRuntime reset the table to Normal mode using the task runtime,
// which is used in cleanup step of cross-keyspace route.
func ResetWithTaskRuntime(
	ctx context.Context,
	taskMgr *storage.TaskManager,
	targetKS string,
	taskID int64,
	plan *importer.Plan,
) error {
	holderID := dxfutil.GenHolderID("cleanup", taskID)
	runtime, releaseFn, err := dxfutil.AcquireTaskRuntime(taskMgr, targetKS, holderID)
	if err != nil {
		return errors.Trace(err)
	}
	defer releaseFn()
	return runtime.AlterTableMode(ctx, BuildRequest(plan, model.TableModeNormal))
}

// BuildRequest build a AlterTableModeRequest for the given plan and mode.
func BuildRequest(plan *importer.Plan, mode model.TableMode) model.AlterTableModeRequest {
	return model.AlterTableModeRequest{
		SchemaID:           plan.DBID,
		TableID:            plan.TableInfo.ID,
		ExpectedSchemaName: plan.DBName,
		ExpectedTableName:  plan.TableInfo.Name.L,
		TableMode:          mode,
	}
}

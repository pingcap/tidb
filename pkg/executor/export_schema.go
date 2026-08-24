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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/pingcap/tidb/pkg/dxf/export"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// exportSchemaDB is one schema and its resolved base tables, built once at
// executor-build time (mirrors ExportTableExec resolving its table up front).
type exportSchemaDB struct {
	dbInfo *model.DBInfo
	tables []*model.TableInfo
}

// ExportSchemaExec submits an export DXF task covering every base table
// across one or more schemas, and optionally waits for it. This is a
// performance-testing prototype, see pkg/dxf/export.
type ExportSchemaExec struct {
	exec.BaseExecutor

	userSctx sessionctx.Context
	plan     *plannercore.ExportSchema
	dbs      []exportSchemaDB
	done     bool
}

func newExportSchemaExec(b exec.BaseExecutor, userSctx sessionctx.Context,
	plan *plannercore.ExportSchema, dbs []exportSchemaDB) *ExportSchemaExec {
	return &ExportSchemaExec{
		BaseExecutor: b,
		userSctx:     userSctx,
		plan:         plan,
		dbs:          dbs,
	}
}

func (e *ExportSchemaExec) validate() error {
	if e.plan.Format != nil && strings.ToLower(*e.plan.Format) != "csv" {
		return errors.Errorf("EXPORT SCHEMA only supports csv format now")
	}
	for _, db := range e.dbs {
		for _, tblInfo := range db.tables {
			for _, col := range tblInfo.Columns {
				if col.IsGenerated() && !col.GeneratedStored {
					return errors.Errorf("EXPORT SCHEMA does not support virtual generated column %s.%s",
						tblInfo.Name.O, col.Name.O)
				}
			}
		}
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ExportSchemaExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)

	if err := e.validate(); err != nil {
		return err
	}
	opts, err := parseExportOptions(e.userSctx, e.plan.Options)
	if err != nil {
		return err
	}
	opts.thread = defaultThread(ctx, opts)

	store := e.userSctx.GetStore()
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return err
	}
	snapshotTS := ver.Ver

	dbSpecs := make([]export.DBSpec, 0, len(e.dbs))
	for _, db := range e.dbs {
		tableIDs := make([]int64, len(db.tables))
		for i, tblInfo := range db.tables {
			tableIDs[i] = tblInfo.ID
		}
		dbSpecs = append(dbSpecs, export.DBSpec{
			DBID:     db.dbInfo.ID,
			DBName:   db.dbInfo.Name.O,
			TableIDs: tableIDs,
		})
	}
	taskMeta := &export.TaskMeta{
		DBs:        dbSpecs,
		SnapshotTS: snapshotTS,
		DestURI:    e.plan.Path,
		Format:     "csv",
		FileSize:   opts.fileSize,
	}
	// rootID only needs to be stable and unique per statement; the first
	// schema's id is as good as any other single id for that purpose.
	taskKey := export.TaskKey(e.dbs[0].dbInfo.ID, snapshotTS)

	// cover the gap until the scheduler's safepoint keeper takes over.
	if pdStore, ok := store.(kv.StorageWithPD); ok {
		err := gc.NewManager(pdStore.GetPDClient(), store.GetCodec().GetKeyspaceID()).
			SetServiceSafePoint(ctx, gc.BRServiceSafePoint{
				ID:       "export-" + taskKey,
				TTL:      submitGCTTL,
				BackupTS: snapshotTS,
			})
		if err != nil {
			logutil.Logger(ctx).Warn("set export gc safepoint failed, snapshot may be GCed during a long export",
				zap.Error(err))
		}
	}

	task, err := export.SubmitTask(ctx, store, taskKey, opts.thread, taskMeta)
	if err != nil {
		return err
	}
	logutil.Logger(ctx).Info("export schema task submitted",
		zap.String("task-key", taskKey), zap.Int64("task-id", task.ID))

	status := "submitted"
	if !opts.detached {
		if err := handle.WaitTaskDoneOrPaused(ctx, task.ID); err != nil {
			return err
		}
		status = "succeed"
	}
	req.AppendInt64(0, task.ID)
	req.AppendString(1, taskKey)
	req.AppendString(2, status)
	return nil
}

var _ exec.Executor = (*ExportSchemaExec)(nil)

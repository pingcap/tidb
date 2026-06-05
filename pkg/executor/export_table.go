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

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/pingcap/tidb/pkg/dxf/export"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	defaultExportFileSize = 256 * units.MiB
	// submitGCTTL covers the gap between submit and the scheduler's own
	// safepoint keeper taking over.
	submitGCTTL = 10 * 60
)

// ExportTableExec submits an export DXF task and optionally waits for it.
// This is a performance-testing prototype, see pkg/dxf/export.
type ExportTableExec struct {
	exec.BaseExecutor

	userSctx sessionctx.Context
	plan     *plannercore.ExportTable
	tbl      table.Table
	done     bool
}

func newExportTableExec(b exec.BaseExecutor, userSctx sessionctx.Context,
	plan *plannercore.ExportTable, tbl table.Table) *ExportTableExec {
	return &ExportTableExec{
		BaseExecutor: b,
		userSctx:     userSctx,
		plan:         plan,
		tbl:          tbl,
	}
}

type exportOptions struct {
	fileSize int64
	// thread is the task concurrency, which is also the per-subtask encoder
	// count. 0 means auto: min(8, executor node CPU).
	thread            int
	writersPerEncoder int
	subtaskRegions    int
	detached          bool
}

func (e *ExportTableExec) parseOptions() (*exportOptions, error) {
	opts := &exportOptions{
		fileSize: defaultExportFileSize,
	}
	evalCtx := e.userSctx.GetExprCtx().GetEvalCtx()
	for _, opt := range e.plan.Options {
		optAsInt64 := func() (int64, error) {
			if opt.Value == nil {
				return 0, errors.Errorf("option %s requires a value", opt.Name)
			}
			d, err := opt.Value.Eval(evalCtx, chunk.Row{})
			if err != nil {
				return 0, err
			}
			return d.ToInt64(evalCtx.TypeCtx())
		}
		switch opt.Name {
		case "file_size":
			if opt.Value == nil {
				return nil, errors.Errorf("option file_size requires a value")
			}
			d, err := opt.Value.Eval(evalCtx, chunk.Row{})
			if err != nil {
				return nil, err
			}
			v, err := units.RAMInBytes(strings.TrimSpace(d.GetString()))
			if err != nil || v <= 0 {
				return nil, errors.Errorf("invalid file_size value %q", d.GetString())
			}
			opts.fileSize = v
		case "thread":
			v, err := optAsInt64()
			if err != nil || v <= 0 || v > 256 {
				return nil, errors.Errorf("invalid thread value")
			}
			opts.thread = int(v)
		case "writers_per_encoder":
			v, err := optAsInt64()
			if err != nil || v <= 0 || v > 16 {
				return nil, errors.Errorf("invalid writers_per_encoder value")
			}
			opts.writersPerEncoder = int(v)
		case "subtask_regions":
			v, err := optAsInt64()
			if err != nil || v <= 0 {
				return nil, errors.Errorf("invalid subtask_regions value")
			}
			opts.subtaskRegions = int(v)
		case "detached":
			opts.detached = true
		default:
			return nil, errors.Errorf("unknown EXPORT TABLE option %s", opt.Name)
		}
	}
	return opts, nil
}

func (e *ExportTableExec) validate() error {
	if e.plan.Format != nil && strings.ToLower(*e.plan.Format) != "csv" {
		return errors.Errorf("EXPORT TABLE only supports csv format now")
	}
	for _, col := range e.tbl.Meta().Columns {
		if col.IsGenerated() && !col.GeneratedStored {
			return errors.Errorf("EXPORT TABLE does not support virtual generated column %s", col.Name.O)
		}
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *ExportTableExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)

	if err := e.validate(); err != nil {
		return err
	}
	opts, err := e.parseOptions()
	if err != nil {
		return err
	}

	if opts.thread == 0 {
		// task creation rejects requiredSlots larger than the executor node
		// CPU, so clamp the default by it.
		opts.thread = 8
		if cpu, err := handle.GetCPUCountOfNode(ctx); err == nil && cpu > 0 {
			opts.thread = min(opts.thread, cpu)
		}
	}

	store := e.userSctx.GetStore()
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return err
	}
	snapshotTS := ver.Ver
	tblInfo := e.tbl.Meta()
	taskMeta := &export.TaskMeta{
		DBName:            e.plan.Table.Schema.O,
		TableInfo:         tblInfo,
		SnapshotTS:        snapshotTS,
		Dest:              e.plan.Path,
		Format:            "csv",
		FileSize:          opts.fileSize,
		SubtaskRegions:    opts.subtaskRegions,
		WritersPerEncoder: opts.writersPerEncoder,
	}
	taskKey := export.TaskKey(tblInfo.ID, snapshotTS)

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
	logutil.Logger(ctx).Info("export table task submitted",
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

var _ exec.Executor = (*ExportTableExec)(nil)

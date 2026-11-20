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

package autoanalyze

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type taskExecutor struct {
	*taskexecutor.BaseTaskExecutor
}

var _ taskexecutor.TaskExecutor = (*taskExecutor)(nil)

func newTaskExecutor(ctx context.Context, task *proto.Task, param taskexecutor.Param) *taskExecutor {
	e := &taskExecutor{BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param)}
	e.BaseTaskExecutor.Extension = e
	return e
}

// IsIdempotent returns true: re-running ANALYZE only refreshes the statistics again.
func (*taskExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (e *taskExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	return &stepExecutor{runtime: e.TaskRuntime, keyspace: task.Keyspace, taskTable: e.GetTaskTable()}, nil
}

// IsRetryableError returns false: the priority queue of the stats owner
// analyzes the table again later if the statement fails.
func (*taskExecutor) IsRetryableError(error) bool {
	return false
}

type stepExecutor struct {
	taskexecutor.BaseStepExecutor
	runtime   sqlsvrapi.Runtime
	keyspace  string
	taskTable taskexecutor.TaskTable
}

// RunSubtask runs the analyze statement in the task keyspace.
func (s *stepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	meta := &TaskMeta{}
	if err := json.Unmarshal(subtask.Meta, meta); err != nil {
		return errors.Trace(err)
	}
	// The domain of the current node. It serves the task keyspace on the classic
	// kernel, and the SYSTEM keyspace on a DXF service node of the next-gen kernel.
	var localDom *domain.Domain
	if err := s.taskTable.WithNewSession(func(se sessionctx.Context) error {
		localDom = domain.GetDomain(se)
		return nil
	}); err != nil {
		return errors.Trace(err)
	}
	if localDom == nil {
		return errors.New("the current node has no domain")
	}

	start := time.Now()
	var err error
	onServiceNode := localDom.Store().GetKeyspace() != s.keyspace
	if onServiceNode {
		err = runOnServiceNode(ctx, s.runtime, localDom, meta)
	} else {
		err = runOnNormalNode(ctx, localDom, meta)
	}
	logutil.Logger(ctx).Info("dist auto analyze subtask finished",
		zap.String("taskKeyspace", s.keyspace), zap.String("sql", meta.SQL), zap.Strings("params", meta.Params),
		zap.Bool("onServiceNode", onServiceNode), zap.Duration("cost", time.Since(start)), zap.Error(err))
	return err
}

// runOnNormalNode runs the statement on a normal TiDB node, which is a node of the task keyspace.
func runOnNormalNode(ctx context.Context, dom *domain.Domain, meta *TaskMeta) error {
	statsHandle := dom.StatsHandle()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)
	return statsutil.CallWithSCtx(statsHandle.SPool(), func(sctx sessionctx.Context) error {
		return runAnalyze(ctx, sctx, statsHandle, dom.SysProcTracker(), meta)
	})
}

// runAnalyze runs the statement of meta in the session. The slot of the subtask does not
// limit it: it builds with tidb_auto_build_stats_concurrency, as on the stats owner.
func runAnalyze(
	ctx context.Context,
	sctx sessionctx.Context,
	procIDGenerator statsutil.AutoAnalyzeProcIDGenerator,
	sysProcTracker sysproctrack.Tracker,
	meta *TaskMeta,
) error {
	_, _, err := exec.RunAnalyzeStmtWithCtx(ctx, sctx, procIDGenerator, sysProcTracker,
		statistics.Version2, meta.NeedVersionRewriteWarn, meta.SQL, meta.args()...)
	return err
}

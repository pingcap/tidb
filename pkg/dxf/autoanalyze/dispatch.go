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
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"go.uber.org/zap"
)

const (
	// rowLimit is the row count from which a statement goes to DXF, because a task adds
	// seconds of scheduling. It is a conservative round number, not a measured break-even.
	rowLimit = 1_000_000
)

// Register registers the auto-analyze task type to DXF, and the dispatcher that the stats
// owner uses for the statements at or above the row limit, see exec.SetDispatcher.
func Register() {
	scheduler.RegisterSchedulerFactory(proto.AutoAnalyze,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			return newScheduler(ctx, task, param)
		},
	)
	taskexecutor.RegisterTaskType(proto.AutoAnalyze,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			return newTaskExecutor(ctx, task, param)
		},
	)
	exec.SetDispatcher(dispatch)
}

// dispatch implements exec.Dispatcher. It returns false, before it changes anything, for a
// statement that is below the row limit or that the task meta cannot describe.
func dispatch(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	_ sysproctrack.Tracker,
	needVersionRewriteWarn bool,
	table exec.AnalyzedTable,
	sql string,
	params ...any,
) (bool, error) {
	// The statement of a table that is gone runs here, and fails as it does without DXF.
	tblInfo, ok := sctx.GetLatestInfoSchema().TableInfoByID(table.ID)
	if !ok {
		return false, nil
	}
	taskMeta, ok := newTaskMeta(tblInfo, needVersionRewriteWarn, sql, params)
	if !ok || !reachesRowLimit(statsHandle, tblInfo, table.PartitionNames) {
		return false, nil
	}
	return true, runAsTask(sctx, statsHandle, taskMeta)
}

// reachesRowLimit reports whether the stats cache of this node has at least
// rowLimit rows for the table or the partitions that the statement analyzes.
func reachesRowLimit(statsHandle statstypes.StatsHandle, tblInfo *model.TableInfo, partitionNames []string) bool {
	var rows int64
	for _, id := range analyzedPhysicalIDs(tblInfo, partitionNames) {
		// The cache can hold pseudo stats: GetPhysicalTableStats puts them there when it
		// finds none. Their row count is a placeholder.
		if tblStats, ok := statsHandle.GetNonPseudoPhysicalTableStats(id); ok && !tblStats.Pseudo {
			rows += tblStats.RealtimeCount
		}
	}
	return rows >= rowLimit
}

// analyzedPhysicalIDs returns the ID of the table, or the IDs of the partitions that the statement
// names. Auto-analyze always names them: a statement that names none stays below the row limit.
func analyzedPhysicalIDs(tblInfo *model.TableInfo, partitionNames []string) []int64 {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}
	}
	named := make(map[string]struct{}, len(partitionNames))
	for _, name := range partitionNames {
		named[strings.ToLower(name)] = struct{}{}
	}
	physicalIDs := make([]int64, 0, len(named))
	for _, def := range pi.Definitions {
		if _, ok := named[def.Name.L]; ok {
			physicalIDs = append(physicalIDs, def.ID)
		}
	}
	return physicalIDs
}

// runAsTask runs the statement as a DXF task and waits for it, so the slot of
// tidb_auto_analyze_concurrency stays taken.
func runAsTask(
	sctx sessionctx.Context,
	statsHandle statstypes.StatsHandle,
	taskMeta *TaskMeta,
) error {
	if _, err := storage.GetDXFSvcTaskMgr(); err != nil {
		// DXF is not ready on this node yet. The priority queue analyzes the table again later.
		return errors.Trace(err)
	}
	meta, err := json.Marshal(taskMeta)
	if err != nil {
		return errors.Trace(err)
	}

	keyspace := sctx.GetStore().GetKeyspace()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	// The key names the table, and ends in a UUID: DXF refuses a key that an
	// earlier task has used.
	taskKey := fmt.Sprintf("%s/%s/%d/%s", keyspace, proto.AutoAnalyze, taskMeta.TableID, uuid.NewString())
	start := time.Now()
	task, err := submitTask(ctx, taskKey, keyspace, meta)
	if err != nil {
		return err
	}
	err = waitTask(ctx, task)
	if err == nil {
		// A statement that runs here ends with loading the new stats into the cache of this
		// node. Without them the priority queue sees the old stats and queues the table again.
		err = loadTableStats(ctx, sctx, statsHandle, taskMeta.TableID)
	}
	statslogutil.StatsLogger().Info("auto analyze dispatched through DXF",
		zap.Int64("taskID", task.ID), zap.String("taskKey", task.Key),
		zap.String("sql", taskMeta.SQL), zap.Strings("params", taskMeta.Params),
		zap.Duration("cost", time.Since(start)), zap.Error(err))
	return err
}

// loadTableStats loads the stats of the table and its partitions from the
// storage into the cache of the stats handle.
func loadTableStats(ctx context.Context, sctx sessionctx.Context, statsHandle statstypes.StatsHandle, tableID int64) error {
	is := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	tblInfo, ok := is.TableInfoByID(tableID)
	if !ok {
		// Nothing to load for a table that is gone.
		return nil
	}
	ids := []int64{tblInfo.ID}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
		}
	}
	return errors.Trace(statsHandle.Update(ctx, is, ids...))
}

// newTaskMeta builds the task meta of the statement. The meta holds identifiers only, so it
// returns false for a statement with any other parameter, which then runs on this node.
func newTaskMeta(tblInfo *model.TableInfo, needVersionRewriteWarn bool, sql string, params []any) (*TaskMeta, bool) {
	meta := &TaskMeta{
		SQL:                    sql,
		Params:                 make([]string, 0, len(params)),
		NeedVersionRewriteWarn: needVersionRewriteWarn,
		TableID:                tblInfo.ID,
	}
	for _, p := range params {
		s, ok := p.(string)
		if !ok {
			return nil, false
		}
		meta.Params = append(meta.Params, s)
	}
	return meta, true
}

// submitTask submits the statement as a task that runs on one node.
func submitTask(ctx context.Context, taskKey, keyspace string, meta []byte) (*proto.TaskBase, error) {
	// The task asks for one slot whatever the build concurrency of its statement
	// is: the scan and the sampling of ANALYZE, most of its work, run in TiKV.
	task, err := handle.SubmitTask(ctx, taskKey, proto.AutoAnalyze, keyspace, 1, targetScope(), 1, meta)
	if err != nil {
		// The task may have been created, do not leave it running unobserved.
		if cancelErr := handle.CancelTask(ctx, taskKey); cancelErr != nil {
			statslogutil.StatsLogger().Warn("cancel auto analyze task failed", zap.String("taskKey", taskKey), zap.Error(cancelErr))
		}
		return nil, errors.Trace(err)
	}
	return &task.TaskBase, nil
}

// targetScope returns the scope of the nodes that run the tasks. On the classic kernel it is empty:
// DXF then prefers the background nodes, and the scope of the stats owner does not limit the tasks.
func targetScope() string {
	if kerneltype.IsNextGen() {
		return handle.NextGenTargetScope
	}
	return ""
}

// waitTask waits until the task succeeds.
func waitTask(ctx context.Context, task *proto.TaskBase) error {
	found, err := handle.WaitTaskDoneOrPausedWithResult(ctx, task.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if found.State != proto.TaskStateSucceed {
		return errors.Errorf("auto analyze task %d stopped with state %s", task.ID, found.State)
	}
	return nil
}

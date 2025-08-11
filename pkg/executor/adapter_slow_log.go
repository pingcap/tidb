// Copyright 2025 PingCAP, Inc.
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
	"bytes"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

func makeExecDetailAccessors(getter func(*execdetails.ExecDetails) interface{}) variable.FieldGetterAndSetter {
	return variable.FieldGetterAndSetter{
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			if items.ExecDetail == nil {
				execDetail := ctx.GetSessionVars().StmtCtx.GetExecDetails()
				items.ExecDetail = &execDetail
			}
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return getter(items.ExecDetail)
		},
	}
}

func makeCopDetailAccessors(getter func(*execdetails.CopTasksDetails) interface{}) variable.FieldGetterAndSetter {
	return variable.FieldGetterAndSetter{
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			if items.CopTasks == nil {
				copTasksDetail := ctx.GetSessionVars().StmtCtx.CopTasksDetails()
				items.CopTasks = copTasksDetail
			}
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return getter(items.CopTasks)
		},
	}
}

var SlowLogFieldGetterAndSetter = map[string]variable.FieldGetterAndSetter{
	"Keyspace_name": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.KeyspaceName = keyspace.GetKeyspaceNameBySettings()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.KeyspaceName
		},
	},
	"Conn_ID": {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().ConnectionID
		},
	},
	"Session_alias": {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().SessionAlias
		},
	},
	"DB": {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().CurrentDB
		},
	},
	"Exec_retry_count": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ExecRetryCount = ctx.GetSessionVars().StmtCtx.ExecRetryCount
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.ExecRetryCount
		},
	},
	"Query_time": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.TimeTotal = ctx.GetSessionVars().GetTotalCostDuration()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.TimeTotal
		},
	},
	"Parse_time": {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationParse
		},
	},
	"Compile_time": {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationCompile
		},
	},
	"Optimize_time": {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationOptimization
		},
	},
	"Wait_TS": {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationWaitTS
		},
	},
	"Rewrite_time": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.RewriteInfo = ctx.GetSessionVars().RewritePhaseInfo
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.RewriteInfo.DurationPreprocessSubQuery.Seconds()
		},
	},
	"Process_time": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.TimeDetail.ProcessTime.Seconds()
	}),
	"Backoff_time": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.BackoffTime.Seconds()
	}),
	"Total_keys": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.ScanDetail.TotalKeys
	}),
	"Process_keys": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.ScanDetail.ProcessedKeys
	}),
	"Prewrite_time": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.PrewriteTime.Seconds()
	}),
	"Commit_time": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.CommitTime.Seconds()
	}),
	"Write_keys": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.WriteKeys
	}),
	"Write_size": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.WriteSize
	}),
	"Prewrite_region": makeExecDetailAccessors(func(d *execdetails.ExecDetails) interface{} {
		return atomic.LoadInt32(&d.CommitDetail.PrewriteRegionNum)
	}),
	"Digest": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, digest := ctx.GetSessionVars().StmtCtx.SQLDigest()
			items.Digest = digest.String()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.Digest
		},
	},
	"Num_cop_tasks": makeCopDetailAccessors(func(d *execdetails.CopTasksDetails) interface{} {
		return d.NumCopTasks
	}),
	"Cop_proc_avg": makeCopDetailAccessors(func(d *execdetails.CopTasksDetails) interface{} {
		return d.ProcessTimeStats.AvgTime
	}),
	"Cop_proc_max": makeCopDetailAccessors(func(d *execdetails.CopTasksDetails) interface{} {
		return d.ProcessTimeStats.MaxTime
	}),
	"Cop_proc_addr": makeCopDetailAccessors(func(d *execdetails.CopTasksDetails) interface{} {
		return d.ProcessTimeStats.MaxAddress
	}),
	"Cop_wait_avg": makeCopDetailAccessors(func(d *execdetails.CopTasksDetails) interface{} {
		return d.WaitTimeStats.AvgTime
	}),
	"Cop_wait_max": makeCopDetailAccessors(func(d *execdetails.CopTasksDetails) interface{} {
		return d.WaitTimeStats.MaxTime
	}),
	"Cop_wait_addr": makeCopDetailAccessors(func(d *execdetails.CopTasksDetails) interface{} {
		return d.WaitTimeStats.MaxAddress
	}),
	"Mem_max": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.MemMax = ctx.GetSessionVars().MemTracker.MaxConsumed()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.MemMax
		},
	},
	"Disk_max": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.DiskMax = ctx.GetSessionVars().DiskTracker.MaxConsumed()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.DiskMax
		},
	},
	"Succ": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.Succ = ctx.GetSessionVars().StmtCtx.ExecSucc
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.Succ
		},
	},
	"Plan_digest": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, planDigest := GetPlanDigest(ctx.GetSessionVars().StmtCtx)
			items.PlanDigest = planDigest.String()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.PlanDigest
		},
	},
	"Resource_group": {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ResourceGroupName = ctx.GetSessionVars().StmtCtx.ResourceGroupName
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.ResourceGroupName
		},
	},
}

// PrepareSlowLogItemsForRules builds a SlowQueryLogItems containing only the fields referenced by current session's SlowLogRules.
// These pre-collected fields are later used for matching SQL execution details against the rules.
func PrepareSlowLogItemsForRules(ctx sessionctx.Context) *variable.SlowQueryLogItems {
	items := &variable.SlowQueryLogItems{}
	for field := range ctx.GetSessionVars().SlowLogRules.AllConditionFields {
		gs := SlowLogFieldGetterAndSetter[field]
		if gs.Setter != nil {
			gs.Setter(ctx, items)
		}
	}

	return items
}

// completeSlowLogItemsForRules fills in the remaining SlowQueryLogItems fields
// that are relevant to triggering the current session's SlowLogRules.
func completeSlowLogItemsForRules(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
	for field, sg := range SlowLogFieldGetterAndSetter {
		if _, ok := ctx.GetSessionVars().SlowLogRules.AllConditionFields[field]; ok {
			continue
		}

		if sg.Setter != nil {
			sg.Setter(ctx, items)
		}
	}
}

// SetSlowLogItems fills the remaining fields of SlowQueryLogItems after SQL execution.
func SetSlowLogItems(a *ExecStmt, txnTS uint64, hasMoreResults bool, items *variable.SlowQueryLogItems) {
	completeSlowLogItemsForRules(a.Ctx, items)

	sessVars := a.Ctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	var indexNames string
	if len(stmtCtx.IndexNames) > 0 {
		// remove duplicate index.
		idxMap := make(map[string]struct{})
		buf := bytes.NewBuffer(make([]byte, 0, 4))
		buf.WriteByte('[')
		for _, idx := range stmtCtx.IndexNames {
			_, ok := idxMap[idx]
			if ok {
				continue
			}
			idxMap[idx] = struct{}{}
			if buf.Len() > 1 {
				buf.WriteByte(',')
			}
			buf.WriteString(idx)
		}
		buf.WriteByte(']')
		indexNames = buf.String()
	}

	stmtDetail, tikvExecDetail, ruDetails := execdetails.GetExecDetailsFromContext(a.GoCtx)

	binaryPlan := ""
	if variable.GenerateBinaryPlan.Load() {
		binaryPlan = getBinaryPlan(a.Ctx)
		if len(binaryPlan) > 0 {
			binaryPlan = variable.SlowLogBinaryPlanPrefix + binaryPlan + variable.SlowLogPlanSuffix
		}
	}

	var keyspaceID uint32
	keyspaceName := keyspace.GetKeyspaceNameBySettings()
	if !keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		keyspaceID = uint32(a.Ctx.GetStore().GetCodec().GetKeyspaceID())
	}

	items.TxnTS = txnTS
	items.KeyspaceName = keyspaceName
	items.KeyspaceID = keyspaceID
	items.SQL = FormatSQL(a.GetTextToLog(true)).String()
	items.IndexNames = indexNames
	items.Plan = getPlanTree(stmtCtx)
	items.BinaryPlan = binaryPlan
	items.Prepared = a.isPreparedStmt
	items.HasMoreResults = hasMoreResults
	items.PlanFromCache = sessVars.FoundInPlanCache
	items.PlanFromBinding = sessVars.FoundInBinding
	items.RewriteInfo = sessVars.RewritePhaseInfo
	items.KVExecDetail = &tikvExecDetail
	items.WriteSQLRespTotal = stmtDetail.WriteSQLRespDuration
	items.ResultRows = stmtCtx.GetResultRowsCount()
	items.IsExplicitTxn = sessVars.TxnCtx.IsExplicit
	items.IsWriteCacheTable = stmtCtx.WaitLockLeaseTime > 0
	items.UsedStats = stmtCtx.GetUsedStatsInfo(false)
	items.IsSyncStatsFailed = stmtCtx.IsSyncStatsFailed
	items.Warnings = variable.CollectWarningsForSlowLog(stmtCtx)
	items.ResourceGroupName = stmtCtx.ResourceGroupName
	items.RRU = ruDetails.RRU()
	items.WRU = ruDetails.WRU()
	items.WaitRUDuration = ruDetails.RUWaitDuration()
	items.CPUUsages = sessVars.SQLCPUUsages.GetCPUUsages()
	items.StorageKV = stmtCtx.IsTiKV.Load()
	items.StorageMPP = stmtCtx.IsTiFlash.Load()

	if a.retryCount > 0 {
		items.ExecRetryTime = items.TimeTotal - sessVars.DurationParse - sessVars.DurationCompile - time.Since(a.retryStartTime)
	}
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok && sessVars.PrevStmt != nil {
		items.PrevStmt = sessVars.PrevStmt.String()
	}
}

// Match checks whether the given SlowQueryLogItems satisfies any of the
// current session's slow log trigger rules.
//
// Matching is evaluated in two levels of logical relationship:
//   - Rules are combined with OR: if any rule is satisfied, the function returns true.
//   - Conditions inside a rule are combined with AND: all conditions must be met for that rule.
//
// Returns true if any rule matches, false otherwise.
func Match(ctx sessionctx.Context, items *variable.SlowQueryLogItems) bool {
	rules := ctx.GetSessionVars().SlowLogRules
	// Or logical relationship
	for _, rule := range rules.Rules {
		matched := true

		// And logical relationship
		for _, condition := range rule.Conditions {
			gs := SlowLogFieldGetterAndSetter[condition.Field]
			value := gs.Getter(ctx, items)

			switch v := value.(type) {
			case bool:
				tv, ok := condition.Threshold.(bool)
				if !ok || v != tv {
					matched = false
					break
				}
			case string:
				tv, ok := condition.Threshold.(string)
				if !ok || v != tv {
					matched = false
					break
				}
			case int64:
				tv, ok := condition.Threshold.(int64)
				if !ok || v < tv {
					matched = false
					break
				}
			case uint64:
				tv, ok := condition.Threshold.(uint64)
				if !ok || v < tv {
					matched = false
					break
				}
			case time.Duration:
				tv, ok := condition.Threshold.(time.Duration)
				if !ok || v < tv {
					matched = false
					break
				}
			default:
				matched = false
				break
			}
		}
		if matched {
			return matched
		}
	}

	return false
}

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

// SlowLogFieldAccessor defines how to get or set a specific field in SlowQueryLogItems.
// - Setter is optional and pre-fills the field before matching if it needs explicit preparation.
// - Getter is required and returns the field value for rule matching.
type SlowLogFieldAccessor struct {
	Setter func(ctx sessionctx.Context, items *variable.SlowQueryLogItems)
	Getter func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{}
}

func makeExecDetailAccessor(getter func(*execdetails.ExecDetails) interface{}) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
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

func makeCopDetailAccessor(getter func(*execdetails.CopTasksDetails) interface{}) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
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

// slowLogRuleFieldAccessors defines the set of field accessors for SlowQueryLogItems
// that are relevant to evaluating and triggering SlowLogRules.
var slowLogRuleFieldAccessors = map[string]SlowLogFieldAccessor{
	variable.SlowLogConnIDStr: {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().ConnectionID
		},
	},
	variable.SlowLogSessAliasStr: {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().SessionAlias
		},
	},
	variable.SlowLogDBStr: {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().CurrentDB
		},
	},
	variable.SlowLogExecRetryCount: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ExecRetryCount = ctx.GetSessionVars().StmtCtx.ExecRetryCount
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.ExecRetryCount
		},
	},
	variable.SlowLogQueryTimeStr: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.TimeTotal = ctx.GetSessionVars().GetTotalCostDuration()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.TimeTotal
		},
	},
	variable.SlowLogParseTimeStr: {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationParse
		},
	},
	variable.SlowLogCompileTimeStr: {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationCompile
		},
	},
	variable.SlowLogOptimizeTimeStr: {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationOptimization
		},
	},
	variable.SlowLogWaitTSTimeStr: {
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return ctx.GetSessionVars().DurationWaitTS
		},
	},
	variable.SlowLogDigestStr: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, digest := ctx.GetSessionVars().StmtCtx.SQLDigest()
			items.Digest = digest.String()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.Digest
		},
	},
	variable.SlowLogNumCopTasksStr: makeCopDetailAccessor(func(d *execdetails.CopTasksDetails) interface{} {
		return d.NumCopTasks
	}),
	variable.SlowLogCopProcAvg: makeCopDetailAccessor(func(d *execdetails.CopTasksDetails) interface{} {
		return d.ProcessTimeStats.AvgTime
	}),
	variable.SlowLogCopProcMax: makeCopDetailAccessor(func(d *execdetails.CopTasksDetails) interface{} {
		return d.ProcessTimeStats.MaxTime
	}),
	variable.SlowLogCopProcAddr: makeCopDetailAccessor(func(d *execdetails.CopTasksDetails) interface{} {
		return d.ProcessTimeStats.MaxAddress
	}),
	variable.SlowLogCopWaitAvg: makeCopDetailAccessor(func(d *execdetails.CopTasksDetails) interface{} {
		return d.WaitTimeStats.AvgTime
	}),
	variable.SlowLogCopWaitMax: makeCopDetailAccessor(func(d *execdetails.CopTasksDetails) interface{} {
		return d.WaitTimeStats.MaxTime
	}),
	variable.SlowLogCopWaitAddr: makeCopDetailAccessor(func(d *execdetails.CopTasksDetails) interface{} {
		return d.WaitTimeStats.MaxAddress
	}),
	variable.SlowLogMemMax: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.MemMax = ctx.GetSessionVars().MemTracker.MaxConsumed()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.MemMax
		},
	},
	variable.SlowLogDiskMax: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.DiskMax = ctx.GetSessionVars().DiskTracker.MaxConsumed()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.DiskMax
		},
	},
	variable.SlowLogWriteSQLRespTotal: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			// TODO: ctx is context.Context, not sessionctx.Context, so we need to use Value instead of GetSessionVars().
			stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
			if stmtDetailRaw != nil {
				stmtDetail := *(stmtDetailRaw.(*execdetails.StmtExecDetails))
				items.WriteSQLRespTotal = stmtDetail.WriteSQLRespDuration
			}
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.WriteSQLRespTotal
		},
	},
	variable.SlowLogSucc: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.Succ = ctx.GetSessionVars().StmtCtx.ExecSucc
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.Succ
		},
	},
	variable.SlowLogPlanDigest: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, planDigest := GetPlanDigest(ctx.GetSessionVars().StmtCtx)
			items.PlanDigest = planDigest.String()
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.PlanDigest
		},
	},
	variable.SlowLogResourceGroup: {
		Setter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ResourceGroupName = ctx.GetSessionVars().StmtCtx.ResourceGroupName
		},
		Getter: func(ctx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.ResourceGroupName
		},
	},
	// The following fields are related to execdetails.ExecDetails.
	execdetails.ProcessTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.TimeDetail.ProcessTime.Seconds()
	}),
	execdetails.BackoffTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.BackoffTime.Seconds()
	}),
	execdetails.TotalKeysStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.ScanDetail.TotalKeys
	}),
	execdetails.ProcessKeysStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.ScanDetail.ProcessedKeys
	}),
	execdetails.PreWriteTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.PrewriteTime.Seconds()
	}),
	execdetails.CommitTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.CommitTime.Seconds()
	}),
	execdetails.WriteKeysStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.WriteKeys
	}),
	execdetails.WriteSizeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return d.CommitDetail.WriteSize
	}),
	execdetails.PrewriteRegionStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails) interface{} {
		return atomic.LoadInt32(&d.CommitDetail.PrewriteRegionNum)
	}),
}

// PrepareSlowLogItemsForRules builds a SlowQueryLogItems containing only the fields referenced by current session's SlowLogRules.
// These pre-collected fields are later used for matching SQL execution details against the rules.
func PrepareSlowLogItemsForRules(ctx sessionctx.Context) *variable.SlowQueryLogItems {
	items := &variable.SlowQueryLogItems{}
	for field := range ctx.GetSessionVars().SlowLogRules.AllConditionFields {
		gs := slowLogRuleFieldAccessors[field]
		if gs.Setter != nil {
			gs.Setter(ctx, items)
		}
	}

	return items
}

// completeSlowLogItemsForRules fills in the remaining SlowQueryLogItems fields
// that are relevant to triggering the current session's SlowLogRules.
func completeSlowLogItemsForRules(ctx sessionctx.Context, items *variable.SlowQueryLogItems) {
	for field, sg := range slowLogRuleFieldAccessors {
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

	_, tikvExecDetail, ruDetails := execdetails.GetExecDetailsFromContext(a.GoCtx)

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
	items.ResultRows = stmtCtx.GetResultRowsCount()
	items.IsExplicitTxn = sessVars.TxnCtx.IsExplicit
	items.IsWriteCacheTable = stmtCtx.WaitLockLeaseTime > 0
	items.UsedStats = stmtCtx.GetUsedStatsInfo(false)
	items.IsSyncStatsFailed = stmtCtx.IsSyncStatsFailed
	items.Warnings = variable.CollectWarningsForSlowLog(stmtCtx)
	items.ResourceGroupName = stmtCtx.ResourceGroupName
	items.RUDetails = ruDetails
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
			gs := slowLogRuleFieldAccessors[condition.Field]
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

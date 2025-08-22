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
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/tikv/client-go/v2/util"
)

// SlowLogFieldAccessor defines how to get or set a specific field in SlowQueryLogItems.
// - Setter is optional and pre-fills the field before matching if it needs explicit preparation.
// - Match evaluates whether the field in SlowQueryLogItems meets a specific threshold.
//   - threshold is the value to compare against when determining a match.
type SlowLogFieldAccessor struct {
	Setter func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems)
	Match  func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool
}

func makeExecDetailAccessor(match func(*execdetails.ExecDetails, any) bool) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			if items.ExecDetail == nil {
				execDetail := seCtx.GetSessionVars().StmtCtx.GetExecDetails()
				items.ExecDetail = &execDetail
			}
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			if items.ExecDetail == nil {
				return false
			}
			return match(items.ExecDetail, threshold)
		},
	}
}

func makeKVExecDetailAccessor(match func(*util.ExecDetails, any) bool) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Setter: func(ctx context.Context, _ sessionctx.Context, items *variable.SlowQueryLogItems) {
			if items.KVExecDetail == nil {
				tikvExecDetailRaw := ctx.Value(util.ExecDetailsKey)
				if tikvExecDetailRaw != nil {
					items.KVExecDetail = tikvExecDetailRaw.(*util.ExecDetails)
				}
			}
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			if items.KVExecDetail == nil {
				return false
			}
			return match(items.KVExecDetail, threshold)
		},
	}
}

func matchStringVal(threshold any, v string) bool {
	tv, ok := threshold.(string)
	if !ok || v != tv {
		return false
	}
	return true
}

func matchBoolVal(threshold any, v bool) bool {
	tv, ok := threshold.(bool)
	if !ok || v != tv {
		return false
	}
	return true
}

func matchInt64Val(threshold any, v int64) bool {
	tv, ok := threshold.(int64)
	if !ok || v < tv {
		return false
	}
	return true
}

func matchUint64Val(threshold any, v uint64) bool {
	tv, ok := threshold.(uint64)
	if !ok || v < tv {
		return false
	}
	return true
}

func matchDurationVal(threshold any, v time.Duration) bool {
	tv, ok := threshold.(float64)
	if !ok || v.Seconds() < tv {
		return false
	}
	return true
}

// SlowLogRuleFieldAccessors defines the set of field accessors for SlowQueryLogItems
// that are relevant to evaluating and triggering SlowLogRules.
// It's exporting for testing.
var SlowLogRuleFieldAccessors = map[string]SlowLogFieldAccessor{
	variable.SlowLogConnIDStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchUint64Val(threshold, seCtx.GetSessionVars().ConnectionID)
		},
	},
	variable.SlowLogSessAliasStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchStringVal(threshold, seCtx.GetSessionVars().SessionAlias)
		},
	},
	variable.SlowLogDBStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchStringVal(threshold, seCtx.GetSessionVars().CurrentDB)
		},
	},
	variable.SlowLogExecRetryCount: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ExecRetryCount = seCtx.GetSessionVars().StmtCtx.ExecRetryCount
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchUint64Val(threshold, items.ExecRetryCount)
		},
	},
	variable.SlowLogQueryTimeStr: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.TimeTotal = seCtx.GetSessionVars().GetTotalCostDuration()
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchDurationVal(threshold, items.TimeTotal)
		},
	},
	variable.SlowLogParseTimeStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchDurationVal(threshold, seCtx.GetSessionVars().DurationParse)
		},
	},
	variable.SlowLogCompileTimeStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchDurationVal(threshold, seCtx.GetSessionVars().DurationCompile)
		},
	},
	variable.SlowLogOptimizeTimeStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchDurationVal(threshold, seCtx.GetSessionVars().DurationOptimization)
		},
	},
	variable.SlowLogWaitTSTimeStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchDurationVal(threshold, seCtx.GetSessionVars().DurationWaitTS)
		},
	},
	variable.SlowLogIsInternalStr: {
		Match: func(seCtx sessionctx.Context, _ *variable.SlowQueryLogItems, threshold any) bool {
			return matchBoolVal(threshold, seCtx.GetSessionVars().InRestrictedSQL)
		},
	},
	variable.SlowLogDigestStr: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, digest := seCtx.GetSessionVars().StmtCtx.SQLDigest()
			items.Digest = digest.String()
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchStringVal(threshold, items.Digest)
		},
	},
	variable.SlowLogNumCopTasksStr: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			copTasksDetail := seCtx.GetSessionVars().StmtCtx.CopTasksDetails()
			items.CopTasks = copTasksDetail
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchInt64Val(threshold, int64(items.CopTasks.NumCopTasks))
		},
	},
	variable.SlowLogMemMax: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.MemMax = seCtx.GetSessionVars().MemTracker.MaxConsumed()
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchInt64Val(threshold, items.MemMax)
		},
	},
	variable.SlowLogDiskMax: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.DiskMax = seCtx.GetSessionVars().DiskTracker.MaxConsumed()
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchInt64Val(threshold, items.DiskMax)
		},
	},
	variable.SlowLogWriteSQLRespTotal: {
		Setter: func(ctx context.Context, _ sessionctx.Context, items *variable.SlowQueryLogItems) {
			stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
			if stmtDetailRaw != nil {
				stmtDetail := *(stmtDetailRaw.(*execdetails.StmtExecDetails))
				items.WriteSQLRespTotal = stmtDetail.WriteSQLRespDuration
			}
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchDurationVal(threshold, items.WriteSQLRespTotal)
		},
	},
	variable.SlowLogSucc: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.Succ = seCtx.GetSessionVars().StmtCtx.ExecSuccess
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchBoolVal(threshold, items.Succ)
		},
	},
	variable.SlowLogPlanDigest: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, planDigest := GetPlanDigest(seCtx.GetSessionVars().StmtCtx)
			items.PlanDigest = planDigest.String()
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchStringVal(threshold, items.PlanDigest)
		},
	},
	variable.SlowLogResourceGroup: {
		Setter: func(_ context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ResourceGroupName = seCtx.GetSessionVars().StmtCtx.ResourceGroupName
		},
		Match: func(_ sessionctx.Context, items *variable.SlowQueryLogItems, threshold any) bool {
			return matchStringVal(threshold, items.ResourceGroupName)
		},
	},
	// The following fields are related to util.ExecDetails.
	variable.SlowLogKVTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchDurationVal(threshold, time.Duration(d.WaitKVRespDuration))
	}),
	variable.SlowLogPDTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchDurationVal(threshold, time.Duration(d.WaitPDRespDuration))
	}),
	variable.SlowLogBackoffTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchDurationVal(threshold, time.Duration(d.BackoffDuration))
	}),
	variable.SlowLogUnpackedBytesSentTiKVTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesSentKVTotal)
	}),
	variable.SlowLogUnpackedBytesReceivedTiKVTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesReceivedKVTotal)
	}),
	variable.SlowLogUnpackedBytesSentTiKVCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesSentKVCrossZone)
	}),
	variable.SlowLogUnpackedBytesReceivedTiKVCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesReceivedKVCrossZone)
	}),
	variable.SlowLogUnpackedBytesSentTiFlashTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesSentMPPTotal)
	}),
	variable.SlowLogUnpackedBytesReceivedTiFlashTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesReceivedMPPTotal)
	}),
	variable.SlowLogUnpackedBytesSentTiFlashCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesSentMPPCrossZone)
	}),
	variable.SlowLogUnpackedBytesReceivedTiFlashCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.UnpackedBytesReceivedMPPCrossZone)
	}),
	// The following fields are related to execdetails.ExecDetails.
	execdetails.ProcessTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchDurationVal(threshold, d.TimeDetail.ProcessTime)
	}),
	execdetails.BackoffTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchDurationVal(threshold, d.BackoffTime)
	}),
	execdetails.TotalKeysStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.ScanDetail.TotalKeys)
	}),
	execdetails.ProcessKeysStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, d.ScanDetail.ProcessedKeys)
	}),
	execdetails.PreWriteTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchDurationVal(threshold, d.CommitDetail.PrewriteTime)
	}),
	execdetails.CommitTimeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchDurationVal(threshold, d.CommitDetail.CommitTime)
	}),
	execdetails.WriteKeysStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, int64(d.CommitDetail.WriteKeys))
	}),
	execdetails.WriteSizeStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, int64(d.CommitDetail.WriteSize))
	}),
	execdetails.PrewriteRegionStr: makeExecDetailAccessor(func(d *execdetails.ExecDetails, threshold any) bool {
		return matchInt64Val(threshold, int64(atomic.LoadInt32(&d.CommitDetail.PrewriteRegionNum)))
	}),
}

// PrepareSlowLogItemsForRules builds a SlowQueryLogItems containing only the fields referenced by current session's SlowLogRules.
// These pre-collected fields are later used for matching SQL execution details against the rules.
func PrepareSlowLogItemsForRules(ctx context.Context, seCtx sessionctx.Context) *variable.SlowQueryLogItems {
	items := &variable.SlowQueryLogItems{}
	for field := range seCtx.GetSessionVars().SlowLogRules.AllConditionFields {
		gs := SlowLogRuleFieldAccessors[field]
		if gs.Setter != nil {
			gs.Setter(ctx, seCtx, items)
		}
	}

	return items
}

// Match checks whether the given SlowQueryLogItems satisfies any of the
// current session's slow log trigger rules.
//
// Matching is evaluated in two levels of logical relationship:
//   - Rules are combined with OR: if any rule is satisfied, the function returns true.
//   - Conditions inside a rule are combined with AND: all conditions must be met for that rule.
//
// Returns true if any rule matches, false otherwise.
func Match(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) bool {
	rules := seCtx.GetSessionVars().SlowLogRules
	// Or logical relationship
	for _, rule := range rules.Rules {
		matched := true

		// And logical relationship
		for _, condition := range rule.Conditions {
			accessor := SlowLogRuleFieldAccessors[condition.Field]
			if ok := accessor.Match(seCtx, items, condition.Threshold); !ok {
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

// CompleteSlowLogItemsForRules fills in the remaining SlowQueryLogItems fields
// that are relevant to triggering the current session's SlowLogRules.
// It's exporting for testing.
func CompleteSlowLogItemsForRules(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
	if items == nil {
		return
	}

	for field, sg := range SlowLogRuleFieldAccessors {
		if _, ok := seCtx.GetSessionVars().SlowLogRules.AllConditionFields[field]; ok {
			continue
		}

		if sg.Setter != nil {
			sg.Setter(ctx, seCtx, items)
		}
	}
}

// SetSlowLogItems fills the remaining fields of SlowQueryLogItems after SQL execution.
func SetSlowLogItems(a *ExecStmt, txnTS uint64, hasMoreResults bool, items *variable.SlowQueryLogItems) {
	CompleteSlowLogItemsForRules(a.GoCtx, a.Ctx, items)

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

	var ruDetails *util.RUDetails
	if ruDetailsVal := a.GoCtx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
		ruDetails = ruDetailsVal.(*util.RUDetails)
	} else {
		ruDetails = util.NewRUDetails()
	}

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

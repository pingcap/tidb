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
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/tikv/client-go/v2/util"
)

// SlowLogFieldAccessor defines how to get or set a specific field in SlowQueryLogItems.
// - Setter is optional and pre-fills the field before matching if it needs explicit preparation.
// - Getter is required and returns the field value for rule matching.
type SlowLogFieldAccessor struct {
	Setter func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems)
	Getter func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{}
}

func makeExecDetailAccessor(getter func(*execdetails.ExecDetails) interface{}) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			if items.ExecDetail == nil {
				execDetail := seCtx.GetSessionVars().StmtCtx.GetExecDetails()
				items.ExecDetail = &execDetail
			}
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return getter(items.ExecDetail)
		},
	}
}

func makeKVExecDetailAccessor(getter func(*util.ExecDetails) interface{}) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			if items.KVExecDetail == nil {
				tikvExecDetailRaw := ctx.Value(util.ExecDetailsKey)
				if tikvExecDetailRaw != nil {
					items.KVExecDetail = tikvExecDetailRaw.(*util.ExecDetails)
				}
			}
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return getter(items.KVExecDetail)
		},
	}
}

// SlowLogRuleFieldAccessors defines the set of field accessors for SlowQueryLogItems
// that are relevant to evaluating and triggering SlowLogRules.
// It's exporting for testing.
var SlowLogRuleFieldAccessors = map[string]SlowLogFieldAccessor{
	variable.SlowLogConnIDStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().ConnectionID
		},
	},
	variable.SlowLogSessAliasStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().SessionAlias
		},
	},
	variable.SlowLogDBStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().CurrentDB
		},
	},
	variable.SlowLogExecRetryCount: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ExecRetryCount = seCtx.GetSessionVars().StmtCtx.ExecRetryCount
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.ExecRetryCount
		},
	},
	variable.SlowLogQueryTimeStr: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.TimeTotal = seCtx.GetSessionVars().GetTotalCostDuration()
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.TimeTotal
		},
	},
	variable.SlowLogParseTimeStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().DurationParse
		},
	},
	variable.SlowLogCompileTimeStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().DurationCompile
		},
	},
	variable.SlowLogOptimizeTimeStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().DurationOptimization
		},
	},
	variable.SlowLogWaitTSTimeStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().DurationWaitTS
		},
	},
	variable.SlowLogIsInternalStr: {
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return seCtx.GetSessionVars().InRestrictedSQL
		},
	},
	variable.SlowLogDigestStr: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, digest := seCtx.GetSessionVars().StmtCtx.SQLDigest()
			items.Digest = digest.String()
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.Digest
		},
	},
	variable.SlowLogNumCopTasksStr: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			copTasksDetail := seCtx.GetSessionVars().StmtCtx.CopTasksDetails()
			items.CopTasks = copTasksDetail
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.CopTasks.NumCopTasks
		},
	},
	variable.SlowLogMemMax: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.MemMax = seCtx.GetSessionVars().MemTracker.MaxConsumed()
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.MemMax
		},
	},
	variable.SlowLogDiskMax: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.DiskMax = seCtx.GetSessionVars().DiskTracker.MaxConsumed()
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.DiskMax
		},
	},
	variable.SlowLogWriteSQLRespTotal: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
			if stmtDetailRaw != nil {
				stmtDetail := *(stmtDetailRaw.(*execdetails.StmtExecDetails))
				items.WriteSQLRespTotal = stmtDetail.WriteSQLRespDuration
			}
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.WriteSQLRespTotal
		},
	},
	variable.SlowLogSucc: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.Succ = seCtx.GetSessionVars().StmtCtx.ExecSuccess
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.Succ
		},
	},
	variable.SlowLogPlanDigest: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			_, planDigest := GetPlanDigest(seCtx.GetSessionVars().StmtCtx)
			items.PlanDigest = planDigest.String()
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.PlanDigest
		},
	},
	variable.SlowLogResourceGroup: {
		Setter: func(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
			items.ResourceGroupName = seCtx.GetSessionVars().StmtCtx.ResourceGroupName
		},
		Getter: func(seCtx sessionctx.Context, items *variable.SlowQueryLogItems) interface{} {
			return items.ResourceGroupName
		},
	},
	// The following fields are related to util.ExecDetails.
	variable.SlowLogKVTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return time.Duration(d.WaitKVRespDuration)
	}),
	variable.SlowLogPDTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return time.Duration(d.WaitPDRespDuration)
	}),
	variable.SlowLogBackoffTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return time.Duration(d.BackoffDuration)
	}),
	variable.SlowLogUnpackedBytesSentTiKVTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesSentKVTotal
	}),
	variable.SlowLogUnpackedBytesReceivedTiKVTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesReceivedKVTotal
	}),
	variable.SlowLogUnpackedBytesSentTiKVCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesSentKVCrossZone
	}),
	variable.SlowLogUnpackedBytesReceivedTiKVCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesReceivedKVCrossZone
	}),
	variable.SlowLogUnpackedBytesSentTiFlashTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesSentMPPTotal
	}),
	variable.SlowLogUnpackedBytesReceivedTiFlashTotal: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesReceivedMPPTotal
	}),
	variable.SlowLogUnpackedBytesSentTiFlashCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesSentMPPCrossZone
	}),
	variable.SlowLogUnpackedBytesReceivedTiFlashCrossZone: makeKVExecDetailAccessor(func(d *util.ExecDetails) interface{} {
		return d.UnpackedBytesReceivedMPPCrossZone
	}),
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

// CompleteSlowLogItemsForRules fills in the remaining SlowQueryLogItems fields
// that are relevant to triggering the current session's SlowLogRules.
// It's exporting for testing.
func CompleteSlowLogItemsForRules(ctx context.Context, seCtx sessionctx.Context, items *variable.SlowQueryLogItems) {
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

		log.Warn(fmt.Sprintf("xxx--------------------------- %#v", rule.Conditions))
		// And logical relationship
		for _, condition := range rule.Conditions {
			gs := SlowLogRuleFieldAccessors[condition.Field]
			value := gs.Getter(seCtx, items)
			log.Warn(fmt.Sprintf("xxx--------------------------- field:%s, val:%v", condition.Field, value))

			switch v := value.(type) {
			case bool:
				tv, ok := condition.Threshold.(bool)
				log.Warn(fmt.Sprintf("xxx-------------------val:%v", tv))
				if !ok || v != tv {
					matched = false
					break
				}
			case string:
				tv, ok := condition.Threshold.(string)
				log.Warn(fmt.Sprintf("xxx--------------------threshold:%v, tv:%q, v:%q, type:%v, type:%v", condition.Threshold, tv, v, reflect.TypeOf(tv), reflect.TypeOf(condition.Threshold)))
				if !ok || v != tv {
					log.Warn(fmt.Sprintf("xxx--------------------v != tv:%v", v != tv))
					matched = false
					break
				}
			case int64:
				tv, ok := condition.Threshold.(int64)
				log.Warn(fmt.Sprintf("xxx--------------------val:%v", tv))
				if !ok || v < tv {
					matched = false
					break
				}
			case uint:
				tv, ok := condition.Threshold.(uint)
				log.Warn(fmt.Sprintf("xxx--------------------threshold:%v, tv:%v, type:%v, type:%v", condition.Threshold, tv, reflect.TypeOf(tv), reflect.TypeOf(condition.Threshold)))
				if !ok || v < tv {
					matched = false
					break
				}
			case uint64:
				tv, ok := condition.Threshold.(uint64)
				log.Warn(fmt.Sprintf("xxx--------------------threshold:%v, tv:%v, type:%v, type:%v", condition.Threshold, tv, reflect.TypeOf(tv), reflect.TypeOf(condition.Threshold)))
				if !ok || v < tv {
					matched = false
					break
				}
			case time.Duration:
				tv, ok := condition.Threshold.(float64)
				log.Warn(fmt.Sprintf("xxx--------------------val:%v", tv))
				if !ok || v.Seconds() < tv {
					matched = false
					break
				}
			default:
				log.Warn(fmt.Sprintf("xxx--------------------val:%T, type:%v", v, reflect.TypeOf(v)))
				matched = false
				break
			}
		}
		log.Warn(fmt.Sprintf("xxx--------------------------- matched:%#v", matched))
		if matched {
			return matched
		}
	}

	return false
}

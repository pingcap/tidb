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
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/tikv/client-go/v2/util"
)

func init() {
	variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogPlanDigest)] = variable.SlowLogFieldAccessor{
		Parse: variable.ParseString,
		Setter: func(_ context.Context, seVars *variable.SessionVars, items *variable.SlowQueryLogItems) {
			_, planDigest := GetPlanDigest(seVars.StmtCtx)
			items.PlanDigest = planDigest.String()
		},
		Match: func(_ *variable.SessionVars, items *variable.SlowQueryLogItems, threshold any) bool {
			return variable.MatchEqual(threshold, strings.ToLower(items.PlanDigest))
		},
	}
}

// PrepareSlowLogItemsForRules builds a SlowQueryLogItems containing only the fields referenced by current session's SlowLogRules.
// These pre-collected fields are later used for matching SQL execution details against the rules.
func PrepareSlowLogItemsForRules(ctx context.Context, seVars *variable.SessionVars) *variable.SlowQueryLogItems {
	items := &variable.SlowQueryLogItems{}
	for field := range seVars.SlowLogRules.AllConditionFields {
		gs := variable.SlowLogRuleFieldAccessors[field]
		if gs.Setter != nil {
			gs.Setter(ctx, seVars, items)
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
func Match(seVars *variable.SessionVars, items *variable.SlowQueryLogItems) bool {
	rules := seVars.SlowLogRules
	// Or logical relationship
	for _, rule := range rules.Rules {
		matched := true

		// And logical relationship
		for _, condition := range rule.Conditions {
			accessor := variable.SlowLogRuleFieldAccessors[strings.ToLower(condition.Field)]
			if ok := accessor.Match(seVars, items, condition.Threshold); !ok {
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
func CompleteSlowLogItemsForRules(ctx context.Context, seVars *variable.SessionVars, items *variable.SlowQueryLogItems) {
	if items == nil {
		return
	}

	for field, sg := range variable.SlowLogRuleFieldAccessors {
		if _, ok := seVars.SlowLogRules.AllConditionFields[field]; ok {
			continue
		}

		if sg.Setter != nil {
			sg.Setter(ctx, seVars, items)
		}
	}
}

// SetSlowLogItems fills the remaining fields of SlowQueryLogItems after SQL execution.
func SetSlowLogItems(a *ExecStmt, txnTS uint64, hasMoreResults bool, items *variable.SlowQueryLogItems) {
	CompleteSlowLogItemsForRules(a.GoCtx, a.Ctx.GetSessionVars(), items)

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

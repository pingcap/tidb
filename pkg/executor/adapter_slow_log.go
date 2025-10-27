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
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
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

var sampleLoggerFactory = logutil.SampleLoggerFactory(time.Minute, 1, zap.String(logutil.LogFieldCategory, "slow log"))

func mergeConditionFields(dst, src map[string]struct{}) {
	for k := range src {
		dst[k] = struct{}{}
	}
}

func updateAllRuleFields(globalRules *slowlogrule.GlobalSlowLogRules, seVars *variable.SessionVars) {
	if seVars.SlowLogRules.NeedUpdateEffectiveFields ||
		globalRules.RawRulesHash != seVars.SlowLogRules.GlobalRawRulesHash {
		allRuleFields := make(map[string]struct{})
		if seVars.SlowLogRules.SlowLogRules != nil {
			mergeConditionFields(allRuleFields, seVars.SlowLogRules.Fields)
		}
		if specificSessionRules, ok := globalRules.RulesMap[int64(seVars.ConnectionID)]; ok {
			mergeConditionFields(allRuleFields, specificSessionRules.Fields)
		}
		if clusterRules, ok := globalRules.RulesMap[variable.UnsetConnID]; ok {
			mergeConditionFields(allRuleFields, clusterRules.Fields)
		}
		seVars.SlowLogRules.EffectiveFields = allRuleFields
		seVars.SlowLogRules.GlobalRawRulesHash = globalRules.RawRulesHash
		seVars.SlowLogRules.NeedUpdateEffectiveFields = false
	}
}

var slowQueryLogItemsPool = sync.Pool{
	New: func() any { return &variable.SlowQueryLogItems{} },
}

func getSlowLogItems() *variable.SlowQueryLogItems {
	return slowQueryLogItemsPool.Get().(*variable.SlowQueryLogItems)
}

func putSlowLogItems(items *variable.SlowQueryLogItems) {
	if items == nil {
		return
	}
	*items = variable.SlowQueryLogItems{}
	slowQueryLogItemsPool.Put(items)
}

// PrepareSlowLogItemsForRules builds a SlowQueryLogItems containing only the fields referenced by the effective slow log rules.
// These pre-collected fields are later used for matching SQL execution details against the rules.
func PrepareSlowLogItemsForRules(ctx context.Context, globalRules *slowlogrule.GlobalSlowLogRules, seVars *variable.SessionVars) *variable.SlowQueryLogItems {
	updateAllRuleFields(globalRules, seVars)
	if len(seVars.SlowLogRules.EffectiveFields) == 0 {
		return nil
	}

	var items *variable.SlowQueryLogItems
	for field := range seVars.SlowLogRules.EffectiveFields {
		if gs, ok := variable.SlowLogRuleFieldAccessors[field]; ok && gs.Setter != nil {
			if items == nil {
				items = getSlowLogItems()
			}
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
func Match(seVars *variable.SessionVars, items *variable.SlowQueryLogItems, rules *slowlogrule.SlowLogRules) bool {
	if rules == nil {
		return false
	}

	// Or logical relationship
	for _, rule := range rules.Rules {
		match := true

		// And logical relationship
		for _, condition := range rule.Conditions {
			accessor := variable.SlowLogRuleFieldAccessors[strings.ToLower(condition.Field)]
			if ok := accessor.Match(seVars, items, condition.Threshold); !ok {
				match = false
				break
			}
		}

		if match {
			return true
		}
	}

	return false
}

// ShouldWriteSlowLog determines whether the current query should be written to the slow log.
// The decision is based on the following rule hierarchy:
//
// 1. Session-level rules: if defined, they take precedence.
// 2. Global-level rules:
//   - If a rule specifies a ConnID that matches the current session, it is applied.
//   - If a rule does not specify ConnID (global default), it is also applied.
//
// 3. All applicable rules are combined with logical OR.
//
// Returns true if any rule matches, otherwise false.
func ShouldWriteSlowLog(globalRules *slowlogrule.GlobalSlowLogRules, seVars *variable.SessionVars, items *variable.SlowQueryLogItems) bool {
	if isMatched := Match(seVars, items, seVars.SlowLogRules.SlowLogRules); isMatched {
		return isMatched
	}

	if specificSessionRules, ok := globalRules.RulesMap[int64(seVars.ConnectionID)]; ok {
		if isMatched := Match(seVars, items, specificSessionRules); isMatched {
			return isMatched
		}
	}
	if clusterRules, ok := globalRules.RulesMap[variable.UnsetConnID]; ok {
		if isMatched := Match(seVars, items, clusterRules); isMatched {
			return isMatched
		}
	}

	return false
}

// CompleteSlowLogItemsForRules fills in the remaining fields of SlowQueryLogItems
// that were not required by the session's effective rule set.
// It's exporting for testing.
func CompleteSlowLogItemsForRules(ctx context.Context, seVars *variable.SessionVars, items *variable.SlowQueryLogItems) {
	for field, sg := range variable.SlowLogRuleFieldAccessors {
		if _, ok := seVars.SlowLogRules.EffectiveFields[field]; ok {
			continue
		}

		if sg.Setter != nil {
			sg.Setter(ctx, seVars, items)
		}
	}
}

// SetSlowLogItems fills the remaining fields of SlowQueryLogItems after SQL execution.
func SetSlowLogItems(a *ExecStmt, txnTS uint64, hasMoreResults bool, items *variable.SlowQueryLogItems) {
	if items == nil {
		return
	}
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
	if txnTS == 0 {
		// TODO: txnTS maybe ambiguous, consider logging stale-read-ts with a new field in the slow log.
		txnTS = sessVars.TxnCtx.StaleReadTs
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

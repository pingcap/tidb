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

	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
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

// resolveBindingDigestInput returns the statement the binding digest should
// be computed from, and — when non-empty — the reason this statement can
// never produce one. EXPLAIN unwraps to its inner statement; EXECUTE resolves
// its prepared statement on demand (there is no reset-time producer anymore,
// so an EXECUTE nobody logs costs no binding normalization at all; resolution
// cannot fail here in a way it didn't already fail during compilation — the
// same *ast.ExecuteStmt node carries the cached prepared statement from that
// lookup, and DEALLOCATE cannot interleave inside a statement). An EXECUTE
// whose inner statement has no binding semantics counts as execute-unresolved,
// not non-dml. An empty reason means the returned statement is DML-shaped and
// eligible for normalization — an empty digest after that point is a restore
// failure.
func resolveBindingDigestInput(stmtNode ast.StmtNode, sessVars *variable.SessionVars) (ast.StmtNode, string) {
	switch x := stmtNode.(type) {
	case *ast.ExecuteStmt:
		prepStmt, err := plannercore.GetPreparedStmt(x, sessVars)
		if err != nil {
			return nil, metrics.LblBindingDigestExecuteUnresolved
		}
		inner, reason := resolveBindingDigestInput(prepStmt.PreparedAst.Stmt, sessVars)
		if reason == metrics.LblBindingDigestNonDML {
			reason = metrics.LblBindingDigestExecuteUnresolved
		}
		return inner, reason
	case *ast.ExplainStmt:
		// Classification only: the digest of an EXPLAIN row is computed from
		// the ExplainStmt itself — norm's own Text()-gated unwrap is the
		// correct one (the text lives on the wrapper; a manual unwrap here
		// would hand norm an inner statement with an empty Text(), which its
		// gate treats as "no bind record wanted").
		if _, reason := resolveBindingDigestInput(x.Stmt, sessVars); reason != "" {
			return nil, reason
		}
		return x, ""
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
		return x, ""
	}
	return nil, metrics.LblBindingDigestNonDML
}

// ensureBindingDigest is the single on-demand computation behind the slow log
// Binding_digest field: the stmtctx memo is consulted first, and a statement
// whose memo is still empty — e.g. TryFastPlan point queries return before
// the binding match, which no longer computes the digest at all — gets its
// digest computed here from resolveBindingDigestInput's statement with the
// execution-time current database (for EXECUTE the prepared inner AST, which
// bakes the PREPARE-time db, so the digest lands on the same vocabulary the
// binding match would hit). Statements without binding semantics (non-DML,
// unresolved EXECUTE, restore failure) return "" — absence, never a fallback
// to the exec digest. A computed value is memoized, so the rule evaluation
// and the flush of the same statement share one normalization pass.
func ensureBindingDigest(stmtNode ast.StmtNode, sessVars *variable.SessionVars) string {
	if digest, ok := sessVars.StmtCtx.BindingDigest(); ok {
		return digest
	}
	inner, reason := resolveBindingDigestInput(stmtNode, sessVars)
	if reason != "" {
		return ""
	}
	normalized, digest := norm.NormalizeStmtForBinding(inner, norm.WithSpecifiedDB(sessVars.CurrentDB))
	if digest == "" {
		return ""
	}
	sessVars.StmtCtx.InitBindingDigest(normalized, digest)
	return digest
}

// prepareBindingDigestForRules is the rule-evaluation consumer of the binding
// digest: a `Binding_digest` trigger rule must fire for fast-path statements
// too (TryFastPlan returns before the binding match, so at rule time the memo
// is still empty and the accessor's memo-only read sees nothing), so the
// digest is computed here on demand, before the rules are matched — but only
// when the effective rule set actually references the field. Not counted: the
// empty-reason counter is a flush-time reconciliation, and a statement whose
// rules reject it is not slow-logged.
func prepareBindingDigestForRules(stmtNode ast.StmtNode, sessVars *variable.SessionVars, items *variable.SlowQueryLogItems) {
	if items == nil || items.BindingDigest != "" {
		return
	}
	if _, ok := sessVars.SlowLogRules.EffectiveFields[strings.ToLower(variable.SlowLogBindingDigest)]; !ok {
		return
	}
	items.BindingDigest = ensureBindingDigest(stmtNode, sessVars)
}

// lazyFillBindingDigest is the flush-time consumer of the binding digest:
// the slow log is being written anyway, which is already the cold path, so a
// statement whose digest is still empty gets it computed via
// ensureBindingDigest. Statements without binding semantics (non-DML,
// EXECUTE whose memo never resolved, restore failure) stay empty —
// never a fallback to the exec digest — and every empty outcome is counted
// here, once per slow-logged statement, with its reason.
func lazyFillBindingDigest(stmtNode ast.StmtNode, sessVars *variable.SessionVars, items *variable.SlowQueryLogItems) {
	if items.BindingDigest != "" {
		return
	}
	reason := bindingDigestEmptyReason(stmtNode, sessVars)
	if reason == "" {
		if digest := ensureBindingDigest(stmtNode, sessVars); digest != "" {
			items.BindingDigest = digest
			return
		}
		reason = metrics.LblBindingDigestRestoreFail
	}
	metrics.BindingDigestEmptyCounter.WithLabelValues(reason).Inc()
}

// bindingDigestEmptyReason classifies why the binding digest stays empty for
// this slow-logged statement: it delegates to
// resolveBindingDigestInput, which also resolves EXECUTE on demand. An empty
// return value means the statement is DML-shaped (EXPLAIN prefix stripped)
// and eligible for the on-demand computation — an empty digest after that
// point is a restore failure. The EXPLAIN edge with an empty Text() (no bind
// record wanted) also lands on restore-fail: the normalizer returns "" for
// it and the distinction carries no operational difference.
func bindingDigestEmptyReason(stmtNode ast.StmtNode, sessVars *variable.SessionVars) string {
	_, reason := resolveBindingDigestInput(stmtNode, sessVars)
	return reason
}

// SetSlowLogItems fills the remaining fields of SlowQueryLogItems after SQL execution.
func SetSlowLogItems(a *ExecStmt, txnTS uint64, hasMoreResults bool, items *variable.SlowQueryLogItems) {
	if items == nil {
		return
	}
	CompleteSlowLogItemsForRules(a.GoCtx, a.Ctx.GetSessionVars(), items)
	lazyFillBindingDigest(a.StmtNode, a.Ctx.GetSessionVars(), items)

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
	items.SQL = formatSlowLogSQL(a.GetTextToLog(true))
	items.IndexNames = indexNames
	items.Plan = getPlanTree(stmtCtx)
	items.BinaryPlan = binaryPlan
	items.Prepared = a.isPreparedStmt
	items.HasMoreResults = hasMoreResults
	items.PlanFromCache = sessVars.FoundInPlanCache
	items.PlanFromBinding = sessVars.FoundInBinding
	items.RewriteInfo = sessVars.RewritePhaseInfo
	items.ResultRows = GetResultRowsCount(stmtCtx, a.Plan)
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
	if sessVars.ConnectionInfo != nil && len(sessVars.ConnectionInfo.Attributes) > 0 {
		items.SessionConnectAttrs = sessVars.ConnectionInfo.Attributes
	}

	if a.retryCount > 0 {
		items.ExecRetryTime = items.TimeTotal - sessVars.DurationParse - sessVars.DurationCompile - time.Since(a.retryStartTime)
	}
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok && sessVars.PrevStmt != nil {
		items.PrevStmt = sessVars.PrevStmt.String()
	}
}

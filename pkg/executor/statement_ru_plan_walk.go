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
	"math"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannercoreutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type statementRUFinalOutcome uint32

type statementRUOperatorState uint8

const (
	statementRUFinalOutcomeUnknown statementRUFinalOutcome = iota
	statementRUFinalOutcomeSuccess
	statementRUFinalOutcomeFailure
)

const (
	statementRUOperatorUnknown statementRUOperatorState = iota
	statementRUOperatorComplete
	statementRUOperatorUnsupported
	statementRUOperatorInvalid
)

// statementRUOperatorResult is a value-only occurrence result. Complete means
// that this supported occurrence was calculated from the evidence currently
// visible at finalization; it does not claim producer-side evidence coverage.
type statementRUOperatorResult struct {
	state      statementRUOperatorState
	outputRows int64
}

// statementRUOwner owns the first-record-wins outcome and the first terminal
// finalization for one ExecStmt. Its calculation setup is released when the
// shared finishOnce is consumed.
type statementRUOwner struct {
	finishOnce   sync.Once
	finalOutcome atomic.Uint32
	rootEOF      atomic.Bool
	// Install-time snapshots reject transient restricted/cursor classifications
	// that may be restored before a delayed result-set terminal.
	restrictedSQLAtInstall bool
	cursorAtInstall        bool
	calculationSetup       statementRUCalculationSetup
}

func newStatementRUOwner(stmt *ExecStmt) *statementRUOwner {
	owner := &statementRUOwner{}
	if stmt == nil || stmt.Ctx == nil {
		return owner
	}
	sessVars := stmt.Ctx.GetSessionVars()
	if sessVars == nil {
		return owner
	}
	owner.restrictedSQLAtInstall = sessVars.InRestrictedSQL
	owner.cursorAtInstall = sessVars.HasStatusFlag(mysql.ServerStatusCursorExists)
	return owner
}

// RecordStatementRUFinalOutcome publishes the final session outcome for the
// statement-local RU finalization. It remains a nil-owner no-op outside the
// current calculation policy and tests. The first record wins and a recorded
// failure consumes the owner immediately; success must be recorded before the
// existing executor terminal can finalize RU.
func (a *ExecStmt) RecordStatementRUFinalOutcome(success bool) {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}

	outcome := statementRUFinalOutcomeFailure
	if success {
		outcome = statementRUFinalOutcomeSuccess
	}
	recorded := owner.finalOutcome.CompareAndSwap(
		uint32(statementRUFinalOutcomeUnknown),
		uint32(outcome),
	)
	if recorded && !success {
		a.abortStatementRU()
	}
}

// abortStatementRU consumes the owner without running RU calculation.
// RU-only failure paths use it instead of mutating legacy lastErrs or running
// a full executor terminal solely for this RU layer.
func (a *ExecStmt) abortStatementRU() {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}
	owner.finishOnce.Do(func() {
		owner.calculationSetup = statementRUCalculationSetup{}
	})
}

// recordStatementRURootEOF records that the root executor returned an empty
// chunk. A successful statement does not imply this: a caller can
// close a RecordSet cleanly before consuming all rows, for example when writing
// rows to the client fails. Publishing that partial work as the statement's RU
// would undercount, so RU v3 metric publication requires this independent bit.
func (a *ExecStmt) recordStatementRURootEOF() {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}
	owner.rootEOF.Store(true)
}

// TODO: when the statement-RU failure metric lands, count the bounded failure
// reasons at these fail-closed exits and publisher recoveries. Until then there
// is deliberately no recorder-shaped no-op API.
func (a *ExecStmt) finishStatementRU(terminalErr error) {
	owner := a.statementRUOwner
	if owner == nil {
		return
	}

	var finalized statementRUFinalizedSnapshot
	publishFinalized := false
	owner.finishOnce.Do(func() {
		calculationSetup := owner.calculationSetup
		owner.calculationSetup = statementRUCalculationSetup{}

		// The entire hook is fail-closed. A panic in eligibility, flat-plan
		// lookup/generation, or calculation must neither make the owner retryable
		// nor interrupt existing terminal bookkeeping.
		defer func() {
			_ = recover()
		}()

		if statementRUFinalOutcome(owner.finalOutcome.Load()) != statementRUFinalOutcomeSuccess || terminalErr != nil {
			return
		}
		// a.Plan remains the statement eligibility guard even though the flat-plan
		// view below comes from StatementContext.
		if a.Ctx == nil || a.Plan == nil {
			return
		}

		sessVars := a.Ctx.GetSessionVars()
		// The snapshots catch eligibility that disappeared before terminal; the
		// live checks catch a classification entered after owner installation.
		if sessVars == nil || owner.restrictedSQLAtInstall || owner.cursorAtInstall ||
			sessVars.InRestrictedSQL || sessVars.HasStatusFlag(mysql.ServerStatusCursorExists) {
			return
		}
		flat := getFlatPlan(sessVars.StmtCtx)
		if flat == nil {
			return
		}

		// The fresh-session slice must use a flat plan rooted at this ExecStmt.
		// General flat-plan generation identity remains outside this layer.
		if len(flat.Main) == 0 || flat.Main[0] == nil || flat.Main[0].Origin != a.Plan {
			return
		}
		finalized, publishFinalized = calculateStatementRU(
			flat,
			sessVars.StmtCtx.RuntimeStatsColl,
			sessVars.RUV2Metrics,
			calculationSetup,
			owner.rootEOF.Load(),
		)
	})
	if publishFinalized {
		publishStatementRUFinalizedSnapshot(a, finalized)
	}
}

// calculateStatementRU directly walks borrowed flat-plan occurrences without
// retaining or mutating them and builds one value-only result. Scan evidence
// belongs to the Reader request component that produced it, so it is
// accumulated at the Reader occurrence; pushed operators and scans do not
// shuttle or consume a second copy.
func calculateStatementRU(
	flat *plannercore.FlatPhysicalPlan,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	metrics *execdetails.RUV2Metrics,
	setup statementRUCalculationSetup,
	rootEOF bool,
) (statementRUFinalizedSnapshot, bool) {
	if !rootEOF || flat == nil || len(flat.Main) == 0 || len(flat.CTEs) != 0 || len(flat.ScalarSubQueries) != 0 {
		return statementRUFinalizedSnapshot{}, false
	}
	calculator := newStatementRUCalculator(setup)
	// Transport bytes are statement evidence in both the current producer and
	// the demo model. Read them once; do not attribute the same aggregate to
	// every Reader occurrence. Missing evidence contributes zero to the
	// best-effort ResultOnly value.
	if metrics != nil && !metrics.Bypass() {
		netBytes := metrics.TiKVCoprocessorResponseBytes()
		if netBytes < 0 {
			return statementRUFinalizedSnapshot{}, false
		}
		calculator.units.NetBytes = float64(netBytes)
	}

	planResult := calculateStatementRUPlan(
		flat.Main,
		0,
		runtimeStatsColl,
		&calculator,
	)
	if planResult.state != statementRUOperatorComplete {
		return statementRUFinalizedSnapshot{}, false
	}
	return calculator.finalize()
}

// calculateStatementRUPlan evaluates one subtree from a canonical
// preorder-serialized tree, visiting children before their parent. ChildrenIdx
// is the only production edge routing. Each parent receives its direct-child
// value result, so later operators can consume child rows without changing the
// traversal framework. FlatPhysicalPlan owns the serialized layout; this
// calculation follows only its explicit child edges.
func calculateStatementRUPlan(
	tree plannercore.FlatPlanTree,
	operatorIndex int,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	calculator *statementRUCalculator,
) statementRUOperatorResult {
	return calculateStatementRUPlanChildFirst(
		tree,
		operatorIndex,
		runtimeStatsColl,
		calculator,
		len(tree),
	)
}

func calculateStatementRUPlanChildFirst(
	tree plannercore.FlatPlanTree,
	operatorIndex int,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	calculator *statementRUCalculator,
	remainingDepth int,
) statementRUOperatorResult {
	if operatorIndex < 0 || operatorIndex >= len(tree) || calculator == nil || remainingDepth <= 0 {
		return statementRUOperatorResult{state: statementRUOperatorInvalid}
	}
	operator := tree[operatorIndex]
	if operator == nil || operator.Origin == nil {
		return statementRUOperatorResult{state: statementRUOperatorInvalid}
	}

	children := make([]statementRUOperatorResult, len(operator.ChildrenIdx))
	childState := statementRUOperatorComplete
	for childOrdinal, childIndex := range operator.ChildrenIdx {
		children[childOrdinal] = calculateStatementRUPlanChildFirst(
			tree,
			childIndex,
			runtimeStatsColl,
			calculator,
			remainingDepth-1,
		)
		childState = mergeStatementRUOperatorState(childState, children[childOrdinal].state)
	}
	if childState != statementRUOperatorComplete {
		return statementRUOperatorResult{state: childState}
	}

	outputRows := int64(0)
	if runtimeStatsColl != nil {
		if operator.IsRoot {
			outputRows = runtimeStatsColl.GetPlanActRows(operator.Origin.ID())
		} else {
			_, outputRows = runtimeStatsColl.GetCopCountAndRows(operator.Origin.ID())
		}
	}
	if outputRows < 0 {
		return statementRUOperatorResult{state: statementRUOperatorInvalid}
	}

	switch origin := operator.Origin.(type) {
	case *physicalop.PhysicalTableReader:
		// Scan-byte accounting is performed at Reader boundaries. A TableReader
		// contributes scan evidence from its single TiKV table request exactly once.
		if !operator.IsRoot || origin.StoreType != kv.TiKV || origin.ReadReqType != physicalop.Cop ||
			origin.TablePlan == nil || len(operator.ChildrenIdx) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if state := collectStatementRUReaderScanBytes(
			tree, operator, runtimeStatsColl, calculator, []base.Plan{origin.TablePlan},
		); state != statementRUOperatorComplete {
			return statementRUOperatorResult{state: state}
		}
	case *physicalop.PhysicalIndexReader:
		// Scan-byte accounting is performed at Reader boundaries. An IndexReader
		// contributes scan evidence from its single TiKV index request exactly once.
		if !operator.IsRoot || origin.IndexPlan == nil || len(operator.ChildrenIdx) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if state := collectStatementRUReaderScanBytes(
			tree, operator, runtimeStatsColl, calculator, []base.Plan{origin.IndexPlan},
		); state != statementRUOperatorComplete {
			return statementRUOperatorResult{state: state}
		}
	case *physicalop.PhysicalIndexLookUpReader:
		// An IndexLookUpReader owns distinct index/build and table/probe requests.
		// Each request-root contribution is collected exactly once at this boundary.
		if !operator.IsRoot || origin.IndexLookUpPushDown ||
			origin.IndexPlan == nil || origin.TablePlan == nil || origin.IndexPlan == origin.TablePlan ||
			len(operator.ChildrenIdx) != 2 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		indexChild := tree[operator.ChildrenIdx[0]]
		tableChild := tree[operator.ChildrenIdx[1]]
		if indexChild == nil || tableChild == nil ||
			indexChild.Label != plannercore.BuildSide || indexChild.IsINLProbeChild ||
			tableChild.Label != plannercore.ProbeSide || !tableChild.IsINLProbeChild {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if state := collectStatementRUReaderScanBytes(
			tree,
			operator,
			runtimeStatsColl,
			calculator,
			[]base.Plan{origin.IndexPlan, origin.TablePlan},
		); state != statementRUOperatorComplete {
			return statementRUOperatorResult{state: state}
		}
	case *physicalop.PhysicalSelection:
		// CPU work for Selection is defined as the child output-row count
		// multiplied by the number of conditions evaluated per row.
		if !statementRUOperatorRunsAtSupportedSite(operator) || len(children) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if !addStatementRUCPUWork(calculator, float64(children[0].outputRows)*float64(len(origin.Conditions))) {
			return statementRUOperatorResult{state: statementRUOperatorInvalid}
		}
	case *physicalop.PhysicalSort:
		// For n > 0, CPU work for Sort is defined as n * log2(max(n, 2)), where n
		// is the child output-row count. Only root Sort is supported.
		if !operator.IsRoot || len(children) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		statementRUAssertOrderingMaterialized(origin.ByItems)
		if !addStatementRUCPUWork(calculator, statementRUSortWork(children[0].outputRows, uint64(children[0].outputRows))) {
			return statementRUOperatorResult{state: statementRUOperatorInvalid}
		}
	case *physicalop.PhysicalTopN:
		// For n > 0 and k > 0, CPU work for TopN is defined as
		// n * log2(max(min(n, k), 2)). Root k is Offset + Count; for pushed TopN,
		// the planner has already folded Offset into Count.
		if !statementRUOperatorRunsAtSupportedSite(operator) || len(children) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		statementRUAssertOrderingMaterialized(origin.ByItems)
		var retainedRows uint64
		if operator.IsRoot {
			if origin.Count == 0 {
				retainedRows = 0
			} else {
				if origin.Offset > math.MaxUint64-origin.Count {
					return statementRUOperatorResult{state: statementRUOperatorInvalid}
				}
				retainedRows = origin.Offset + origin.Count
			}
		} else {
			if origin.Offset != 0 {
				return statementRUOperatorResult{state: statementRUOperatorUnsupported}
			}
			retainedRows = origin.Count
		}
		if !addStatementRUCPUWork(calculator, statementRUSortWork(children[0].outputRows, retainedRows)) {
			return statementRUOperatorResult{state: statementRUOperatorInvalid}
		}
	case *physicalop.PhysicalLimit:
		// CPU work for Limit is defined as its child output-row count.
		if !statementRUOperatorRunsAtSupportedSite(operator) || len(children) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if !addStatementRUCPUWork(calculator, float64(children[0].outputRows)) {
			return statementRUOperatorResult{state: statementRUOperatorInvalid}
		}
	case *physicalop.PhysicalTableScan, *physicalop.PhysicalIndexScan:
		// TableScan and IndexScan do not contribute units directly. Their scan
		// evidence is accounted for by the owning Reader boundary.
		if operator.IsRoot || len(operator.ChildrenIdx) != 0 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if operator.StoreType != kv.TiKV || operator.ReqType != physicalop.Cop {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	default:
		// Operators not listed above, including Projection, are outside the
		// supported statement-RU model and therefore fail closed.
		return statementRUOperatorResult{state: statementRUOperatorUnsupported}
	}

	return statementRUOperatorResult{state: statementRUOperatorComplete, outputRows: outputRows}
}

func mergeStatementRUOperatorState(left, right statementRUOperatorState) statementRUOperatorState {
	if left == statementRUOperatorInvalid || right == statementRUOperatorInvalid {
		return statementRUOperatorInvalid
	}
	if left != statementRUOperatorComplete || right != statementRUOperatorComplete {
		return statementRUOperatorUnsupported
	}
	return statementRUOperatorComplete
}

func statementRUOperatorRunsAtSupportedSite(operator *plannercore.FlatOperator) bool {
	return operator.IsRoot || (operator.StoreType == kv.TiKV && operator.ReqType == physicalop.Cop)
}

func collectStatementRUReaderScanBytes(
	tree plannercore.FlatPlanTree,
	operator *plannercore.FlatOperator,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	calculator *statementRUCalculator,
	requestRoots []base.Plan,
) statementRUOperatorState {
	if len(operator.ChildrenIdx) != len(requestRoots) {
		return statementRUOperatorUnsupported
	}
	for branchOrdinal, requestRoot := range requestRoots {
		childIndex := operator.ChildrenIdx[branchOrdinal]
		if childIndex < 0 || childIndex >= len(tree) || tree[childIndex] == nil {
			return statementRUOperatorInvalid
		}
		child := tree[childIndex]
		if child.Origin != requestRoot || child.IsRoot ||
			child.StoreType != kv.TiKV || child.ReqType != physicalop.Cop {
			return statementRUOperatorUnsupported
		}
		if runtimeStatsColl == nil {
			continue
		}
		detail, found := runtimeStatsColl.GetCopScanDetail(requestRoot.ID())
		if !found {
			continue
		}
		scanEvidence := classifyStatementRUScanEvidence(
			detail.TotalKeys,
			detail.ProcessedKeys,
			detail.ProcessedKeysSize,
		)
		switch scanEvidence.state {
		case statementRUScanEvidenceUnavailable:
			continue
		case statementRUScanEvidenceValid:
			if !addStatementRUScanBytes(calculator, scanEvidence.scanBytes) {
				return statementRUOperatorInvalid
			}
		default:
			return statementRUOperatorInvalid
		}
	}
	return statementRUOperatorComplete
}

func statementRUSortWork(inputRows int64, retainedRows uint64) float64 {
	if inputRows <= 0 || retainedRows == 0 {
		return 0
	}
	rowsToRetain := math.Min(float64(inputRows), float64(retainedRows))
	return float64(inputRows) * math.Log2(math.Max(rowsToRetain, 2))
}

// statementRUAssertOrderingMaterialized checks a planner invariant in intest
// builds without making it another production RU eligibility condition.
func statementRUAssertOrderingMaterialized(byItems []*plannercoreutil.ByItems) {
	intest.AssertFunc(func() bool {
		for _, item := range byItems {
			if item == nil || item.Expr == nil {
				return false
			}
			if _, scalar := item.Expr.(*expression.ScalarFunction); scalar {
				return false
			}
		}
		return true
	}, "statement RU expects Sort/TopN ordering expressions to be materialized")
}

func addStatementRUCPUWork(calculator *statementRUCalculator, work float64) bool {
	if calculator == nil || work < 0 || math.IsNaN(work) || math.IsInf(work, 0) {
		return false
	}
	calculator.units.CPUWork += work
	return !math.IsInf(calculator.units.CPUWork, 0)
}

func addStatementRUScanBytes(calculator *statementRUCalculator, scanBytes float64) bool {
	if calculator == nil || scanBytes < 0 || math.IsNaN(scanBytes) || math.IsInf(scanBytes, 0) {
		return false
	}
	calculator.units.ScanBytes += scanBytes
	return !math.IsInf(calculator.units.ScanBytes, 0)
}

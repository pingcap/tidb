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
	state              statementRUOperatorState
	outputRows         int64
	outputRowsObserved bool
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
		// General flat-plan generation identity is not statement-RU evidence.
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
	return calculateStatementRUInternal(flat, runtimeStatsColl, metrics, setup, rootEOF, nil)
}

func calculateStatementRUWithOperators(
	flat *plannercore.FlatPhysicalPlan,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	metrics *execdetails.RUV2Metrics,
	setup statementRUCalculationSetup,
	rootEOF bool,
) (statementRUFinalizedSnapshot, *plannercore.ExplainRUResult, bool) {
	operatorRUs := plannercore.NewExplainRUResult(flat)
	finalized, ok := calculateStatementRUInternal(flat, runtimeStatsColl, metrics, setup, rootEOF, operatorRUs)
	if !ok {
		return statementRUFinalizedSnapshot{}, nil, false
	}
	operatorRUs.TotalRU = finalized.result.TotalRU
	return finalized, operatorRUs, true
}

// calculateStatementRUInternal shares one forest traversal between formal
// statement-RU reporting and synchronous RU EXPLAIN.
//
// operatorRUs is nil on the formal path: that path constructs no
// ExplainRUResult and skips the per-operator RU projection and writes.
//
// The recursion still threads the optional output slice and snapshots
// calculator units before each subtree and operator, so ResultOnly's
// zero-allocation contract covers only the EXPLAIN result shapes; it does
// not mean that RU EXPLAIN instrumentation adds zero CPU work to formal
// reporting.
func calculateStatementRUInternal(
	flat *plannercore.FlatPhysicalPlan,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	metrics *execdetails.RUV2Metrics,
	setup statementRUCalculationSetup,
	rootEOF bool,
	operatorRUs *plannercore.ExplainRUResult,
) (statementRUFinalizedSnapshot, bool) {
	if !rootEOF || flat == nil || len(flat.Main) == 0 {
		return statementRUFinalizedSnapshot{}, false
	}
	calculator := newStatementRUCalculator(setup)
	// Transport bytes are statement-owned evidence. Read them once; do not
	// attribute the same aggregate to every Reader occurrence. Missing evidence
	// contributes zero to the best-effort ResultOnly value.
	if metrics != nil && !metrics.Bypass() {
		netBytes := metrics.TiKVCoprocessorResponseBytes()
		if netBytes < 0 {
			return statementRUFinalizedSnapshot{}, false
		}
		calculator.units.NetBytes = float64(netBytes)
	}

	// The forest consumes only currently visible typed evidence. Response-level
	// total-key-size, repeated Sort/TopN lifecycle, and recursive/Apply execution-
	// opportunity evidence are not all available yet, so the finalized calibration
	// remains Incomplete and the result is neither exact nor a mathematical upper
	// or lower bound. Invalid values and malformed tree structure still fail closed.
	mainRootUnits := calculator.units
	mainResult := calculateStatementRUPlan(
		flat.Main,
		0,
		runtimeStatsColl,
		&calculator,
		mainRootUnits,
		statementRUExplainTree(operatorRUs, statementRUForestMain, 0),
	)
	if mainResult.state != statementRUOperatorComplete {
		return statementRUFinalizedSnapshot{}, false
	}
	// FlattenPhysicalPlan deduplicates definitions by CTE.IDForStorage. Walking
	// each tree once therefore charges one shared producer once; consumers stay
	// in their owning trees and keep their own output-row evidence for parents.
	for treeOrdinal, tree := range flat.CTEs {
		result := calculateStatementRUPlan(
			tree,
			0,
			runtimeStatsColl,
			&calculator,
			statementRURawUnits{},
			statementRUExplainTree(operatorRUs, statementRUForestCTE, treeOrdinal),
		)
		if result.state != statementRUOperatorComplete {
			return statementRUFinalizedSnapshot{}, false
		}
	}
	for treeOrdinal, tree := range flat.ScalarSubQueries {
		result := calculateStatementRUPlan(
			tree,
			0,
			runtimeStatsColl,
			&calculator,
			statementRURawUnits{},
			statementRUExplainTree(operatorRUs, statementRUForestScalarSubQuery, treeOrdinal),
		)
		if result.state != statementRUOperatorComplete {
			return statementRUFinalizedSnapshot{}, false
		}
	}
	finalized, ok := calculator.finalize()
	if !ok {
		return statementRUFinalizedSnapshot{}, false
	}
	return finalized, true
}

type statementRUForestKind uint8

const (
	statementRUForestMain statementRUForestKind = iota
	statementRUForestCTE
	statementRUForestScalarSubQuery
)

func statementRUExplainTree(
	result *plannercore.ExplainRUResult,
	kind statementRUForestKind,
	treeOrdinal int,
) []plannercore.ExplainRUOperatorResult {
	if result == nil {
		return nil
	}
	switch kind {
	case statementRUForestMain:
		return result.Main
	case statementRUForestCTE:
		if treeOrdinal >= 0 && treeOrdinal < len(result.CTEs) {
			return result.CTEs[treeOrdinal]
		}
	case statementRUForestScalarSubQuery:
		if treeOrdinal >= 0 && treeOrdinal < len(result.ScalarSubQueries) {
			return result.ScalarSubQueries[treeOrdinal]
		}
	}
	return nil
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
	rootOwnedUnits statementRURawUnits,
	operatorRUs []plannercore.ExplainRUOperatorResult,
) statementRUOperatorResult {
	if !validateStatementRUFlatTree(tree) {
		return statementRUOperatorResult{state: statementRUOperatorInvalid}
	}
	return calculateStatementRUPlanChildFirst(
		tree,
		operatorIndex,
		runtimeStatsColl,
		calculator,
		len(tree),
		rootOwnedUnits,
		operatorRUs,
	)
}

// validateStatementRUFlatTree verifies the canonical depth-first shape without
// allocating occurrence state on the ResultOnly path. Every child subtree must
// begin exactly where the preceding subtree ends, so duplicate references,
// backward/cyclic edges, and unreachable entries all fail closed.
func validateStatementRUFlatTree(tree plannercore.FlatPlanTree) bool {
	if len(tree) == 0 {
		return false
	}
	next, ok := validateStatementRUFlatSubtree(tree, 0, len(tree))
	return ok && next == len(tree)
}

func validateStatementRUFlatSubtree(
	tree plannercore.FlatPlanTree,
	operatorIndex int,
	remainingDepth int,
) (int, bool) {
	if operatorIndex < 0 || operatorIndex >= len(tree) || remainingDepth <= 0 {
		return 0, false
	}
	operator := tree[operatorIndex]
	if operator == nil || operator.Origin == nil {
		return 0, false
	}
	next := operatorIndex + 1
	for _, childIndex := range operator.ChildrenIdx {
		if childIndex != next || childIndex <= operatorIndex {
			return 0, false
		}
		var ok bool
		next, ok = validateStatementRUFlatSubtree(tree, childIndex, remainingDepth-1)
		if !ok {
			return 0, false
		}
	}
	return next, true
}

func calculateStatementRUPlanChildFirst(
	tree plannercore.FlatPlanTree,
	operatorIndex int,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	calculator *statementRUCalculator,
	remainingDepth int,
	rootOwnedUnits statementRURawUnits,
	operatorRUs []plannercore.ExplainRUOperatorResult,
) statementRUOperatorResult {
	if operatorIndex < 0 || operatorIndex >= len(tree) || calculator == nil || remainingDepth <= 0 {
		return statementRUOperatorResult{state: statementRUOperatorInvalid}
	}
	operator := tree[operatorIndex]
	if operator == nil || operator.Origin == nil {
		return statementRUOperatorResult{state: statementRUOperatorInvalid}
	}
	beforeSubtree := calculator.units
	children := make([]statementRUOperatorResult, len(operator.ChildrenIdx))
	childState := statementRUOperatorComplete
	for childOrdinal, childIndex := range operator.ChildrenIdx {
		children[childOrdinal] = calculateStatementRUPlanChildFirst(
			tree,
			childIndex,
			runtimeStatsColl,
			calculator,
			remainingDepth-1,
			rootOwnedUnits,
			operatorRUs,
		)
		childState = mergeStatementRUOperatorState(childState, children[childOrdinal].state)
	}
	if childState != statementRUOperatorComplete {
		return statementRUOperatorResult{state: childState}
	}

	outputRows := int64(0)
	outputRowsObserved := false
	// Typed snapshots preserve observed zero through outputRowsObserved. Missing
	// evidence remains an unobserved zero for best-effort linear/wrapper formulas,
	// while operators that require proof of execution explicitly test Observed.
	// Negative or otherwise invalid snapshots fail closed. Because the former two
	// cases are not interchangeable, every successful forest remains calibration-
	// Incomplete until direct opportunity coverage is available.
	if runtimeStatsColl != nil {
		if operator.IsRoot {
			snapshot := runtimeStatsColl.GetRootRowsSnapshot(operator.Origin.ID())
			if snapshot.Invalid() {
				return statementRUOperatorResult{state: statementRUOperatorInvalid}
			}
			outputRows = snapshot.Rows
			outputRowsObserved = snapshot.Observed()
		} else {
			snapshot := runtimeStatsColl.GetCopRowsSnapshot(operator.Origin.ID())
			if snapshot.Invalid {
				return statementRUOperatorResult{state: statementRUOperatorInvalid}
			}
			outputRows = snapshot.Rows
			// Missing summary slots remain marked by Complete, but do not discard
			// rows from other valid responses for the same cop occurrence.
			outputRowsObserved = snapshot.Observed()
		}
	}
	if outputRows < 0 {
		return statementRUOperatorResult{state: statementRUOperatorInvalid}
	}

	beforeOperator := calculator.units

	switch origin := operator.Origin.(type) {
	case *plannercore.Analyze:
		// ANALYZE can issue independent requests for indexes, partitions, and
		// split ranges. Its scan-byte estimate is accumulated once per logical
		// request before their nonlinear scan-detail fields are flattened.
		if !operator.IsRoot || len(operator.ChildrenIdx) != 0 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if runtimeStatsColl != nil {
			scanBytes, found := runtimeStatsColl.GetAnalyzeScanBytes(origin.ID())
			if found && !addStatementRUScanBytes(calculator, scanBytes) {
				return statementRUOperatorResult{state: statementRUOperatorInvalid}
			}
		}
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
	case *physicalop.CTEDefinition:
		// A CTEDefinition is the zero-self-RU ownership wrapper for one shared
		// materialization. FlattenPhysicalPlan emits it once per IDForStorage;
		// seed and recursive descendants already carry runtime evidence merged
		// across actual rounds and must not be multiplied by consumer count or a
		// separately inferred recursion count. Until direct phase/opportunity
		// evidence exists, an intentionally skipped phase and missing evidence
		// can both appear as zero; only a zero caused by missing evidence may
		// undercount actual work. Forest calibration is therefore Incomplete and
		// the result is not a mathematical bound.
		if !operator.IsRoot || len(children) < 1 || len(children) > 2 ||
			tree[operator.ChildrenIdx[0]].Label != plannercore.SeedPart ||
			(len(children) == 2 && tree[operator.ChildrenIdx[1]].Label != plannercore.RecursivePart) {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	case *physicalop.PhysicalCTE:
		// The consumer owns no producer work. Its observed rows remain the
		// occurrence output consumed by a parent Selection/Join/Aggregation.
		if !operator.IsRoot || len(children) != 0 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	case *physicalop.PhysicalCTETable:
		// A recursive CTE-table read is an orchestration leaf with zero modeled
		// self RU; its actual output rows still feed the recursive parent tree.
		if !operator.IsRoot || len(children) != 0 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	case *plannercore.ScalarSubqueryEvalCtx:
		// ScalarSubqueryEvalCtx only owns the independent-tree boundary. The
		// physical child was executed during optimization and owns all modeled
		// work; it does not need another main-record-set EOF.
		if !operator.IsRoot || len(children) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	case *physicalop.PhysicalProjection:
		// Projection CPU work is one expression slot per actual input row. This
		// is linear across repeated executions, so merged child rows are the
		// sufficient statistic for the current model.
		if !statementRUOperatorRunsAtSupportedSite(operator) || len(children) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		expressionCount := len(origin.Exprs)
		if !addStatementRUCPUWork(
			calculator,
			float64(children[0].outputRows)*float64(expressionCount),
		) {
			return statementRUOperatorResult{state: statementRUOperatorInvalid}
		}
	case *physicalop.PhysicalMaxOneRow:
		// MaxOneRow examines the direct child's actual output. It must not use
		// the NULL row synthesized for an empty scalar subquery as input work.
		if !operator.IsRoot || len(children) != 1 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
		if !addStatementRUCPUWork(calculator, float64(children[0].outputRows)) {
			return statementRUOperatorResult{state: statementRUOperatorInvalid}
		}
	case *physicalop.PhysicalTableDual:
		// TableDual has zero modeled self RU. Runtime evidence, rather than the
		// optimizer estimate, distinguishes its actual zero/one output for a
		// parent operator.
		if !operator.IsRoot || len(children) != 0 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	case *physicalop.PhysicalUnionAll:
		// UnionAll is a zero-self-RU orchestration wrapper; each executed child
		// subtree contributes independently.
		if !operator.IsRoot || len(children) == 0 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	case *physicalop.PhysicalApply:
		// Apply is a zero-self-RU orchestration wrapper in this model. Child
		// runtime rows accumulate repeated inner work, but the current evidence
		// cannot distinguish cache/filter skips from missing attempts and cannot
		// reconstruct repeated nonlinear Sort/TopN work. The best-effort result
		// therefore may undercount, remains Incomplete, and must not be treated as
		// exact or as a mathematical bound.
		if !operator.IsRoot || len(children) != 2 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
		}
	case *physicalop.PhysicalSequence:
		// Sequence orders CTE orchestration and the main query but owns no modeled
		// self RU. Its explicit children remain responsible for their own work.
		if !operator.IsRoot || len(children) == 0 {
			return statementRUOperatorResult{state: statementRUOperatorUnsupported}
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
		// is the child output-row count. Only root Sort is supported. When one
		// occurrence is reopened by recursive CTE or Apply, current row evidence
		// merges the executions before this nonlinear formula; superadditivity can
		// overestimate their separately accumulated work until lifecycle-ready
		// ordering evidence is available.
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
		// the planner has already folded Offset into Count. Repeated executions
		// are currently merged before this nonlinear formula and can overestimate
		// the sum of per-lifecycle work.
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
	case *physicalop.PhysicalHashJoin, *physicalop.PhysicalMergeJoin,
		*physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexHashJoin,
		*physicalop.PhysicalIndexMergeJoin:
		state := collectStatementRUJoinUnits(
			operator, children, outputRows, outputRowsObserved, runtimeStatsColl, calculator,
		)
		if state != statementRUOperatorComplete {
			return statementRUOperatorResult{state: state}
		}
	case *physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg:
		state := collectStatementRUAggregationUnits(
			operator, children, outputRows, outputRowsObserved, runtimeStatsColl, calculator,
		)
		if state != statementRUOperatorComplete {
			return statementRUOperatorResult{state: state}
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
		// Operators not listed above are outside the
		// supported statement-RU model and therefore fail closed.
		return statementRUOperatorResult{state: statementRUOperatorUnsupported}
	}

	if operatorRUs != nil {
		if len(operatorRUs) != len(tree) {
			return statementRUOperatorResult{state: statementRUOperatorInvalid}
		}
		selfUnits := subtractStatementRURawUnits(calculator.units, beforeOperator)
		cumUnits := subtractStatementRURawUnits(calculator.units, beforeSubtree)
		if operatorIndex == 0 {
			selfUnits = addStatementRURawUnits(selfUnits, rootOwnedUnits)
			cumUnits = addStatementRURawUnits(cumUnits, rootOwnedUnits)
		}
		operatorRUs[operatorIndex].SelfRU = calculateStatementRUResultOnly(selfUnits).TotalRU
		operatorRUs[operatorIndex].CumRU = calculateStatementRUResultOnly(cumUnits).TotalRU
	}

	return statementRUOperatorResult{
		state:              statementRUOperatorComplete,
		outputRows:         outputRows,
		outputRowsObserved: outputRowsObserved,
	}
}

// collectStatementRUJoinUnits charges one supported root Join occurrence using
// these formulas:
//
//	CPUWork = (left child rows + right child rows) * expression count
//	JoinOutputRows = output rows
//	HashStateRows = completed HashJoin lookup-state rows
//
// statementRUJoinContractForPlan defines the expression count for each Join
// subtype. Cop/TiFlash, FullOuter, and incomplete runtime evidence fail closed.
func collectStatementRUJoinUnits(
	operator *plannercore.FlatOperator,
	children []statementRUOperatorResult,
	outputRows int64,
	outputRowsObserved bool,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	calculator *statementRUCalculator,
) statementRUOperatorState {
	delta := statementRUCalculator{}
	if !operator.IsRoot || !children[0].outputRowsObserved || !children[1].outputRowsObserved || !outputRowsObserved {
		return statementRUOperatorUnsupported
	}
	contract, ok := statementRUJoinContractForPlan(operator.Origin)
	if !ok {
		return statementRUOperatorUnsupported
	}
	inputRows := float64(children[0].outputRows) + float64(children[1].outputRows)
	if !addStatementRUCPUWork(&delta, inputRows*float64(contract.expressionCount)) ||
		!addStatementRUJoinOutputRows(&delta, float64(outputRows)) {
		return statementRUOperatorInvalid
	}
	if contract.hashState {
		if runtimeStatsColl == nil {
			return statementRUOperatorUnsupported
		}
		snapshot, found := runtimeStatsColl.GetRootHashStateRowsSnapshot(operator.Origin.ID())
		if !found {
			return statementRUOperatorUnsupported
		}
		if !snapshot.Complete() {
			if snapshot.Invalid() {
				return statementRUOperatorInvalid
			}
			return statementRUOperatorUnsupported
		}
		if !addStatementRUHashStateRows(&delta, float64(snapshot.Rows)) {
			return statementRUOperatorInvalid
		}
	}
	if !mergeStatementRUUnitDelta(calculator, delta.units) {
		return statementRUOperatorInvalid
	}
	return statementRUOperatorComplete
}

type statementRUJoinContract struct {
	expressionCount int
	hashState       bool
}

// statementRUJoinContractForPlan is the single support matrix for root Join
// accounting. Its expression count formulas are:
//
//	HashJoin       = EqualConditions + NAEqualConditions + LeftConditions + RightConditions + OtherConditions
//	MergeJoin      = CompareFuncs + LeftConditions + RightConditions + OtherConditions
//	IndexJoin      = OuterJoinKeys + LeftConditions + RightConditions + OtherConditions + CompareFilters.OpType
//	IndexHashJoin  = OuterHashKeys + LeftConditions + RightConditions + OtherConditions + CompareFilters.OpType
//	IndexMergeJoin = CompareFuncs + OuterCompareFuncs + LeftConditions + RightConditions + OtherConditions + CompareFilters.OpType
func statementRUJoinContractForPlan(plan base.Plan) (statementRUJoinContract, bool) {
	contract := statementRUJoinContract{}
	compareFilterCount := func(filters *physicalop.ColWithCmpFuncManager) int {
		if filters == nil {
			return 0
		}
		return len(filters.OpType)
	}
	var joinType base.JoinType
	switch join := plan.(type) {
	case *physicalop.PhysicalHashJoin:
		joinType = join.JoinType
		contract.hashState = true
		contract.expressionCount = len(join.EqualConditions) + len(join.NAEqualConditions) +
			len(join.LeftConditions) + len(join.RightConditions) + len(join.OtherConditions)
	case *physicalop.PhysicalMergeJoin:
		joinType = join.JoinType
		contract.expressionCount = len(join.CompareFuncs) + len(join.LeftConditions) +
			len(join.RightConditions) + len(join.OtherConditions)
	case *physicalop.PhysicalIndexJoin:
		joinType = join.JoinType
		contract.expressionCount = len(join.OuterJoinKeys) + len(join.LeftConditions) +
			len(join.RightConditions) + len(join.OtherConditions) + compareFilterCount(join.CompareFilters)
	case *physicalop.PhysicalIndexHashJoin:
		joinType = join.JoinType
		contract.expressionCount = len(join.OuterHashKeys) + len(join.LeftConditions) +
			len(join.RightConditions) + len(join.OtherConditions) + compareFilterCount(join.CompareFilters)
	case *physicalop.PhysicalIndexMergeJoin:
		joinType = join.JoinType
		contract.expressionCount = len(join.CompareFuncs) + len(join.OuterCompareFuncs) +
			len(join.LeftConditions) + len(join.RightConditions) + len(join.OtherConditions) +
			compareFilterCount(join.CompareFilters)
	default:
		return statementRUJoinContract{}, false
	}
	return contract, joinType != base.FullOuterJoin
}

// collectStatementRUAggregationUnits charges one supported root or TiKV cop
// Aggregation occurrence using these formulas:
//
//	CPUWork = child rows * (GroupByItems + AggFuncs)
//	HashStateRows = completed root HashAgg group-map rows, or observed TiKV HashAgg output rows
//
// A response with a missing summary contributes no rows while other valid
// responses remain chargeable. StreamAgg has no hash state. TiFlash, malformed
// evidence, and an occurrence with no valid summary remain unsupported.
func collectStatementRUAggregationUnits(
	operator *plannercore.FlatOperator,
	children []statementRUOperatorResult,
	outputRows int64,
	outputRowsObserved bool,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
	calculator *statementRUCalculator,
) statementRUOperatorState {
	delta := statementRUCalculator{}
	if !statementRUOperatorRunsAtSupportedSite(operator) || !children[0].outputRowsObserved || !outputRowsObserved {
		return statementRUOperatorUnsupported
	}
	var baseAgg *physicalop.BasePhysicalAgg
	hashAgg := false
	switch agg := operator.Origin.(type) {
	case *physicalop.PhysicalHashAgg:
		hashAgg = true
		baseAgg = &agg.BasePhysicalAgg
	case *physicalop.PhysicalStreamAgg:
		baseAgg = &agg.BasePhysicalAgg
	default:
		return statementRUOperatorUnsupported
	}
	expressionCount := len(baseAgg.GroupByItems) + len(baseAgg.AggFuncs)
	if !addStatementRUCPUWork(
		&delta,
		float64(children[0].outputRows)*float64(expressionCount),
	) {
		return statementRUOperatorInvalid
	}
	if !hashAgg {
		if !mergeStatementRUUnitDelta(calculator, delta.units) {
			return statementRUOperatorInvalid
		}
		return statementRUOperatorComplete
	}
	if runtimeStatsColl == nil {
		return statementRUOperatorUnsupported
	}
	if operator.IsRoot {
		snapshot, found := runtimeStatsColl.GetRootHashStateRowsSnapshot(operator.Origin.ID())
		if !found {
			return statementRUOperatorUnsupported
		}
		if !snapshot.Complete() {
			if snapshot.Invalid() {
				return statementRUOperatorInvalid
			}
			return statementRUOperatorUnsupported
		}
		if !addStatementRUHashStateRows(&delta, float64(snapshot.Rows)) {
			return statementRUOperatorInvalid
		}
		if !mergeStatementRUUnitDelta(calculator, delta.units) {
			return statementRUOperatorInvalid
		}
		return statementRUOperatorComplete
	}
	// Each valid TiKV HashAgg execution summary reports the group states produced
	// by that response. Missing response summaries are marked by execdetails and
	// skipped; outputRows contains only the observed summaries.
	if !addStatementRUHashStateRows(&delta, float64(outputRows)) {
		return statementRUOperatorInvalid
	}
	if !mergeStatementRUUnitDelta(calculator, delta.units) {
		return statementRUOperatorInvalid
	}
	return statementRUOperatorComplete
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
	// GetCopScanDetail currently exposes totals merged across responses. Applying
	// the processed-byte ratio after that merge can estimate above or below the
	// sum of response-local physical sizes even when every field is present. A
	// later producer change will consume response-level total-key-size evidence;
	// until then the whole forest remains calibration-Incomplete.
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

func addStatementRUHashStateRows(calculator *statementRUCalculator, rows float64) bool {
	if calculator == nil || rows < 0 || math.IsNaN(rows) || math.IsInf(rows, 0) {
		return false
	}
	calculator.units.HashStateRows += rows
	return !math.IsInf(calculator.units.HashStateRows, 0)
}

func addStatementRUJoinOutputRows(calculator *statementRUCalculator, rows float64) bool {
	if calculator == nil || rows < 0 || math.IsNaN(rows) || math.IsInf(rows, 0) {
		return false
	}
	calculator.units.JoinOutputRows += rows
	return !math.IsInf(calculator.units.JoinOutputRows, 0)
}

// mergeStatementRUUnitDelta commits one fully validated operator occurrence.
// Mutating a copy prevents a later field overflow from retaining a partial
// occurrence in the statement-local calculator.
func mergeStatementRUUnitDelta(calculator *statementRUCalculator, delta statementRURawUnits) bool {
	if calculator == nil {
		return false
	}
	merged := *calculator
	if !addStatementRUCPUWork(&merged, delta.CPUWork) ||
		!addStatementRUScanBytes(&merged, delta.ScanBytes) ||
		!addStatementRUHashStateRows(&merged, delta.HashStateRows) ||
		!addStatementRUJoinOutputRows(&merged, delta.JoinOutputRows) {
		return false
	}
	*calculator = merged
	return true
}

func addStatementRURawUnits(left, right statementRURawUnits) statementRURawUnits {
	return statementRURawUnits{
		CPUWork:              left.CPUWork + right.CPUWork,
		ScanBytes:            left.ScanBytes + right.ScanBytes,
		NetBytes:             left.NetBytes + right.NetBytes,
		FrontendCompileBytes: left.FrontendCompileBytes + right.FrontendCompileBytes,
		HashStateRows:        left.HashStateRows + right.HashStateRows,
		JoinOutputRows:       left.JoinOutputRows + right.JoinOutputRows,
	}
}

func subtractStatementRURawUnits(left, right statementRURawUnits) statementRURawUnits {
	return statementRURawUnits{
		CPUWork:              left.CPUWork - right.CPUWork,
		ScanBytes:            left.ScanBytes - right.ScanBytes,
		NetBytes:             left.NetBytes - right.NetBytes,
		FrontendCompileBytes: left.FrontendCompileBytes - right.FrontendCompileBytes,
		HashStateRows:        left.HashStateRows - right.HashStateRows,
		JoinOutputRows:       left.JoinOutputRows - right.JoinOutputRows,
	}
}

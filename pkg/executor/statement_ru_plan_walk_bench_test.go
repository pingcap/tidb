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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	statementRUExecStmtSink   *ExecStmt
	statementRUCalculatorSink statementRUCalculator
	statementRUFinalizedSink  statementRUFinalizedSnapshot
	statementRUOperatorSink   statementRUOperatorResult
	statementRUExplainSink    *plannercore.ExplainRUResult
	statementRUCalculatedSink bool
)

func newStatementRUForestBenchmark(
	b testing.TB,
	cteCount int,
	scalarCount int,
) (*plannercore.FlatPhysicalPlan, *execdetails.RuntimeStatsColl) {
	b.Helper()
	fixture := newStatementRUSimpleSelectFixture(b)
	planCtx := fixture.stmt.Ctx.(*mock.Context)
	stmtCtx := planCtx.GetSessionVars().StmtCtx
	base := stmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	flat := &plannercore.FlatPhysicalPlan{
		Main:             append(plannercore.FlatPlanTree(nil), base.Main...),
		CTEs:             make([]plannercore.FlatPlanTree, cteCount),
		ScalarSubQueries: make([]plannercore.FlatPlanTree, scalarCount),
	}
	nextID := 1000
	newProjectionAndDual := func() (*physicalop.PhysicalProjection, *physicalop.PhysicalTableDual) {
		projection := physicalop.PhysicalProjection{
			Exprs: []expression.Expression{&expression.Column{}},
		}.Init(planCtx, &property.StatsInfo{RowCount: 4}, 0)
		projection.SetID(nextID)
		nextID++
		dual := physicalop.PhysicalTableDual{RowCount: 4}.Init(planCtx, &property.StatsInfo{RowCount: 4}, 0)
		dual.SetID(nextID)
		nextID++
		stmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(dual.ID(), true).Record(0, 4)
		return projection, dual
	}
	for i := range flat.CTEs {
		cte := physicalop.PhysicalCTE{
			CTE: &logicalop.CTEClass{IDForStorage: nextID},
		}.Init(planCtx, &property.StatsInfo{RowCount: 4})
		cte.SetID(nextID)
		root := (*physicalop.CTEDefinition)(cte)
		nextID++
		projection, dual := newProjectionAndDual()
		flat.CTEs[i] = plannercore.FlatPlanTree{
			{Origin: root, IsRoot: true, StoreType: kv.TiDB, ChildrenIdx: []int{1}},
			{Origin: projection, Label: plannercore.SeedPart, IsRoot: true, StoreType: kv.TiDB, ChildrenIdx: []int{2}},
			{Origin: dual, IsRoot: true, StoreType: kv.TiDB},
		}
	}
	for i := range flat.ScalarSubQueries {
		root := plannercore.ScalarSubqueryEvalCtx{}.Init(planCtx, 0)
		root.SetID(nextID)
		nextID++
		projection, dual := newProjectionAndDual()
		flat.ScalarSubQueries[i] = plannercore.FlatPlanTree{
			{Origin: root, IsRoot: true, StoreType: kv.TiDB, ChildrenIdx: []int{1}},
			{Origin: projection, IsRoot: true, StoreType: kv.TiDB, ChildrenIdx: []int{2}},
			{Origin: dual, IsRoot: true, StoreType: kv.TiDB},
		}
	}
	return flat, stmtCtx.RuntimeStatsColl
}

func BenchmarkStatementRUExecStmtSetup(b *testing.B) {
	// This is the absolute allocation of an otherwise-empty ExecStmt with the
	// nullable owner field, not an end-to-end statement setup delta.
	b.ReportAllocs()
	for b.Loop() {
		statementRUExecStmtSink = &ExecStmt{}
	}
}

func BenchmarkStatementRUNilHooks(b *testing.B) {
	stmt := &ExecStmt{}

	b.Run("outcome", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			stmt.RecordStatementRUFinalOutcome(true)
		}
	})

	b.Run("finish", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			stmt.finishStatementRU(nil)
		}
	})
}

func BenchmarkStatementRUOperatorCalculation(b *testing.B) {
	// This timer contains only representative occurrence-local formulas and
	// typed accumulation. It excludes tree routing, runtime-stat lookup,
	// finalization, and publication.
	b.ReportAllocs()
	for b.Loop() {
		calculator := statementRUCalculator{}
		valid := addStatementRUCPUWork(&calculator, statementRUSortWork(100, 10))
		statementRUCalculatorSink = calculator
		statementRUCalculatedSink = valid
	}
}

func BenchmarkStatementRUJoinAggOperatorCalculation(b *testing.B) {
	b.Run("join", func(b *testing.B) {
		// This timer contains the Join occurrence's checked input sum and typed
		// CPU/output/state accumulation. It excludes evidence lookup and traversal.
		b.ReportAllocs()
		for b.Loop() {
			calculator := statementRUCalculator{}
			inputRows, valid := checkedStatementRURowSum(100, 50)
			valid = valid && addStatementRUCPUWork(&calculator, float64(inputRows)*3)
			valid = valid && addStatementRUJoinOutputRows(&calculator, 40)
			valid = valid && addStatementRUHashStateRows(&calculator, 50)
			statementRUCalculatorSink = calculator
			statementRUCalculatedSink = valid
		}
	})

	b.Run("hash-aggregation", func(b *testing.B) {
		// This timer contains the HashAgg occurrence's CPU/state accumulation.
		// It excludes executor group-map construction and evidence lookup.
		b.ReportAllocs()
		for b.Loop() {
			calculator := statementRUCalculator{}
			valid := addStatementRUCPUWork(&calculator, 100*3)
			valid = valid && addStatementRUHashStateRows(&calculator, 25)
			statementRUCalculatorSink = calculator
			statementRUCalculatedSink = valid
		}
	})
}

func BenchmarkStatementRUExecutionDetailsAggregation(b *testing.B) {
	zero := uint64(0)
	rows := uint64(25)
	summary := &tipb.ExecutorExecutionSummary{
		TimeProcessedNs: &zero,
		NumProducedRows: &rows,
		NumIterations:   &zero,
	}
	var reuse *execdetails.RuntimeStatsColl

	// This timer covers one cop-response expectation, typed summary merge, and
	// value-only snapshot. It excludes protobuf decoding and network transport.
	b.ReportAllocs()
	for b.Loop() {
		reuse = execdetails.NewRuntimeStatsColl(reuse)
		reuse.RecordExpectedCopResponseSummaries([]int{1})
		reuse.RecordOneCopTask(1, kv.TiKV, summary)
		statementRUCalculatedSink = reuse.GetCopRowsSnapshot(1).Observed()
	}
}

func BenchmarkStatementRUTreeTraversal(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b)
	flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	setup := fixture.owner.calculationSetup
	stmtCtx := fixture.stmt.Ctx.GetSessionVars().StmtCtx
	calculator := newStatementRUCalculator(setup)
	result := calculateStatementRUPlan(
		flat.Main, 0, stmtCtx.RuntimeStatsColl, &calculator, calculator.units, nil,
	)
	if result.state != statementRUOperatorComplete {
		b.Fatalf("benchmark fixture must be supported, got state %v", result.state)
	}
	b.ResetTimer()

	// This timer starts with the canonical root occurrence and ends after the
	// single child-first walk. It includes runtime-stat lookups and typed
	// accumulation, but excludes statement aggregates and finalization.
	b.ReportAllocs()
	for b.Loop() {
		calculator := newStatementRUCalculator(setup)
		rootOwnedUnits := calculator.units
		statementRUOperatorSink = calculateStatementRUPlan(
			flat.Main,
			0,
			stmtCtx.RuntimeStatsColl,
			&calculator,
			rootOwnedUnits,
			nil,
		)
		statementRUCalculatorSink = calculator
	}
}

func BenchmarkStatementRUForestCalculation(b *testing.B) {
	for _, scenario := range []struct {
		name        string
		cteCount    int
		scalarCount int
	}{
		{name: "ordinary"},
		{name: "one-scalar", scalarCount: 1},
		{name: "one-cte", cteCount: 1},
		{name: "multiple-cte-scalar", cteCount: 2, scalarCount: 2},
		{name: "large-forest", cteCount: 8, scalarCount: 8},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			flat, coll := newStatementRUForestBenchmark(b, scenario.cteCount, scenario.scalarCount)
			setup := statementRUCalculationSetup{frontendCompileBytes: 7}

			b.Run("result-only", func(b *testing.B) {
				if _, ok := calculateStatementRU(flat, coll, nil, setup, true); !ok {
					b.Fatal("benchmark fixture must complete ResultOnly calculation")
				}
				b.ResetTimer()
				// This timer covers statement-wide aggregation, forest traversal,
				// calculation, and value-only finalize. Plan construction and
				// publication are outside the timed region.
				b.ReportAllocs()
				for b.Loop() {
					finalized, ok := calculateStatementRU(flat, coll, nil, setup, true)
					statementRUFinalizedSink = finalized
					statementRUCalculatedSink = ok
				}
			})

			b.Run("explain-result-construction", func(b *testing.B) {
				if result := plannercore.NewExplainRUResult(flat); result == nil {
					b.Fatal("benchmark fixture must produce an EXPLAIN result")
				}
				b.ResetTimer()
				// This isolates the linear forest copy that constructs the
				// occurrence-aligned result shape; it excludes RU calculation,
				// tree-edge traversal, formulas, rendering, and SQL execution.
				b.ReportAllocs()
				for b.Loop() {
					statementRUExplainSink = plannercore.NewExplainRUResult(flat)
				}
			})

			b.Run("explain-calculation", func(b *testing.B) {
				if _, result, ok := calculateStatementRUWithOperators(flat, coll, nil, setup, true); !ok || result == nil {
					b.Fatal("benchmark fixture must complete EXPLAIN calculation")
				}
				b.ResetTimer()
				// This adds the occurrence result to the ResultOnly calculation.
				// It excludes EXPLAIN row formatting and SQL execution.
				b.ReportAllocs()
				for b.Loop() {
					finalized, result, ok := calculateStatementRUWithOperators(flat, coll, nil, setup, true)
					statementRUFinalizedSink = finalized
					statementRUExplainSink = result
					statementRUCalculatedSink = ok
				}
			})
		})
	}
}

func BenchmarkStatementRUFinalizePublication(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b)
	calculator := statementRUCalculator{
		units: statementRURawUnits{
			CPUWork:              5,
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: fixture.owner.calculationSetup.frontendCompileBytes,
			HashStateRows:        7,
			JoinOutputRows:       8,
		},
	}

	// This timer covers value-only freeze plus both existing publication
	// boundaries. It excludes operator traversal and terminal lifecycle.
	b.ReportAllocs()
	for b.Loop() {
		finalized, ok := calculator.finalize()
		if !ok {
			b.Fatal("valid benchmark units failed to finalize")
		}
		publishStatementRUFinalizedSnapshot(fixture.stmt, finalized)
		statementRUFinalizedSink = finalized
	}
}

func BenchmarkStatementRUSyntheticTerminal(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b)
	stmt := fixture.stmt
	stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx
	flat := stmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)

	// This timer starts before the production owner installer and ends after
	// RU v3 metric publication and the dormant calibration boundary. It manually invokes
	// lifecycle hooks against one reused synthetic ExecStmt; it excludes compile,
	// executor Next/Close, session completion, and RUv2/network finalization, so it
	// must not be reported as end-to-end SELECT latency.
	for _, cacheMode := range []struct {
		name               string
		populateAtTerminal bool
	}{
		{name: "cache-hit", populateAtTerminal: true},
		{name: "cache-miss"},
	} {
		b.Run(cacheMode.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				stmtCtx.SetFlatPlan(nil)
				installStatementRUOwner(stmt)
				if cacheMode.populateAtTerminal {
					stmtCtx.SetFlatPlan(flat)
				}
				stmt.recordStatementRURootEOF()
				stmt.RecordStatementRUFinalOutcome(true)
				stmt.finishStatementRU(nil)
			}
		})
	}
}

func BenchmarkStatementRUOwnerSetup(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b)
	stmt := fixture.stmt
	stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx
	stmtCtx.SetFlatPlan(nil)

	// The fixture's flat-cache reset is setup outside b.Loop. The timed region is
	// the production owner installer plus the sink assignment that keeps its
	// allocation observable.
	b.ReportAllocs()
	for b.Loop() {
		installStatementRUOwner(stmt)
		statementRUExecStmtSink = stmt
	}
}

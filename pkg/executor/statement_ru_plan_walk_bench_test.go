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
	"fmt"
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
)

var (
	statementRUExecStmtSink    *ExecStmt
	statementRUFlatPlanSink    *plannercore.FlatPhysicalPlan
	statementRUCalculatorSink  statementRUCalculator
	statementRUFinalizedSink   statementRUFinalizedSnapshot
	statementRUResultSink      statementRUResultOnly
	statementRUExecDetailsSink execdetails.ExecDetails
	statementRUScanBytesSink   float64
	statementRUStateSink       statementRUCalibrationState
	statementRUVisitSink       int
)

func buildStatementRUPlanChainForBenchmark(operatorCount int) (*ExecStmt, base.Plan) {
	ctx := mock.NewContext()
	stats := &property.StatsInfo{RowCount: 1}
	var plan base.PhysicalPlan = physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	for range operatorCount - 1 {
		limit := physicalop.PhysicalLimit{}.Init(ctx, stats, 0)
		limit.SetChildren(plan)
		plan = limit
	}
	ctx.GetSessionVars().StmtCtx.SetPlan(plan)
	return &ExecStmt{Ctx: ctx, Plan: plan}, plan
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
			stmt.finishStatementRU(nil, execdetails.ExecDetails{})
		}
	})
}

func BenchmarkStatementRUPlanWalk(b *testing.B) {
	for _, operatorCount := range []int{1, 11, 50, 200} {
		b.Run(fmt.Sprintf("operators=%d", operatorCount), func(b *testing.B) {
			stmt, plan := buildStatementRUPlanChainForBenchmark(operatorCount)
			stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx
			flat := plannercore.FlattenPhysicalPlan(plan, false)
			observe := func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
				statementRUVisitSink++
			}
			visit := func(
				treeKind statementRUPlanTreeKind,
				treeIndex int,
				operatorIndex int,
				operator *plannercore.FlatOperator,
				scanBytes float64,
			) float64 {
				observe(treeKind, treeIndex, operatorIndex, operator)
				return scanBytes
			}

			b.Run("flatten-physical-plan", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					statementRUFlatPlanSink = plannercore.FlattenPhysicalPlan(plan, false)
				}
			})

			b.Run("get-flat-plan/cache-hit", func(b *testing.B) {
				stmtCtx.SetFlatPlan(flat)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					statementRUFlatPlanSink = getFlatPlan(stmtCtx)
				}
			})

			b.Run("get-flat-plan/cache-miss", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					stmtCtx.SetFlatPlan(nil)
					statementRUFlatPlanSink = getFlatPlan(stmtCtx)
				}
			})

			b.Run("walk-only", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					walkStatementRUFlatPlan(flat, visit)
				}
			})

			// These paths include owner setup, outcome publication, flat lookup,
			// and the synchronous occurrence walk, but not SQL compilation/execution.
			b.Run("successful-plan-walk/cache-hit", func(b *testing.B) {
				stmtCtx.SetFlatPlan(flat)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					stmt.statementRUOwner = newStatementRUPlanWalkVisitorOwnerForTest(stmt, observe)
					stmt.RecordStatementRUFinalOutcome(true)
					stmt.finishStatementRU(nil, execdetails.ExecDetails{})
				}
			})

			b.Run("successful-plan-walk/cache-miss", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					stmtCtx.SetFlatPlan(nil)
					stmt.statementRUOwner = newStatementRUPlanWalkVisitorOwnerForTest(stmt, observe)
					stmt.RecordStatementRUFinalOutcome(true)
					stmt.finishStatementRU(nil, execdetails.ExecDetails{})
				}
			})
		})
	}
}

func BenchmarkStatementRUComponents(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b, true, true)
	flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	setup := fixture.owner.calculationSetup

	b.Run("calculator-setup", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			statementRUCalculatorSink = newStatementRUCalculator(setup)
		}
	})

	b.Run("walk-only", func(b *testing.B) {
		visit := func(
			_ statementRUPlanTreeKind,
			_ int,
			_ int,
			_ *plannercore.FlatOperator,
			scanBytes float64,
		) float64 {
			statementRUVisitSink++
			return scanBytes
		}
		b.ReportAllocs()
		for b.Loop() {
			walkStatementRUFlatPlan(flat, visit)
		}
	})

	b.Run("statement-scan-evidence", func(b *testing.B) {
		stmtCtx := fixture.stmt.Ctx.GetSessionVars().StmtCtx
		// The adapter already performs this read for existing terminal accounting
		// and passes the value to statement RU. Keep it as a component reference,
		// not as stack-2 incremental cost.
		b.ReportAllocs()
		for b.Loop() {
			statementRUExecDetailsSink = stmtCtx.GetExecDetails()
		}
	})

	b.Run("scan-unit", func(b *testing.B) {
		detail := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetExecDetails().ScanDetail
		b.ReportAllocs()
		for b.Loop() {
			statementRUScanBytesSink, statementRUStateSink = statementRUScanBytes(
				detail.TotalKeys,
				detail.ProcessedKeys,
				detail.ProcessedKeysSize,
			)
		}
	})

	b.Run("finalize", func(b *testing.B) {
		calculator := statementRUCalculator{units: statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: setup.frontendCompileBytes,
		}}
		b.ReportAllocs()
		for b.Loop() {
			statementRUFinalizedSink = calculator.finalize(true)
		}
	})

	b.Run("result-construction", func(b *testing.B) {
		units := statementRURawUnits{ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15}
		b.ReportAllocs()
		for b.Loop() {
			statementRUResultSink = calculateStatementRUResultOnly(units)
		}
	})

	b.Run("result-publication/current-logger", func(b *testing.B) {
		result := statementRUResultOnly{TotalRU: 45}
		b.ReportAllocs()
		for b.Loop() {
			publishStatementRUResultSafely(fixture.stmt, result)
		}
	})

	b.Run("calibration-publication/dormant-consumer", func(b *testing.B) {
		units := statementRURawUnits{ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15}
		b.ReportAllocs()
		for b.Loop() {
			publishStatementRUCalibrationSafely(fixture.stmt, statementRUCalibrationSnapshot{
				State: statementRUCalibrationComplete,
				Units: units,
			})
		}
	})
}

func BenchmarkStatementRUSyntheticFinalization(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b, true, true)
	stmt := fixture.stmt
	stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx
	flat := stmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	execDetail := stmtCtx.GetExecDetails()

	// This timer starts before the production owner installer and ends after
	// ResultOnly and the dormant calibration boundary. It manually invokes
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
				stmt.finishStatementRU(nil, execDetail)
			}
		})
	}
}

func BenchmarkStatementRUOwnerSetup(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b, true, true)
	stmt := fixture.stmt
	stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx

	// This includes clearing the flat cache required by installer eligibility.
	// Every eligible read statement pays this setup cost; it is not the allocation
	// cost of newStatementRUOwner in isolation.
	b.ReportAllocs()
	for b.Loop() {
		stmtCtx.SetFlatPlan(nil)
		installStatementRUOwner(stmt)
		statementRUExecStmtSink = stmt
	}
}

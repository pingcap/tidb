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
	"github.com/pingcap/tidb/pkg/util/mock"
	"go.uber.org/zap"
)

var (
	statementRUExecStmtSink    *ExecStmt
	statementRUFlatPlanSink    *plannercore.FlatPhysicalPlan
	statementRUPublicationSink statementRUSimpleSelectPublication
	statementRUResultSink      statementRUResultOnly
	statementRUCalibrationSink statementRUCalibrationSnapshot
	statementRUAccumulatorSink statementRUSimpleSelectAccumulator
	statementRUVisitSink       int
)

func consumeStatementRUResultForBenchmark(_ *zap.Logger, _ uint64, result statementRUResultOnly) {
	statementRUResultSink = result
}

func discardStatementRUResultForBenchmark(*zap.Logger, uint64, statementRUResultOnly) {}

func consumeStatementRUCalibrationForBenchmark(snapshot statementRUCalibrationSnapshot) {
	statementRUCalibrationSink = snapshot
}

func discardStatementRUCalibrationForBenchmark(statementRUCalibrationSnapshot) {}

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

	b.Run("finish-plan-walk", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			stmt.finishStatementRUPlanWalk(nil)
		}
	})
}

func BenchmarkStatementRUPlanWalk(b *testing.B) {
	for _, operatorCount := range []int{1, 11, 50, 200} {
		b.Run(fmt.Sprintf("operators=%d", operatorCount), func(b *testing.B) {
			stmt, plan := buildStatementRUPlanChainForBenchmark(operatorCount)
			stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx
			flat := plannercore.FlattenPhysicalPlan(plan, false)
			visit := func(statementRUPlanTreeKind, int, int, *plannercore.FlatOperator) {
				statementRUVisitSink++
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

			// These paths include per-statement owner setup, successful outcome
			// publication, flat-plan lookup, and the synchronous occurrence walk.
			b.Run("successful-plan-walk/cache-hit", func(b *testing.B) {
				stmtCtx.SetFlatPlan(flat)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					stmt.statementRUPlanWalkOwner = newStatementRUPlanWalkVisitorOwner(stmt, visit)
					stmt.RecordStatementRUFinalOutcome(true)
					stmt.finishStatementRUPlanWalk(nil)
				}
			})

			b.Run("successful-plan-walk/cache-miss", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					stmtCtx.SetFlatPlan(nil)
					stmt.statementRUPlanWalkOwner = newStatementRUPlanWalkVisitorOwner(stmt, visit)
					stmt.RecordStatementRUFinalOutcome(true)
					stmt.finishStatementRUPlanWalk(nil)
				}
			})
		})
	}
}

func BenchmarkStatementRUSimpleSelectComponents(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b, true, true)
	stmtCtx := fixture.stmt.Ctx.GetSessionVars().StmtCtx
	flat := stmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	execDetails := stmtCtx.GetExecDetails()
	execView := statementRUTerminalExecDetailsView{execDetails: execDetails}

	b.Run("occurrence-calculation", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			accumulator := statementRUSimpleSelectAccumulator{}
			accumulator.start(flat)
			binding, owned := bindStatementRUSimpleSelectPlan(fixture.stmt.Plan)
			if !owned {
				b.Fatal("simple SELECT fixture lost its reader-owned scan")
			}
			walkStatementRUFlatPlan(flat, func(
				treeKind statementRUPlanTreeKind,
				_ int,
				_ int,
				operator *plannercore.FlatOperator,
			) {
				accumulator.observe(binding, treeKind, operator)
			})
			accumulator.finish()
			statementRUAccumulatorSink = accumulator
		}
	})

	for _, calibration := range []bool{false, true} {
		name := "canonical-freeze/result-only"
		if calibration {
			name = "canonical-freeze/result-and-calibration"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				run := *fixture.run
				run.resultPublisher = discardStatementRUResultForBenchmark
				if calibration {
					run.calibrationPublisher = discardStatementRUCalibrationForBenchmark
				} else {
					run.calibrationPublisher = nil
				}
				statementRUPublicationSink = run.freeze(fixture.stmt, fixture.owner, flat, &execView)
			}
		})
	}

	b.Run("result-projection-and-publication", func(b *testing.B) {
		units := statementRURawUnits{ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15}
		weights := statementRUPlaceholderWeightSnapshot()
		b.ReportAllocs()
		for b.Loop() {
			total, ok := weights.total(units)
			if !ok {
				b.Fatal("valid fixture became invalid")
			}
			publication := statementRUSimpleSelectPublication{
				resultPublisher: consumeStatementRUResultForBenchmark,
				result:          statementRUResultOnly{TotalRU: total},
				hasResult:       true,
			}
			publication.publish()
		}
	})

	b.Run("default-result-publication/debug-disabled-nop-logger", func(b *testing.B) {
		publication := fixture.run.freeze(fixture.stmt, fixture.owner, flat, &execView)
		if !publication.hasResult {
			b.Fatal("complete fixture did not produce a ResultOnly publication")
		}
		publication.resultLogger = zap.NewNop()
		b.ReportAllocs()
		for b.Loop() {
			publication.publish()
		}
	})

	b.Run("calibration-snapshot-and-publication", func(b *testing.B) {
		units := statementRURawUnits{ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15}
		b.ReportAllocs()
		for b.Loop() {
			snapshot := statementRUCalibrationSnapshot{
				State: statementRUCalibrationComplete,
				Units: units,
			}
			publication := statementRUSimpleSelectPublication{
				calibrationPublisher: consumeStatementRUCalibrationForBenchmark,
				calibration:          snapshot,
				hasCalibration:       true,
			}
			publication.publish()
		}
	})
}

func BenchmarkStatementRUSimpleSelectSetupAndTerminal(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b, true, true)
	stmt := fixture.stmt
	stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx
	flat := stmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	execDetails := stmtCtx.GetExecDetails()
	execView := statementRUTerminalExecDetailsView{execDetails: execDetails}

	// A fresh statement must have no flat cache when the owner is installed.
	// The cache-hit case models another statement-lifecycle consumer populating
	// that same-generation cache before terminal, as observed by the real SELECT
	// probe. The cache-miss case includes stack 1's terminal flatten fallback.
	for _, cacheMode := range []struct {
		name               string
		populateAtTerminal bool
	}{
		{name: "cache-hit", populateAtTerminal: true},
		{name: "cache-miss"},
	} {
		for _, calibration := range []bool{false, true} {
			name := "result-only/" + cacheMode.name
			if calibration {
				name = "result-and-calibration/" + cacheMode.name
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					stmtCtx.SetFlatPlan(nil)
					installStatementRUSimpleSelectOwner(stmt)
					if cacheMode.populateAtTerminal {
						stmtCtx.SetFlatPlan(flat)
					}
					stmt.recordStatementRURootEOF()
					stmt.sealStatementRUResultTermination()
					if calibration {
						stmt.statementRUPlanWalkOwner.simpleSelectRun.calibrationPublisher =
							discardStatementRUCalibrationForBenchmark
					}
					stmt.RecordStatementRUFinalOutcome(true)
					stmt.finishStatementRUPlanWalkWithExecDetails(nil, &execView)
				}
			})
		}
	}
}

func BenchmarkStatementRUSimpleSelectOwnerSetup(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b, true, true)
	stmt := fixture.stmt
	stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx

	b.ReportAllocs()
	for b.Loop() {
		stmtCtx.SetFlatPlan(nil)
		installStatementRUSimpleSelectOwner(stmt)
		statementRUExecStmtSink = stmt
	}
}

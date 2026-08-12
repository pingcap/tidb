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

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)

var (
	statementRUExecStmtSink   *ExecStmt
	statementRUCalculatorSink statementRUCalculator
	statementRUFinalizedSink  statementRUFinalizedSnapshot
	statementRUResultSink     statementRUResultOnly
	statementRUScanBytesSink  float64
	statementRUStateSink      statementRUCalibrationState
	statementRUCalculatedSink bool
)

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

func BenchmarkStatementRUComponents(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b)
	flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	setup := fixture.owner.calculationSetup

	b.Run("calculator-setup", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			statementRUCalculatorSink = newStatementRUCalculator(setup)
		}
	})

	b.Run("calculate", func(b *testing.B) {
		stmtCtx := fixture.stmt.Ctx.GetSessionVars().StmtCtx
		metrics := fixture.stmt.Ctx.GetSessionVars().RUV2Metrics
		b.ReportAllocs()
		for b.Loop() {
			statementRUFinalizedSink, statementRUCalculatedSink = calculateStatementRU(
				flat,
				stmtCtx.RuntimeStatsColl,
				metrics,
				setup,
				true,
			)
		}
	})

	b.Run("scan-detail-lookup", func(b *testing.B) {
		stmtCtx := fixture.stmt.Ctx.GetSessionVars().StmtCtx
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		b.ReportAllocs()
		for b.Loop() {
			detail, _ := stmtCtx.RuntimeStatsColl.GetCopScanDetail(reader.TablePlan.ID())
			statementRUScanBytesSink = float64(detail.ProcessedKeysSize)
		}
	})

	b.Run("scan-byte-estimate", func(b *testing.B) {
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		detail, _ := fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetCopScanDetail(reader.TablePlan.ID())
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
		calculator := statementRUCalculator{
			units: statementRURawUnits{
				ScanBytes:            10,
				NetBytes:             20,
				FrontendCompileBytes: setup.frontendCompileBytes,
			},
		}
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
	fixture := newStatementRUSimpleSelectFixture(b)
	stmt := fixture.stmt
	stmtCtx := stmt.Ctx.GetSessionVars().StmtCtx
	flat := stmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)

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

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
)

var (
	statementRUExecStmtSink   *ExecStmt
	statementRUCalculatorSink statementRUCalculator
	statementRUFinalizedSink  statementRUFinalizedSnapshot
	statementRUOperatorSink   statementRUOperatorResult
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

func BenchmarkStatementRUTreeTraversal(b *testing.B) {
	fixture := newStatementRUSimpleSelectFixture(b)
	flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	setup := fixture.owner.calculationSetup
	stmtCtx := fixture.stmt.Ctx.GetSessionVars().StmtCtx

	// This timer starts with the canonical root occurrence and ends after the
	// single child-first walk. It includes runtime-stat lookups and typed
	// accumulation, but excludes statement aggregates and finalization.
	b.ReportAllocs()
	for b.Loop() {
		calculator := newStatementRUCalculator(setup)
		statementRUOperatorSink = calculateStatementRUPlan(
			flat.Main,
			0,
			stmtCtx.RuntimeStatsColl,
			&calculator,
			nil,
		)
		statementRUCalculatorSink = calculator
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

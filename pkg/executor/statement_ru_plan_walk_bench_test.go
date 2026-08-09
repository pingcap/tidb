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
)

var (
	statementRUExecStmtSink *ExecStmt
	statementRUFlatPlanSink *plannercore.FlatPhysicalPlan
	statementRUVisitSink    int
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
					stmt.statementRUPlanWalkOwner = newStatementRUPlanWalkOwner(stmt, visit)
					stmt.RecordStatementRUFinalOutcome(true)
					stmt.finishStatementRUPlanWalk(nil)
				}
			})

			b.Run("successful-plan-walk/cache-miss", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					stmtCtx.SetFlatPlan(nil)
					stmt.statementRUPlanWalkOwner = newStatementRUPlanWalkOwner(stmt, visit)
					stmt.RecordStatementRUFinalOutcome(true)
					stmt.finishStatementRUPlanWalk(nil)
				}
			})
		})
	}
}

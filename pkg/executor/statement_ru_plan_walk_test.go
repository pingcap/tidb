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
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func newStatementRUOwnerForTest() (*ExecStmt, *statementRUOwner) {
	ctx := mock.NewContext()
	plan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	ctx.GetSessionVars().StmtCtx.SetPlan(plan)
	stmt := &ExecStmt{
		Ctx:   ctx,
		GoCtx: context.Background(),
		Plan:  plan,
	}
	owner := newStatementRUOwner(stmt)
	owner.calculationSetup.frontendCompileBytes = 1
	stmt.statementRUOwner = owner
	return stmt, owner
}

type statementRUPanicOnceContext struct {
	context.Context
	panicked atomic.Bool
}

func (ctx *statementRUPanicOnceContext) Value(key any) any {
	if ctx.panicked.CompareAndSwap(false, true) {
		panic("statement RU recordSet panic test")
	}
	return ctx.Context.Value(key)
}

// StatementRUOwnerObservationForTest exposes lifecycle state to external-package
// tests without adding a callback or probe field to the production owner.
type StatementRUOwnerObservationForTest struct {
	owner        *statementRUOwner
	initialSetup statementRUCalculationSetup
}

// ObserveStatementRUOwnerForTest returns a handle to the production-installed
// owner. It must not make an otherwise-ineligible statement appear eligible.
func ObserveStatementRUOwnerForTest(stmt *ExecStmt) *StatementRUOwnerObservationForTest {
	if stmt == nil || stmt.statementRUOwner == nil {
		return nil
	}
	return &StatementRUOwnerObservationForTest{
		owner:        stmt.statementRUOwner,
		initialSetup: stmt.statementRUOwner.calculationSetup,
	}
}

// ConsumedForTest reports whether the first terminal or abort cleared the setup.
// A zero initial setup cannot distinguish an unconsumed owner from a consumed
// one, so it fails closed. Call this only after the lifecycle has quiesced.
func (observation *StatementRUOwnerObservationForTest) ConsumedForTest() bool {
	return observation != nil && observation.owner != nil &&
		observation.initialSetup != (statementRUCalculationSetup{}) &&
		observation.owner.calculationSetup == (statementRUCalculationSetup{})
}

// RecordedSuccessForTest reports whether the session recorded success first.
func (observation *StatementRUOwnerObservationForTest) RecordedSuccessForTest() bool {
	return observation != nil && observation.owner != nil &&
		statementRUFinalOutcome(observation.owner.finalOutcome.Load()) == statementRUFinalOutcomeSuccess
}

func TestStatementRUCalculationTraversal(t *testing.T) {
	setPlan := func(fixture statementRUSimpleSelectFixture, plan base.PhysicalPlan) {
		fixture.stmt.Plan = plan
		stmtCtx := fixture.stmt.Ctx.GetSessionVars().StmtCtx
		stmtCtx.SetPlan(plan)
		stmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(plan, false))
	}
	recordRootRows := func(fixture statementRUSimpleSelectFixture, plan base.Plan, rows int64) {
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.
			GetBasicRuntimeStats(plan.ID(), true).SetRowNum(rows)
	}
	recordCopRows := func(fixture statementRUSimpleSelectFixture, plan base.Plan, rows uint64) {
		zero := uint64(0)
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RecordOneCopTask(
			plan.ID(),
			kv.TiKV,
			&tipb.ExecutorExecutionSummary{
				TimeProcessedNs: &zero,
				NumProducedRows: &rows,
				NumIterations:   &zero,
				Concurrency:     &zero,
			},
		)
	}
	recordScan := func(
		fixture statementRUSimpleSelectFixture,
		requestRoot base.Plan,
		totalKeys, processedKeys, processedKeysSize int64,
	) {
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RecordCopStats(
			requestRoot.ID(),
			kv.TiKV,
			&util.ScanDetail{
				TotalKeys:         totalKeys,
				ProcessedKeys:     processedKeys,
				ProcessedKeysSize: processedKeysSize,
			},
			util.TimeDetail{},
			nil,
			nil,
		)
	}
	newIndexLookupPlan := func(fixture statementRUSimpleSelectFixture) (
		*physicalop.PhysicalIndexLookUpReader,
		*physicalop.PhysicalIndexScan,
		*physicalop.PhysicalTableScan,
	) {
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		indexScan := (&physicalop.PhysicalIndexScan{
			Table:            &model.TableInfo{},
			Index:            &model.IndexInfo{},
			DataSourceSchema: expression.NewSchema(),
		}).Init(planCtx, 0)
		tableScan := (&physicalop.PhysicalTableScan{
			Table:     &model.TableInfo{},
			StoreType: kv.TiKV,
		}).Init(planCtx, 0)
		tableScan.SetSchema(expression.NewSchema())
		indexLookup := (physicalop.PhysicalIndexLookUpReader{
			IndexPlan: indexScan,
			TablePlan: tableScan,
		}).Init(planCtx, 0, plannerutil.IndexLookUpPushDownNone)
		return indexLookup, indexScan, tableScan
	}
	requirePublication := func(
		t *testing.T,
		fixture statementRUSimpleSelectFixture,
		wantUnits statementRURawUnits,
	) {
		var calibrationCount atomic.Int64
		var snapshot statementRUCalibrationSnapshot
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
			snapshot = published
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, int64(1), calibrationCount.Load())
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Equal(t, wantUnits, snapshot.Units)
		require.InDelta(t, calculateStatementRUResultOnly(wantUnits).TotalRU,
			testutil.ToFloat64(metrics.RUV3Total)-totalBefore, 1e-9)
		require.Zero(t, fixture.owner.calculationSetup)
	}
	requireNoPublication := func(t *testing.T, fixture statementRUSimpleSelectFixture) {
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV3Total))
		require.Zero(t, calibrationCount.Load())
	}

	t.Run("Reader scan evidence is collected during calculation", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		// Statement-level ExecDetails is deliberately unrelated to the Reader's
		// own cop runtime stats and must not affect this calculation.
		fixture.mergeStatementScanDetail(&util.ScanDetail{
			TotalKeys:         100,
			ProcessedKeys:     100,
			ProcessedKeysSize: 10000,
		})
		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("pushed Selection uses child rows without duplicating Reader scan evidence", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		selection := physicalop.PhysicalSelection{
			Conditions: []expression.Expression{expression.NewOne(), expression.NewOne()},
		}.Init(planCtx, &property.StatsInfo{RowCount: 1}, 0)
		selection.SetChildren(scan)
		reader.TablePlan = selection
		reader.TablePlans = physicalop.FlattenListPushDownPlan(selection)
		fixture.recordReaderScanDetail(reader, 1, 1, 10)
		recordCopRows(fixture, scan, 7)
		setPlan(fixture, reader)

		requirePublication(t, fixture, statementRURawUnits{
			CPUWork:              14,
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("root Selection uses direct child output rows", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		selection := physicalop.PhysicalSelection{
			Conditions: []expression.Expression{expression.NewOne(), expression.NewOne(), expression.NewOne()},
		}.Init(planCtx, &property.StatsInfo{RowCount: 1}, 0)
		selection.SetChildren(reader)
		recordRootRows(fixture, reader, 4)
		setPlan(fixture, selection)

		requirePublication(t, fixture, statementRURawUnits{
			CPUWork:              12,
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("root Sort uses direct child output rows", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		sort := physicalop.PhysicalSort{}.Init(planCtx, &property.StatsInfo{RowCount: 8}, 0)
		sort.SetChildren(reader)
		recordRootRows(fixture, reader, 8)
		setPlan(fixture, sort)

		requirePublication(t, fixture, statementRURawUnits{
			CPUWork:              24,
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("intest catches root Sort with unmaterialized scalar ordering", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		sort := physicalop.PhysicalSort{
			ByItems: []*plannerutil.ByItems{{Expr: &expression.ScalarFunction{}}},
		}.Init(planCtx, &property.StatsInfo{RowCount: 8}, 0)
		require.Panics(t, func() {
			statementRUAssertOrderingMaterialized(sort.ByItems)
		})
		sort.SetChildren(reader)
		setPlan(fixture, sort)

		requireNoPublication(t, fixture)
	})

	t.Run("root TopN uses checked offset plus count", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		topN := physicalop.PhysicalTopN{Offset: 5, Count: 5}.
			Init(planCtx, &property.StatsInfo{RowCount: 5}, 0)
		topN.SetChildren(reader)
		recordRootRows(fixture, reader, 100)
		setPlan(fixture, topN)

		requirePublication(t, fixture, statementRURawUnits{
			CPUWork:              100 * math.Log2(10),
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("root TopN count zero ignores offset without overflow", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		topN := physicalop.PhysicalTopN{Offset: math.MaxUint64, Count: 0}.
			Init(planCtx, &property.StatsInfo{}, 0)
		topN.SetChildren(reader)
		recordRootRows(fixture, reader, 100)
		setPlan(fixture, topN)

		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("root TopN offset plus count overflow fails closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		topN := physicalop.PhysicalTopN{Offset: math.MaxUint64, Count: 1}.
			Init(planCtx, &property.StatsInfo{}, 0)
		topN.SetChildren(reader)
		setPlan(fixture, topN)

		requireNoPublication(t, fixture)
	})

	t.Run("root Limit uses direct child output rows", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		limit := physicalop.PhysicalLimit{Count: 8}.
			Init(planCtx, &property.StatsInfo{RowCount: 8}, 0)
		limit.SetChildren(reader)
		recordRootRows(fixture, reader, 13)
		setPlan(fixture, limit)

		requirePublication(t, fixture, statementRURawUnits{
			CPUWork:              13,
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("zero-count root Limit still charges visible Reader scan evidence", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		limit := physicalop.PhysicalLimit{Count: 0}.
			Init(planCtx, &property.StatsInfo{}, 0)
		limit.SetChildren(reader)
		setPlan(fixture, limit)

		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("pushed TopN uses zero offset contract", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		topN := physicalop.PhysicalTopN{Count: 8}.
			Init(planCtx, &property.StatsInfo{RowCount: 8}, 0)
		topN.SetChildren(scan)
		reader.TablePlan = topN
		reader.TablePlans = physicalop.FlattenListPushDownPlan(topN)
		recordCopRows(fixture, scan, 100)
		fixture.recordReaderScanDetail(reader, 1, 1, 10)
		setPlan(fixture, reader)

		requirePublication(t, fixture, statementRURawUnits{
			CPUWork:              300,
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("pushed TopN with nonzero offset fails closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		topN := physicalop.PhysicalTopN{Offset: 1, Count: 8}.
			Init(planCtx, &property.StatsInfo{RowCount: 8}, 0)
		topN.SetChildren(scan)
		reader.TablePlan = topN
		reader.TablePlans = physicalop.FlattenListPushDownPlan(topN)
		setPlan(fixture, reader)

		requireNoPublication(t, fixture)
	})

	t.Run("pushed Limit uses direct child output rows", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		limit := physicalop.PhysicalLimit{Count: 8}.
			Init(planCtx, &property.StatsInfo{RowCount: 8}, 0)
		limit.SetChildren(scan)
		reader.TablePlan = limit
		reader.TablePlans = physicalop.FlattenListPushDownPlan(limit)
		recordCopRows(fixture, scan, 11)
		fixture.recordReaderScanDetail(reader, 1, 1, 10)
		setPlan(fixture, reader)

		requirePublication(t, fixture, statementRURawUnits{
			CPUWork:              11,
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("missing child row evidence contributes zero", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		selection := physicalop.PhysicalSelection{
			Conditions: []expression.Expression{expression.NewOne()},
		}.Init(planCtx, &property.StatsInfo{RowCount: 1}, 0)
		selection.SetChildren(scan)
		reader.TablePlan = selection
		reader.TablePlans = physicalop.FlattenListPushDownPlan(selection)
		fixture.recordReaderScanDetail(reader, 1, 1, 10)
		setPlan(fixture, reader)

		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("IndexReader owns one optional request branch", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		indexScan := (&physicalop.PhysicalIndexScan{
			Table:            &model.TableInfo{},
			Index:            &model.IndexInfo{},
			DataSourceSchema: expression.NewSchema(),
		}).Init(planCtx, 0)
		indexReader := (&physicalop.PhysicalIndexReader{IndexPlan: indexScan}).Init(planCtx, 0)
		recordScan(fixture, indexScan, 4, 2, 6)
		setPlan(fixture, indexReader)

		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            12,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("IndexReader partial scan detail contributes zero", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		indexScan := (&physicalop.PhysicalIndexScan{
			Table:            &model.TableInfo{},
			Index:            &model.IndexInfo{},
			DataSourceSchema: expression.NewSchema(),
		}).Init(planCtx, 0)
		indexReader := (&physicalop.PhysicalIndexReader{IndexPlan: indexScan}).Init(planCtx, 0)
		recordScan(fixture, indexScan, 0, 2, 0)
		setPlan(fixture, indexReader)

		requirePublication(t, fixture, statementRURawUnits{
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("IndexReader contradictory scan detail fails closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		indexScan := (&physicalop.PhysicalIndexScan{
			Table:            &model.TableInfo{},
			Index:            &model.IndexInfo{},
			DataSourceSchema: expression.NewSchema(),
		}).Init(planCtx, 0)
		indexReader := (&physicalop.PhysicalIndexReader{IndexPlan: indexScan}).Init(planCtx, 0)
		recordScan(fixture, indexScan, 10, 0, 1)
		setPlan(fixture, indexReader)

		requireNoPublication(t, fixture)
	})

	t.Run("IndexLookup sums visible index and table request branches", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		indexLookup, indexScan, tableScan := newIndexLookupPlan(fixture)
		recordScan(fixture, indexScan, 4, 2, 6)
		recordScan(fixture, tableScan, 3, 3, 21)
		setPlan(fixture, indexLookup)

		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            33,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("IndexLookup missing table branch contributes zero", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		indexLookup, indexScan, _ := newIndexLookupPlan(fixture)
		recordScan(fixture, indexScan, 4, 2, 6)
		setPlan(fixture, indexLookup)

		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            12,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("IndexLookup missing index branch contributes zero", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		indexLookup, _, tableScan := newIndexLookupPlan(fixture)
		recordScan(fixture, tableScan, 3, 3, 21)
		setPlan(fixture, indexLookup)

		requirePublication(t, fixture, statementRURawUnits{
			ScanBytes:            21,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		})
	})

	t.Run("IndexLookup branch role mismatch fails closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		indexLookup, _, _ := newIndexLookupPlan(fixture)
		setPlan(fixture, indexLookup)
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		flat.Main[flat.Main[0].ChildrenIdx[0]].Label = plannercore.Empty

		requireNoPublication(t, fixture)
	})

	t.Run("root TableScan is outside the current slice", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		fixture.stmt.Plan = scan
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(scan, false))

		requireNoPublication(t, fixture)
	})

	t.Run("nested TableReader is outside the current slice", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		nestedReader := (&physicalop.PhysicalTableReader{
			TablePlan: scan,
			StoreType: reader.StoreType,
		}).Init(planCtx, 0)
		reader.TablePlan = nestedReader
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(reader, false))

		requireNoPublication(t, fixture)
	})

	t.Run("invalid child edge fails closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		flat.Main[0].ChildrenIdx = []int{len(flat.Main)}

		requireNoPublication(t, fixture)
	})

	t.Run("self-referential child edge fails closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		flat.Main[0].ChildrenIdx = []int{0}

		requireNoPublication(t, fixture)
	})

	t.Run("two-node child cycle fails closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		flat.Main[1].ChildrenIdx = []int{0}

		requireNoPublication(t, fixture)
	})

	t.Run("present negative child rows fail closed", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		selection := physicalop.PhysicalSelection{
			Conditions: []expression.Expression{expression.NewOne()},
		}.Init(planCtx, &property.StatsInfo{RowCount: 1}, 0)
		selection.SetChildren(reader)
		recordRootRows(fixture, reader, -1)
		setPlan(fixture, selection)

		requireNoPublication(t, fixture)
	})

	t.Run("unsupported intermediate operator publishes nothing", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		planCtx := fixture.stmt.Ctx.(*mock.Context)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		scan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		projection := physicalop.PhysicalProjection{}.Init(planCtx, &property.StatsInfo{RowCount: 1}, 0)
		projection.SetChildren(scan)
		reader.TablePlan = projection
		reader.TablePlans = physicalop.FlattenListPushDownPlan(projection)
		fixture.recordReaderScanDetail(reader, 1, 1, 10)
		setPlan(fixture, reader)

		requireNoPublication(t, fixture)
	})
}

func TestStatementRUFinalOutcomeFirstRecordWins(t *testing.T) {
	t.Run("nil owner is off", func(t *testing.T) {
		stmt := &ExecStmt{}
		require.NotPanics(t, func() {
			stmt.RecordStatementRUFinalOutcome(true)
			stmt.finishStatementRUForTest(nil)
		})
	})

	t.Run("owner observation distinguishes pending and consumed setup", func(t *testing.T) {
		stmt, _ := newStatementRUOwnerForTest()
		observation := ObserveStatementRUOwnerForTest(stmt)
		require.NotNil(t, observation)
		require.False(t, observation.ConsumedForTest())

		stmt.RecordStatementRUFinalOutcome(false)
		require.True(t, observation.ConsumedForTest())
	})

	t.Run("owner observation fails closed for zero initial setup", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		owner.calculationSetup = statementRUCalculationSetup{}
		observation := ObserveStatementRUOwnerForTest(stmt)
		require.NotNil(t, observation)
		require.False(t, observation.ConsumedForTest())

		stmt.RecordStatementRUFinalOutcome(false)
		require.False(t, observation.ConsumedForTest())
	})

	t.Run("unknown terminal consumes once", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("recorded failure consumes once", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(false)
		stmt.RecordStatementRUFinalOutcome(true)
		require.Equal(t, statementRUFinalOutcomeFailure, statementRUFinalOutcome(owner.finalOutcome.Load()))
		require.Zero(t, owner.calculationSetup)
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	for _, tc := range []struct {
		name         string
		firstSuccess bool
		second       bool
		wantOutcome  statementRUFinalOutcome
	}{
		{name: "success then success", firstSuccess: true, second: true, wantOutcome: statementRUFinalOutcomeSuccess},
		{name: "success then failure", firstSuccess: true, second: false, wantOutcome: statementRUFinalOutcomeSuccess},
		{name: "failure then success", firstSuccess: false, second: true, wantOutcome: statementRUFinalOutcomeFailure},
		{name: "failure then failure", firstSuccess: false, second: false, wantOutcome: statementRUFinalOutcomeFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, owner := newStatementRUOwnerForTest()
			stmt.RecordStatementRUFinalOutcome(tc.firstSuccess)
			stmt.RecordStatementRUFinalOutcome(tc.second)
			require.Equal(t, tc.wantOutcome, statementRUFinalOutcome(owner.finalOutcome.Load()))
			stmt.finishStatementRUForTest(nil)
			require.Zero(t, owner.calculationSetup)
		})
	}
}

func TestStatementRUTerminalFirstCallWins(t *testing.T) {
	t.Run("recordSet SQLKiller reaches terminal", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
		t.Cleanup(func() { stmt.Ctx.GetSessionVars().SQLKiller.Reset() })
		rs := &recordSet{stmt: stmt}

		require.Error(t, rs.Next(context.Background(), nil))
		require.Empty(t, rs.lastErrs, "the RU-only abort must not change legacy terminal errors")
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("recordSet recovered panic reaches terminal", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		rs := &recordSet{stmt: stmt}
		ctx := &statementRUPanicOnceContext{Context: context.Background()}

		require.Error(t, rs.Next(ctx, nil))
		require.Empty(t, rs.lastErrs, "the RU-only abort must not change legacy terminal errors")
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("terminal error then success", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(errors.New("terminal error"))
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("deadline then success", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(context.DeadlineExceeded)
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("restricted then success", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().InRestrictedSQL = true
		stmt.finishStatementRUForTest(nil)
		stmt.Ctx.GetSessionVars().InRestrictedSQL = false
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("cursor then success", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.Ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
		stmt.finishStatementRUForTest(nil)
		stmt.Ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, false)
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("nil plan then plan", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		plan := stmt.Plan
		stmt.Plan = nil
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		stmt.Plan = plan
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("empty plan then plan", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		plan := stmt.Plan
		stmt.Plan = &physicalop.Insert{}
		stmt.Ctx.GetSessionVars().StmtCtx.SetPlan(stmt.Plan)
		stmt.Ctx.GetSessionVars().StmtCtx.SetFlatPlan(nil)
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		stmt.Plan = plan
		stmt.finishStatementRUForTest(nil)
		require.Zero(t, owner.calculationSetup)
	})

	t.Run("success then terminal error", func(t *testing.T) {
		stmt, owner := newStatementRUOwnerForTest()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		stmt.finishStatementRUForTest(errors.New("late terminal error"))
		require.Zero(t, owner.calculationSetup)
	})
}

func TestStatementRUTerminalUsesStmtCtxFlatPlanCache(t *testing.T) {
	ctx := mock.NewContext()
	stats := &property.StatsInfo{RowCount: 1}
	stalePlan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	stalePlan.SetID(101)
	currentPlan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	currentPlan.SetID(202)

	ctx.GetSessionVars().StmtCtx.SetPlan(currentPlan)
	ctx.GetSessionVars().StmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(stalePlan, false))

	stmt := &ExecStmt{
		Ctx:  ctx,
		Plan: currentPlan,
	}
	owner := newStatementRUOwner(stmt)
	owner.calculationSetup.frontendCompileBytes = 1
	stmt.statementRUOwner = owner
	stmt.RecordStatementRUFinalOutcome(true)
	stmt.finishStatementRUForTest(nil)
	// This intentionally characterizes the current getFlatPlan contract. It does
	// not prove that the cached Origin belongs to the current ExecStmt generation.
	flat := ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
	require.Same(t, stalePlan, flat.Main[0].Origin)
	require.Zero(t, owner.calculationSetup)
}

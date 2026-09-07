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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

const (
	statementRUSimpleSelectSQLForTest      = "select * from t"
	statementRUCalibrationFailpointForTest = "github.com/pingcap/tidb/pkg/executor/observeStatementRUCalibrationUnitsForTest"
)

type statementRUSimpleSelectFixture struct {
	stmt  *ExecStmt
	owner *statementRUOwner
}

func (a *ExecStmt) finishStatementRUForTest(terminalErr error) {
	a.finishStatementRU(terminalErr)
}

func newStatementRUSimpleSelectFixture(t testing.TB) statementRUSimpleSelectFixture {
	t.Helper()
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	ctx.GetSessionVars().StmtCtx.IsReadOnly = true
	planPartInfo := &physicalop.PhysPlanPartInfo{}
	scan := (&physicalop.PhysicalTableScan{
		Table:        &model.TableInfo{},
		StoreType:    kv.TiKV,
		PlanPartInfo: planPartInfo,
	}).Init(ctx, 0)
	reader := (&physicalop.PhysicalTableReader{
		TablePlan:    scan,
		TablePlans:   []base.PhysicalPlan{scan},
		StoreType:    kv.TiKV,
		PlanPartInfo: planPartInfo,
	}).Init(ctx, 0)
	selectStmt := &ast.SelectStmt{Kind: ast.SelectStmtKindSelect}
	selectStmt.SetText(nil, statementRUSimpleSelectSQLForTest)
	stmt := &ExecStmt{
		Ctx:      ctx,
		GoCtx:    context.Background(),
		Plan:     reader,
		StmtNode: selectStmt,
	}
	ctx.GetSessionVars().StmtCtx.SetPlan(reader)
	installStatementRUOwner(stmt)
	require.NotNil(t, stmt.statementRUOwner)
	owner := stmt.statementRUOwner
	ctx.GetSessionVars().StmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(reader, false))

	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RecordCopStats(
		reader.TablePlan.ID(),
		kv.TiKV,
		&util.ScanDetail{
			TotalKeys:         1,
			ProcessedKeys:     1,
			ProcessedKeysSize: 10,
		},
		util.TimeDetail{},
		nil,
		nil,
	)
	metrics := execdetails.NewRUV2Metrics()
	metrics.AddTiKVCoprocessorResponseBytes(20)
	ctx.GetSessionVars().RUV2Metrics = metrics
	stmt.recordStatementRURootEOF()
	return statementRUSimpleSelectFixture{stmt: stmt, owner: owner}
}

func (fixture statementRUSimpleSelectFixture) mergeStatementScanDetail(detail *util.ScanDetail) {
	fixture.stmt.Ctx.GetSessionVars().StmtCtx.MergeCopExecDetails(&execdetails.CopExecDetails{ScanDetail: detail}, 0)
}

func (fixture statementRUSimpleSelectFixture) recordReaderScanDetail(
	reader *physicalop.PhysicalTableReader,
	totalKeys, processedKeys, processedKeysSize int64,
) {
	fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RecordCopStats(
		reader.TablePlan.ID(),
		reader.StoreType,
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

func observeStatementRUCalibrationForTest(
	t testing.TB,
	observe func(statementRUCalibrationSnapshot),
) {
	t.Helper()
	testfailpoint.EnableCall(t, statementRUCalibrationFailpointForTest, func(
		_ uint64,
		stateName string,
		cpuWork, scanBytes, netBytes, frontendCompileBytes, hashStateRows, joinOutputRows float64,
	) {
		state := statementRUCalibrationUnknown
		switch stateName {
		case statementRUCalibrationComplete.String():
			state = statementRUCalibrationComplete
		case statementRUCalibrationIncomplete.String():
			state = statementRUCalibrationIncomplete
		default:
			require.FailNow(t, "unexpected calibration state", stateName)
		}
		observe(statementRUCalibrationSnapshot{
			State: state,
			Units: statementRURawUnits{
				CPUWork:              cpuWork,
				ScanBytes:            scanBytes,
				NetBytes:             netBytes,
				FrontendCompileBytes: frontendCompileBytes,
				HashStateRows:        hashStateRows,
				JoinOutputRows:       joinOutputRows,
			},
		})
	})
}

func TestStatementRUResultFinalizationAndPublication(t *testing.T) {
	fixture := newStatementRUSimpleSelectFixture(t)
	var calibrationCount atomic.Int64
	var snapshot statementRUCalibrationSnapshot
	totalBefore := testutil.ToFloat64(metrics.RUV3Total)
	readBefore := testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead))
	tikvBefore := testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))
	var totalAtCalibration float64
	observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
		calibrationCount.Add(1)
		snapshot = published
		totalAtCalibration = testutil.ToFloat64(metrics.RUV3Total) - totalBefore
	})

	fixture.stmt.RecordStatementRUFinalOutcome(true)
	const callers = 32
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			fixture.stmt.finishStatementRUForTest(nil)
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), calibrationCount.Load())
	require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
	require.Equal(t, statementRURawUnits{
		ScanBytes:            10,
		NetBytes:             20,
		FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
	}, snapshot.Units)
	expectedResult := calculateStatementRUResultOnly(snapshot.Units)
	require.Equal(t, expectedResult.TotalRU, totalAtCalibration)
	require.Equal(t, expectedResult.TotalRU, testutil.ToFloat64(metrics.RUV3Total)-totalBefore)
	require.Equal(t, expectedResult.TotalRU,
		testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead))-readBefore)
	require.Equal(t, snapshot.Units.ScanBytes+snapshot.Units.NetBytes,
		testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore)
	require.Zero(t, fixture.owner.calculationSetup)

	fixture.stmt.finishStatementRUForTest(nil)
	require.Equal(t, int64(1), calibrationCount.Load())
	require.Equal(t, expectedResult.TotalRU, testutil.ToFloat64(metrics.RUV3Total)-totalBefore)
	require.Equal(t, expectedResult.TotalRU,
		testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead))-readBefore)
	require.Equal(t, snapshot.Units.ScanBytes+snapshot.Units.NetBytes,
		testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore)
	require.Equal(t, float64(10), snapshot.Units.ScanBytes)
	require.Equal(t, float64(20), snapshot.Units.NetBytes)
}

func TestStatementRUResultProjectionCompleteness(t *testing.T) {
	t.Run("frontend missing is zero only for ResultOnly", func(t *testing.T) {
		finalized, ok := (statementRUCalculator{units: statementRURawUnits{
			ScanBytes: 10,
			NetBytes:  20,
		}}).finalize()
		require.True(t, ok)
		require.Equal(t, statementRUResultOnly{TotalRU: 30}, finalized.result)
		require.Equal(t, statementRUCalibrationIncomplete, finalized.calibrationState)
		require.Zero(t, finalized.units.FrontendCompileBytes)
	})

	t.Run("scan missing contributes zero to best effort result", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
		var snapshot statementRUCalibrationSnapshot
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			snapshot = published
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, float64(35), testutil.ToFloat64(metrics.RUV3Total)-totalBefore)
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Zero(t, snapshot.Units.ScanBytes)
		require.Equal(t, float64(20), snapshot.Units.NetBytes)
	})

	t.Run("net missing contributes zero to best effort result", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.stmt.Ctx.GetSessionVars().RUV2Metrics = execdetails.NewRUV2Metrics()
		var snapshot statementRUCalibrationSnapshot
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			snapshot = published
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, float64(25), testutil.ToFloat64(metrics.RUV3Total)-totalBefore)
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Equal(t, float64(10), snapshot.Units.ScanBytes)
		require.Zero(t, snapshot.Units.NetBytes)
	})

	t.Run("runtime stats missing contributes zero to best effort result", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = nil
		var snapshot statementRUCalibrationSnapshot
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			snapshot = published
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, float64(35), testutil.ToFloat64(metrics.RUV3Total)-totalBefore)
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Zero(t, snapshot.Units.CPUWork)
		require.Zero(t, snapshot.Units.ScanBytes)
		require.Equal(t, float64(20), snapshot.Units.NetBytes)
	})

	t.Run("early close suppresses RU v3 metrics", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.owner.rootEOF.Store(false)
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)

		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV3Total))
		require.Zero(t, calibrationCount.Load())
	})

	t.Run("invalid evidence suppresses both publications", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.recordReaderScanDetail(fixture.stmt.Plan.(*physicalop.PhysicalTableReader), 0, 0, -11)
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)

		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV3Total))
		require.Zero(t, calibrationCount.Load())
		require.Zero(t, fixture.owner.calculationSetup)
	})

	t.Run("terminal error publishes no uninitialized snapshot", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(context.Canceled)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV3Total))
		require.Zero(t, calibrationCount.Load())
		require.Zero(t, fixture.owner.calculationSetup)
	})

	t.Run("terminal hook panic publishes no snapshot", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.SetFlatPlan("invalid flat plan test value")
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		require.NotPanics(t, func() { fixture.stmt.finishStatementRUForTest(nil) })
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV3Total))
		require.Zero(t, calibrationCount.Load())
		require.Zero(t, fixture.owner.calculationSetup)
	})
}

func TestStatementRUPublisherIsolation(t *testing.T) {
	t.Run("calibration panic is isolated", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			panic("calibration")
		})
		require.NotPanics(t, func() {
			publishStatementRUCalibrationSafely(fixture.stmt, statementRUCalibrationSnapshot{State: statementRUCalibrationComplete})
		})
	})
}

func TestStatementRUResultValueContracts(t *testing.T) {
	t.Run("calculator finalizes typed units without plan input", func(t *testing.T) {
		calculator := statementRUCalculator{
			units: statementRURawUnits{
				CPUWork:              5,
				ScanBytes:            10,
				NetBytes:             20,
				FrontendCompileBytes: 15,
				HashStateRows:        7,
				JoinOutputRows:       8,
			},
		}
		finalized, ok := calculator.finalize()
		require.True(t, ok)
		require.Equal(t, statementRUResultOnly{TotalRU: 65}, finalized.result)
		require.Equal(t, statementRUCalibrationIncomplete, finalized.calibrationState)
	})

	t.Run("placeholder formula stays pinned", func(t *testing.T) {
		units := statementRURawUnits{
			CPUWork: 5, ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15,
			HashStateRows: 7, JoinOutputRows: 8,
		}
		require.Equal(t, statementRUResultOnly{TotalRU: 65}, calculateStatementRUResultOnly(units))
	})

	t.Run("operator unit arithmetic preserves Join and Agg units", func(t *testing.T) {
		baseUnits := statementRURawUnits{
			CPUWork: 1, ScanBytes: 2, NetBytes: 3, FrontendCompileBytes: 4,
			HashStateRows: 5, JoinOutputRows: 6,
		}
		delta := statementRURawUnits{
			CPUWork: 7, ScanBytes: 8, NetBytes: 9, FrontendCompileBytes: 10,
			HashStateRows: 11, JoinOutputRows: 12,
		}
		combined := addStatementRURawUnits(baseUnits, delta)
		require.Equal(t, statementRURawUnits{
			CPUWork: 8, ScanBytes: 10, NetBytes: 12, FrontendCompileBytes: 14,
			HashStateRows: 16, JoinOutputRows: 18,
		}, combined)
		require.Equal(t, delta, subtractStatementRURawUnits(combined, baseUnits))
	})

	t.Run("engine projection preserves the lower layer boundary", func(t *testing.T) {
		units := statementRURawUnits{CPUWork: 5, ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15}
		finalized := statementRUFinalizedSnapshot{
			units:  units,
			result: calculateStatementRUResultOnly(units),
		}
		tikvBefore := testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))
		publishStatementRUMetricsSafely(finalized)
		require.Equal(t, float64(30),
			testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore)
	})

	t.Run("publisher uses the frozen snapshot after live evidence changes", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		sessVars := fixture.stmt.Ctx.GetSessionVars()
		flat := sessVars.StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		finalized, ok := calculateStatementRU(
			flat,
			sessVars.StmtCtx.RuntimeStatsColl,
			sessVars.RUV2Metrics,
			fixture.owner.calculationSetup,
			true,
		)
		require.True(t, ok)
		require.Equal(t, float64(10), finalized.units.ScanBytes)
		require.Equal(t, float64(20), finalized.units.NetBytes)

		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		fixture.recordReaderScanDetail(reader, 9, 3, 30)
		sessVars.RUV2Metrics.AddTiKVCoprocessorResponseBytes(100)
		liveDetail, found := sessVars.StmtCtx.RuntimeStatsColl.GetCopScanDetail(reader.TablePlan.ID())
		require.True(t, found)
		liveScanEvidence := classifyStatementRUScanEvidence(
			liveDetail.TotalKeys,
			liveDetail.ProcessedKeys,
			liveDetail.ProcessedKeysSize,
		)
		require.Equal(t, statementRUScanEvidenceValid, liveScanEvidence.state)
		require.NotEqual(t, finalized.units.ScanBytes, liveScanEvidence.scanBytes)
		require.NotEqual(t, finalized.units.NetBytes, float64(sessVars.RUV2Metrics.TiKVCoprocessorResponseBytes()))

		var calibrationCount atomic.Int64
		var snapshot statementRUCalibrationSnapshot
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
			snapshot = published
		})
		totalBefore := testutil.ToFloat64(metrics.RUV3Total)
		publishStatementRUFinalizedSnapshot(fixture.stmt, finalized)

		require.Equal(t, int64(1), calibrationCount.Load())
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Equal(t, finalized.units, snapshot.Units)
		require.Equal(t, finalized.result.TotalRU, testutil.ToFloat64(metrics.RUV3Total)-totalBefore)
	})

	t.Run("scan evidence has one valid unavailable invalid classification", func(t *testing.T) {
		evidence := classifyStatementRUScanEvidence(10, 2, 6)
		require.Equal(t, statementRUScanEvidenceValid, evidence.state)
		require.Equal(t, float64(30), evidence.scanBytes)

		evidence = classifyStatementRUScanEvidence(10, 0, 0)
		require.Equal(t, statementRUScanEvidenceValid, evidence.state)
		require.Zero(t, evidence.scanBytes)

		require.Equal(t, statementRUScanEvidenceInvalid, classifyStatementRUScanEvidence(10, 0, 1).state)
		require.Equal(t, statementRUScanEvidenceInvalid, classifyStatementRUScanEvidence(-1, 1, 1).state)
		require.Equal(t, statementRUScanEvidenceUnavailable, classifyStatementRUScanEvidence(0, 1, 1).state)
		require.Equal(t, statementRUScanEvidenceUnavailable, classifyStatementRUScanEvidence(1, 1, 0).state)
	})

	t.Run("finalized and published payloads contain no live references", func(t *testing.T) {
		for _, value := range []any{
			statementRUCalculator{},
			statementRUOperatorResult{},
			statementRURawUnits{},
			statementRUFinalizedSnapshot{},
			statementRUResultOnly{},
			statementRUCalibrationSnapshot{},
		} {
			requireStatementRUValueOnlyType(t, reflect.TypeOf(value))
		}
	})

	t.Run("publication contracts contain only approved scalar fields", func(t *testing.T) {
		calculatorType := reflect.TypeOf(statementRUCalculator{})
		require.Equal(t, []string{
			"units",
		}, statementRUFieldNames(calculatorType))
		unitsType := reflect.TypeOf(statementRURawUnits{})
		require.Equal(t, []string{
			"CPUWork", "ScanBytes", "NetBytes", "FrontendCompileBytes", "HashStateRows", "JoinOutputRows",
		}, statementRUFieldNames(unitsType))
		resultType := reflect.TypeOf(statementRUResultOnly{})
		require.Equal(t, []string{"TotalRU"}, statementRUFieldNames(resultType))
		snapshotType := reflect.TypeOf(statementRUCalibrationSnapshot{})
		require.Equal(t, []string{"State", "Units"}, statementRUFieldNames(snapshotType))
		require.Equal(t, unitsType, snapshotType.Field(1).Type)
	})
}

func statementRUFieldNames(valueType reflect.Type) []string {
	names := make([]string, valueType.NumField())
	for i := range valueType.NumField() {
		names[i] = valueType.Field(i).Name
	}
	return names
}

func requireStatementRUValueOnlyType(t *testing.T, valueType reflect.Type) {
	t.Helper()
	for i := range valueType.NumField() {
		fieldType := valueType.Field(i).Type
		if fieldType.Kind() == reflect.Struct {
			requireStatementRUValueOnlyType(t, fieldType)
			continue
		}
		require.NotContains(t, []reflect.Kind{
			reflect.Chan,
			reflect.Func,
			reflect.Interface,
			reflect.Map,
			reflect.Pointer,
			reflect.Slice,
			reflect.UnsafePointer,
		}, fieldType.Kind())
	}
}

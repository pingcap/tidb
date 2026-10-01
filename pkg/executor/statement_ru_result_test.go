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
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/resourcegroup/ruv2"
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
	// Existing unit fixtures observe full-mode units as well as engine results.
	owner.calculationSetup.fullReport = true
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
		cpuWork, scanBytes, netBytes, frontendCompileBytes, hashStateRows, joinOutputRows, writeStatement, operatorNum, writeKeys, writeBytes, crossAZNetBytes float64,
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
			Units: ruv2.StmtUnits{
				CPUWork:              cpuWork,
				ScanBytes:            scanBytes,
				NetBytes:             netBytes,
				CrossAZNetBytes:      crossAZNetBytes,
				FrontendCompileBytes: frontendCompileBytes,
				HashStateRows:        hashStateRows,
				JoinOutputRows:       joinOutputRows,
				WriteStatement:       writeStatement, OperatorNum: operatorNum, WriteKeys: writeKeys, WriteBytes: writeBytes,
			},
		})
	})
}

func TestStatementRUResultFinalizationAndPublication(t *testing.T) {
	fixture := newStatementRUSimpleSelectFixture(t)
	var calibrationCount atomic.Int64
	var snapshot statementRUCalibrationSnapshot
	totalBefore := testutil.ToFloat64(metrics.RUV2Total)
	readBefore := testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("select"))
	tikvBefore := testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(metrics.LblEngineTiKV))
	var totalAtCalibration float64
	observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
		calibrationCount.Add(1)
		snapshot = published
		totalAtCalibration = testutil.ToFloat64(metrics.RUV2Total) - totalBefore
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
	require.Equal(t, ruv2.StmtUnits{
		OperatorNum:          2,
		ScanBytes:            10,
		NetBytes:             20,
		FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
	}, snapshot.Units)
	expectedResult, valid := ruv2.Calculate(snapshot.Units, ruv2.DefaultWeights())
	require.True(t, valid)
	require.InDelta(t, expectedResult.TotalRU, totalAtCalibration, 1e-9)
	require.InDelta(t, expectedResult.TotalRU, testutil.ToFloat64(metrics.RUV2Total)-totalBefore, 1e-9)
	require.InDelta(t, expectedResult.TotalRU,
		testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("select"))-readBefore, 1e-9)
	require.InDelta(t, snapshot.Units.ScanBytes+snapshot.Units.NetBytes+1,
		testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore, 1e-9)
	require.Zero(t, fixture.owner.calculationSetup)

	fixture.stmt.finishStatementRUForTest(nil)
	require.Equal(t, int64(1), calibrationCount.Load())
	require.InDelta(t, expectedResult.TotalRU, testutil.ToFloat64(metrics.RUV2Total)-totalBefore, 1e-9)
	require.InDelta(t, expectedResult.TotalRU,
		testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("select"))-readBefore, 1e-9)
	require.InDelta(t, snapshot.Units.ScanBytes+snapshot.Units.NetBytes+1,
		testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore, 1e-9)
	require.Equal(t, float64(10), snapshot.Units.ScanBytes)
	require.Equal(t, float64(20), snapshot.Units.NetBytes)
}

func TestStatementRUResultProjectionCompleteness(t *testing.T) {
	t.Run("frontend missing is zero only for ResultOnly", func(t *testing.T) {
		finalized, ok := (&statementRUCalculator{units: ruv2.StmtUnits{
			ScanBytes: 10,
			NetBytes:  20,
		}}).finalize()
		require.True(t, ok)
		require.Equal(t, ruv2.StmtResult{TotalRU: 30}, finalized.result)
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
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.InDelta(t, float64(37), testutil.ToFloat64(metrics.RUV2Total)-totalBefore, 1e-9)
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
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.InDelta(t, float64(27), testutil.ToFloat64(metrics.RUV2Total)-totalBefore, 1e-9)
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
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.InDelta(t, float64(37), testutil.ToFloat64(metrics.RUV2Total)-totalBefore, 1e-9)
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
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)

		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV2Total))
		require.Zero(t, calibrationCount.Load())
	})

	t.Run("invalid evidence suppresses both publications", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.recordReaderScanDetail(fixture.stmt.Plan.(*physicalop.PhysicalTableReader), 0, 0, -11)
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)

		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV2Total))
		require.Zero(t, calibrationCount.Load())
		require.Zero(t, fixture.owner.calculationSetup)
	})

	t.Run("terminal error publishes no uninitialized snapshot", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(context.Canceled)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV2Total))
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
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		require.NotPanics(t, func() { fixture.stmt.finishStatementRUForTest(nil) })
		fixture.stmt.finishStatementRUForTest(nil)
		require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV2Total))
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

func TestStatementRUUsesConfig(t *testing.T) {
	t.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.RUV2.StmtWeights.CPUWork = 2
		cfg.RUV2.StmtWeights.ScanByte = 3
	})

	calculator := statementRUCalculator{units: ruv2.StmtUnits{CPUWork: 5, ScanBytes: 7}}
	calculator.recordOperatorUnits(statementRUTiDB, calculator.units)
	finalized, ok := calculator.finalize()
	require.True(t, ok)
	require.Equal(t, float64(31), finalized.result.TotalRU) // 2*5 + 3*7
	require.Equal(t, statementRUEngineResult{TiDB: 10, TiKV: 21}, finalized.engineRU)

	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.RUV2.StmtWeights.CPUWork = 0
	})
	finalized, ok = calculator.finalize()
	require.True(t, ok)
	require.Equal(t, float64(21), finalized.result.TotalRU) // 0*5 + 3*7
	require.Equal(t, statementRUEngineResult{TiKV: 21}, finalized.engineRU)

	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.RUV2.StmtWeights = ruv2.StmtWeights{
			CPUWork: 2, ScanByte: 3, NetByte: 5, FrontendCompileByte: 7,
			HashStateRow: 11, JoinOutputRow: 13, WriteStatement: 17,
			OperatorNum: 19, WriteKey: 23, WriteByte: 29,
		}
	})
	for _, full := range []bool{false, true} {
		calculator := newStatementRUCalculator(statementRUCalculationSetup{frontendCompileBytes: 11, fullReport: full})
		calculator.units.WriteStatement = 1
		calculator.units.WriteKeys = 31
		calculator.units.WriteBytes = 37
		for engine, units := range []ruv2.StmtUnits{
			{CPUWork: 2, HashStateRows: 3, JoinOutputRows: 5, OperatorNum: 7},
			{CPUWork: 13, HashStateRows: 17, OperatorNum: 19, ScanBytes: 23, NetBytes: 29},
		} {
			calculator.units = calculator.units.Add(units)
			calculator.recordOperatorUnits(statementRUEngine(engine), units)
			if full {
				calculator.report.addOperator(statementRUEngine(engine), statementRUHashAgg, units)
			}
		}
		finalized, ok := calculator.finalize()
		require.True(t, ok)
		require.Equal(t, statementRUEngineResult{TiDB: 329, TiKV: 2574}, finalized.engineRU)
		require.Equal(t, float64(2903), finalized.result.TotalRU)
		if full {
			requireStatementRUReportConservation(t, finalized)
		}
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)
		tidbBefore := testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues("tidb"))
		tikvBefore := testutil.ToFloat64(metrics.RUV2ByEngineTiKV)
		publishStatementRUMetricsSafely(&finalized)
		require.InDelta(t, 2903, testutil.ToFloat64(metrics.RUV2Total)-totalBefore, 1e-9)
		require.InDelta(t, 329, testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues("tidb"))-tidbBefore, 1e-9)
		require.InDelta(t, 2574, testutil.ToFloat64(metrics.RUV2ByEngineTiKV)-tikvBefore, 1e-9)
	}
}

func TestStatementRUResultValueContracts(t *testing.T) {
	t.Run("pointer calculation preserves prior terminal bits", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		base := statementRUCalculator{
			units: ruv2.StmtUnits{CPUWork: 6, HashStateRows: 6, OperatorNum: 6,
				JoinOutputRows: 6, ScanBytes: 8, NetBytes: 9, CrossAZNetBytes: 1,
				FrontendCompileBytes: 4, WriteStatement: 1, WriteKeys: 5, WriteBytes: 7},
			compute: [statementRUEngineCount]statementRUComputeUnits{
				{cpuWork: 1, hashStateRows: 1, operatorNum: 1, joinOutputRows: 1},
				{cpuWork: 2, hashStateRows: 2, operatorNum: 2, joinOutputRows: 2},
				{cpuWork: 3, hashStateRows: 3, operatorNum: 3, joinOutputRows: 3,
					scanBytes: 3, netBytes: 3, crossAZNetBytes: 1},
			},
		}
		check := func(calculator statementRUCalculator, weights ruv2.StmtWeights) {
			t.Helper()
			// Each call replaces the config, so a stale weights snapshot is detected.
			config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = weights })
			for _, full := range []bool{false, true} {
				calculator.report = nil
				if full {
					calculator.report = new(statementRUFullReport)
					calculator.report.add(statementRUTiDB, statementRUProjection, ruv2.StmtUnits{CPUWork: 11})
				}
				before := calculator
				var reportBefore statementRUFullReport
				if full {
					reportBefore = *calculator.report
				}
				want, wantOK := statementRUReferenceFinalize(calculator, weights)
				got, ok := calculator.finalize()
				require.Equal(t, wantOK, ok)
				requireStatementRUBitsEqual(t, want, got)
				requireStatementRUBitsEqual(t, before, calculator)
				if full {
					requireStatementRUBitsEqual(t, reportBefore, *calculator.report)
					if ok {
						require.NotSame(t, calculator.report, got.report)
						calculator.report.units[statementRUTiDB][statementRUProjection].CPUWork = 99
						requireStatementRUBitsEqual(t, want, got)
					}
				}
			}
		}
		weights := ruv2.DefaultWeights()
		weights.CrossAZNetByte = 2
		check(base, weights)
		for _, value := range []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64,
			math.Nextafter(1, 0), math.Nextafter(1, 2), 1 << 53, math.MaxFloat64,
			-math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1), math.NaN()} {
			for i := range reflect.TypeFor[ruv2.StmtUnits]().NumField() {
				calculator := base
				reflect.ValueOf(&calculator.units).Elem().Field(i).SetFloat(value)
				check(calculator, weights)
			}
			for engine := range statementRUEngineCount {
				for field := range 7 {
					calculator := base
					compute := &calculator.compute[engine]
					fields := []*float64{&compute.cpuWork, &compute.hashStateRows, &compute.operatorNum,
						&compute.joinOutputRows, &compute.scanBytes, &compute.netBytes, &compute.crossAZNetBytes}
					*fields[field] = value
					check(calculator, weights)
				}
			}
			for i := range reflect.TypeFor[ruv2.StmtWeights]().NumField() {
				changed := weights
				reflect.ValueOf(&changed).Elem().Field(i).SetFloat(value)
				check(base, changed)
				check(statementRUCalculator{}, changed)
			}
		}
		negativeZero := statementRUCalculator{}
		for i := range reflect.TypeFor[ruv2.StmtUnits]().NumField() {
			reflect.ValueOf(&negativeZero.units).Elem().Field(i).SetFloat(math.Copysign(0, -1))
		}
		check(negativeZero, weights)
		check(statementRUCalculator{units: ruv2.StmtUnits{CPUWork: 1 << 53, ScanBytes: 1, NetBytes: 1}}, weights)
		check(statementRUCalculator{units: ruv2.StmtUnits{CPUWork: math.SmallestNonzeroFloat64}}, ruv2.StmtWeights{CPUWork: 0.5})
	})

	t.Run("frontend compile bytes follow plan cache hits", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		vars := fixture.stmt.Ctx.GetSessionVars()
		for _, originalSQL := range []string{"", statementRUSimpleSelectSQLForTest} {
			vars.StmtCtx.OriginalSQL = originalSQL
			for _, hit := range []bool{false, true, false} {
				vars.FoundInPlanCache = hit
				bytes := statementRUFrontendCompileBytes(fixture.stmt)
				if hit {
					require.Zero(t, bytes)
				} else {
					require.Positive(t, bytes)
				}
			}
		}
	})

	t.Run("calculator finalizes typed units without plan input", func(t *testing.T) {
		calculator := statementRUCalculator{
			units: ruv2.StmtUnits{
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
		require.Equal(t, ruv2.StmtResult{TotalRU: 65}, finalized.result)
		require.Equal(t, statementRUCalibrationIncomplete, finalized.calibrationState)
	})

	t.Run("write units and reporting snapshot", func(t *testing.T) {
		units := ruv2.StmtUnits{WriteStatement: 1, OperatorNum: 3, WriteKeys: 2, WriteBytes: 100}
		result, valid := ruv2.Calculate(units, ruv2.DefaultWeights())
		require.True(t, valid)
		require.Equal(t, float64(106), result.TotalRU)
		doubledUnits := units.Add(units)
		require.Equal(t, units, doubledUnits.Sub(units))
		for _, field := range []string{"WriteStatement", "OperatorNum", "WriteKeys", "WriteBytes"} {
			invalid := units
			reflect.ValueOf(&invalid).Elem().FieldByName(field).SetFloat(-1)
			require.False(t, invalid.Valid(), field)
		}
		m := execdetails.NewRUV2Metrics()
		details := &util.CommitDetails{WriteKeys: 3, WriteSize: 150}
		writes := snapshotStatementRUWrites(details)
		require.Equal(t, statementRUWriteSnapshot{keys: 3, bytes: 150}, writes)
		details.WriteKeys = 9
		details.WriteSize = 900
		require.Equal(t, statementRUWriteSnapshot{keys: 3, bytes: 150}, writes)
		require.Zero(t, snapshotStatementRUWrites(nil))
		ctx := mock.NewContext()
		plan := physicalop.Insert{}.Init(ctx)
		coll := execdetails.NewRuntimeStatsColl(nil)
		coll.RegisterStats(plan.ID(), &execdetails.WriteRuntimeStats{})
		flat := plannercore.FlattenPhysicalPlan(plan, false)
		// Committed writes come from the snapshot even when RUv2 metrics are absent.
		for _, metricsInput := range []*execdetails.RUV2Metrics{m, nil} {
			result, ok := calculateStatementRU(flat, coll, metricsInput, writes, statementRUCalculationSetup{}, true)
			require.True(t, ok)
			require.Equal(t, float64(3), result.units.WriteKeys)
			require.Equal(t, float64(150), result.units.WriteBytes)
		}
		writeBefore := testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("commit"))
		tikvBefore := testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(metrics.LblEngineTiKV))
		calculator := statementRUCalculator{units: units, report: new(statementRUFullReport)}
		calculator.recordOperatorUnits(statementRUTiDB, units)
		finalized, ok := calculator.finalize()
		require.True(t, ok)
		finalized.sqlType = "commit"
		publishStatementRUMetricsSafely(&finalized)
		require.InDelta(t, finalized.result.TotalRU,
			testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("commit"))-writeBefore, 1e-9)
		require.InDelta(t, finalized.result.TotalRU-4,
			testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore, 1e-9)
	})

	t.Run("empty commit has no physical operators or DML charge", func(t *testing.T) {
		ctx := mock.NewContext()
		node := &ast.CommitStmt{}
		node.SetText(nil, "commit")
		stmt := &ExecStmt{Ctx: ctx, GoCtx: context.Background(), StmtNode: node,
			Plan: &plannercore.Simple{Statement: node}}
		installStatementRUOwner(stmt)
		require.NotNil(t, stmt.statementRUOwner)
		stmt.statementRUOwner.calculationSetup.fullReport = true
		var snapshot statementRUCalibrationSnapshot
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) { snapshot = published })
		writeBefore := testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("commit"))
		readBefore := testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("select"))
		stmt.recordStatementRURootEOF()
		stmt.RecordStatementRUFinalOutcome(true)
		stmt.finishStatementRUForTest(nil)
		stmt.finishStatementRUForTest(nil)
		require.Equal(t, ruv2.StmtUnits{FrontendCompileBytes: 6}, snapshot.Units)
		require.InDelta(t, float64(6), testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("commit"))-writeBefore, 1e-9)
		require.Equal(t, readBefore, testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues("select")))
	})

	t.Run("placeholder formula stays pinned", func(t *testing.T) {
		units := ruv2.StmtUnits{
			CPUWork: 5, ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15,
			HashStateRows: 7, JoinOutputRows: 8,
		}
		result, valid := ruv2.Calculate(units, ruv2.DefaultWeights())
		require.True(t, valid)
		require.Equal(t, ruv2.StmtResult{TotalRU: 65}, result)
	})

	t.Run("operator unit arithmetic preserves Join and Agg units", func(t *testing.T) {
		baseUnits := ruv2.StmtUnits{
			CPUWork: 1, ScanBytes: 2, NetBytes: 3, FrontendCompileBytes: 4,
			HashStateRows: 5, JoinOutputRows: 6,
		}
		delta := ruv2.StmtUnits{
			CPUWork: 7, ScanBytes: 8, NetBytes: 9, FrontendCompileBytes: 10,
			HashStateRows: 11, JoinOutputRows: 12,
		}
		combined := baseUnits.Add(delta)
		require.Equal(t, ruv2.StmtUnits{
			CPUWork: 8, ScanBytes: 10, NetBytes: 12, FrontendCompileBytes: 14,
			HashStateRows: 16, JoinOutputRows: 18,
		}, combined)
		require.Equal(t, delta, combined.Sub(baseUnits))
	})

	t.Run("engine projection preserves the lower layer boundary", func(t *testing.T) {
		units := ruv2.StmtUnits{CPUWork: 5, ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15}
		calculator := statementRUCalculator{units: units}
		calculator.recordOperatorUnits(statementRUTiDB, units)
		finalized, ok := calculator.finalize()
		require.True(t, ok)
		tikvBefore := testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(metrics.LblEngineTiKV))
		publishStatementRUMetricsSafely(&finalized)
		require.InDelta(t, float64(30),
			testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore, 1e-9)
	})

	t.Run("publisher uses the frozen snapshot after live evidence changes", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		sessVars := fixture.stmt.Ctx.GetSessionVars()
		flat := sessVars.StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		finalized, ok := calculateStatementRU(
			flat,
			sessVars.StmtCtx.RuntimeStatsColl,
			sessVars.RUV2Metrics,
			snapshotStatementRUWrites(sessVars.StmtCtx.GetExecDetails().CommitDetail),
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
		totalBefore := testutil.ToFloat64(metrics.RUV2Total)
		publishStatementRUFinalizedSnapshot(fixture.stmt, &finalized)

		require.Equal(t, int64(1), calibrationCount.Load())
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Equal(t, finalized.units, snapshot.Units)
		require.InDelta(t, finalized.result.TotalRU, testutil.ToFloat64(metrics.RUV2Total)-totalBefore, 1e-9)
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
			ruv2.StmtUnits{},
			statementRUFinalizedSnapshot{},
			ruv2.StmtResult{},
			statementRUCalibrationSnapshot{},
		} {
			requireStatementRUValueOnlyType(t, reflect.TypeOf(value))
		}
	})

	t.Run("publication contracts contain only approved scalar fields", func(t *testing.T) {
		calculatorType := reflect.TypeOf(statementRUCalculator{})
		require.Equal(t, []string{
			"units", "compute", "report",
		}, statementRUFieldNames(calculatorType))
		unitsType := reflect.TypeOf(ruv2.StmtUnits{})
		require.Equal(t, []string{
			"WriteStatement", "OperatorNum", "WriteKeys", "WriteBytes",
			"CPUWork", "ScanBytes", "NetBytes", "CrossAZNetBytes", "FrontendCompileBytes", "HashStateRows", "JoinOutputRows",
		}, statementRUFieldNames(unitsType))
		resultType := reflect.TypeOf(ruv2.StmtResult{})
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
		if fieldType == reflect.TypeOf((*statementRUFullReport)(nil)) {
			fieldType = fieldType.Elem()
		}
		for fieldType.Kind() == reflect.Array {
			fieldType = fieldType.Elem()
		}
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

func TestStatementRUTTLJobEligibility(t *testing.T) {
	for _, tc := range []struct {
		name       string
		restricted bool
		source     string
		jobID      string
		eligible   bool
		ttl        bool
	}{
		{"ttl job", true, kv.InternalTxnTTL, "job-1", true, true},
		{"global ttl", true, kv.InternalTxnTTL, "", false, false},
		{"other internal", true, kv.InternalTxnOthers, "job-1", false, false},
		{"external", false, kv.InternalTxnTTL, "job-1", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newStatementRUSimpleSelectFixture(t)
			stmt := fixture.stmt
			vars := stmt.Ctx.GetSessionVars()
			flat := vars.StmtCtx.GetFlatPlan()
			vars.StmtCtx.SetFlatPlan(nil)
			vars.InRestrictedSQL = tc.restricted
			vars.RequestSourceType = tc.source
			vars.TTLJobID = tc.jobID
			stmt.statementRUOwner = nil
			installStatementRUOwner(stmt)
			vars.StmtCtx.SetFlatPlan(flat)
			require.Equal(t, tc.eligible, stmt.statementRUOwner != nil)
			// The original attribution survives session restoration before terminal.
			vars.TTLJobID = ""
			vars.InRestrictedSQL = false
			stmt.recordStatementRURootEOF()
			stmt.RecordStatementRUFinalOutcome(true)
			before := testutil.ToFloat64(metrics.RUV2Total)
			ttlBefore := testutil.ToFloat64(metrics.RUV2TTLTotal)
			stmt.finishStatementRU(nil)
			delta := testutil.ToFloat64(metrics.RUV2Total) - before
			ttlDelta := testutil.ToFloat64(metrics.RUV2TTLTotal) - ttlBefore
			if tc.eligible {
				require.Positive(t, delta)
			} else {
				require.Zero(t, delta)
			}
			if tc.ttl {
				require.InDelta(t, delta, ttlDelta, 1e-9)
			} else {
				require.Zero(t, ttlDelta)
			}
			stmt.finishStatementRU(nil)
			require.Equal(t, ttlBefore+ttlDelta, testutil.ToFloat64(metrics.RUV2TTLTotal))
		})
	}
}

// These reference helpers retain the value-based arithmetic before the pointer experiment.
func statementRUReferenceCalculate(units ruv2.StmtUnits, weights ruv2.StmtWeights) (ruv2.StmtResult, bool) {
	valid := func(value any) bool {
		fields := reflect.ValueOf(value)
		for i := range fields.NumField() {
			v := fields.Field(i).Float()
			if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
				return false
			}
		}
		return true
	}
	if units.CrossAZNetBytes > units.NetBytes || !valid(units) || !valid(weights) {
		return ruv2.StmtResult{}, false
	}
	totalRU := weights.CPUWork*units.CPUWork +
		weights.ScanByte*units.ScanBytes +
		weights.NetByte*units.NetBytes +
		weights.CrossAZNetByte*units.CrossAZNetBytes +
		weights.FrontendCompileByte*units.FrontendCompileBytes +
		weights.HashStateRow*units.HashStateRows +
		weights.JoinOutputRow*units.JoinOutputRows +
		weights.WriteStatement*units.WriteStatement +
		weights.OperatorNum*units.OperatorNum +
		weights.WriteKey*units.WriteKeys +
		weights.WriteByte*units.WriteBytes
	if totalRU < 0 || math.IsNaN(totalRU) || math.IsInf(totalRU, 0) {
		return ruv2.StmtResult{}, false
	}
	return ruv2.StmtResult{TotalRU: totalRU}, true
}

func statementRUReferenceEngineResult(calculator statementRUCalculator, weights ruv2.StmtWeights) statementRUEngineResult {
	tidb, tikv, tiflash := calculator.compute[statementRUTiDB], calculator.compute[statementRUTiKV], calculator.compute[statementRUTiFlash]
	units := calculator.units
	return statementRUEngineResult{
		TiDB: weights.CPUWork*tidb.cpuWork + weights.HashStateRow*tidb.hashStateRows +
			weights.OperatorNum*tidb.operatorNum + weights.JoinOutputRow*(units.JoinOutputRows-tiflash.joinOutputRows) +
			weights.FrontendCompileByte*units.FrontendCompileBytes + weights.WriteStatement*units.WriteStatement,
		TiKV: weights.CPUWork*tikv.cpuWork + weights.HashStateRow*tikv.hashStateRows +
			weights.OperatorNum*tikv.operatorNum + weights.ScanByte*(units.ScanBytes-tiflash.scanBytes) +
			weights.NetByte*(units.NetBytes-tiflash.netBytes) + weights.WriteKey*units.WriteKeys + weights.WriteByte*units.WriteBytes,
		TiFlash: statementRUReferenceTiFlashRU(calculator, weights),
	}
}

func statementRUReferenceTiFlashRU(calculator statementRUCalculator, weights ruv2.StmtWeights) float64 {
	tiflash := calculator.compute[statementRUTiFlash]
	return weights.CPUWork*tiflash.cpuWork + weights.HashStateRow*tiflash.hashStateRows +
		weights.OperatorNum*tiflash.operatorNum + weights.JoinOutputRow*tiflash.joinOutputRows +
		weights.ScanByte*tiflash.scanBytes + weights.NetByte*tiflash.netBytes + weights.CrossAZNetByte*tiflash.crossAZNetBytes
}

func statementRUReferenceFinalize(calculator statementRUCalculator, weights ruv2.StmtWeights) (statementRUFinalizedSnapshot, bool) {
	result, ok := statementRUReferenceCalculate(calculator.units, weights)
	if !ok {
		return statementRUFailed(statementRUOperatorInvalid), false
	}
	engineRU := statementRUReferenceEngineResult(calculator, weights)
	// TotalRU already includes the original TiFlash RU, so add only the extra
	// (multiplier - 1) copies: total - original TiFlash RU + scaled TiFlash RU.
	result.TotalRU += engineRU.TiFlash * (statementRUTiFlashMultiplier - 1)
	// Keep the per-engine value consistent with the adjusted statement total.
	engineRU.TiFlash *= statementRUTiFlashMultiplier
	for _, ru := range [...]float64{result.TotalRU, engineRU.TiDB, engineRU.TiKV, engineRU.TiFlash} {
		if ru < 0 || math.IsNaN(ru) || math.IsInf(ru, 0) {
			return statementRUFailed(statementRUOperatorInvalid), false
		}
	}
	frozenReport := calculator.report
	if frozenReport != nil {
		// Freeze full-mode details independently of the mutable accumulator.
		report := *frozenReport
		report.addStatementUnits(calculator.units)
		frozenReport = &report
	}
	return statementRUFinalizedSnapshot{
		units:            calculator.units,
		result:           result,
		engineRU:         engineRU,
		report:           frozenReport,
		calibrationState: statementRUCalibrationIncomplete,
		sqlType:          "select",
	}, true
}

// Compare all nested floats by bits, including signed zero and NaN payloads.
func requireStatementRUBitsEqual(t *testing.T, want, got any) {
	t.Helper()
	var compare func(reflect.Value, reflect.Value)
	compare = func(want, got reflect.Value) {
		require.Equal(t, want.Type(), got.Type())
		switch want.Kind() {
		case reflect.Float64:
			require.Equal(t, math.Float64bits(want.Float()), math.Float64bits(got.Float()))
		case reflect.Struct:
			for i := range want.NumField() {
				compare(want.Field(i), got.Field(i))
			}
		case reflect.Array:
			for i := range want.Len() {
				compare(want.Index(i), got.Index(i))
			}
		case reflect.Pointer:
			require.Equal(t, want.IsNil(), got.IsNil())
			if !want.IsNil() {
				compare(want.Elem(), got.Elem())
			}
		case reflect.Bool:
			require.Equal(t, want.Bool(), got.Bool())
		case reflect.String:
			require.Equal(t, want.String(), got.String())
		case reflect.Uint8:
			require.Equal(t, want.Uint(), got.Uint())
		default:
			t.Fatalf("unhandled comparison kind %v", want.Kind())
		}
	}
	compare(reflect.ValueOf(want), reflect.ValueOf(got))
}

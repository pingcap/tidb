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

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/resourcegroup/ruv2"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
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
	setStatementRUFullReportForTest(owner, true)
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
	require.Zero(t, fixture.owner.calculationSetup())

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
		require.Zero(t, fixture.owner.calculationSetup())
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
		require.Zero(t, fixture.owner.calculationSetup())
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
		require.Zero(t, fixture.owner.calculationSetup())
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

type statementRUTextCountingStmt struct {
	ast.StmtNode
	originalTextCalls int
}

func (stmt *statementRUTextCountingStmt) OriginalText() string {
	stmt.originalTextCalls++
	return stmt.StmtNode.OriginalText()
}

type statementRUPointScalarCountingStats struct {
	kind        int
	stats       util.PointResponseStats
	reads       *atomic.Int64
	panicOnRead bool
}

func (stats *statementRUPointScalarCountingStats) String() string { return "" }
func (stats *statementRUPointScalarCountingStats) Tp() int {
	if stats.kind != 0 {
		return stats.kind
	}
	return execdetails.TpRuntimeStatsWithSnapshot
}
func (stats *statementRUPointScalarCountingStats) Clone() execdetails.RuntimeStats {
	cloned := *stats
	return &cloned
}
func (stats *statementRUPointScalarCountingStats) Merge(other execdetails.RuntimeStats) {
	if other, ok := other.(*statementRUPointScalarCountingStats); ok {
		stats.stats.Merge(other.stats)
	}
}
func (stats *statementRUPointScalarCountingStats) GetPointResponseStats() util.PointResponseStats {
	stats.reads.Add(1)
	if stats.panicOnRead {
		panic("point scalar evidence read")
	}
	return stats.stats
}

// This fixture setter is used only before first terminal/concurrent use.
func setStatementRUInstallFlagForTest(owner *statementRUOwner, flag uint8, enabled bool) {
	owner.installFlags &^= flag
	if enabled {
		owner.installFlags |= flag
	}
}

// This fixture setter is used only before first terminal/concurrent use.
func setStatementRUFullReportForTest(owner *statementRUOwner, full bool) {
	owner.calculationFullReport = full
	setStatementRUInstallFlagForTest(owner, statementRUInstallFull, full)
}

func TestStatementRUResultValueContracts(t *testing.T) {
	t.Run("single concrete snapshot projection", testStatementRUSingleSnapshot)
	t.Run("result terminal agrees with legacy terminal across lifecycle and current plan", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = ruv2.DefaultWeights() })
		for _, kind := range []string{"point", "batch", "reader", "commit"} {
			for _, state := range []string{"success", "wrapped", "unknown", "failed", "aborted", "no EOF", "error", "nil plan", "nil context", "restricted", "cursor", "TTL"} {
				t.Run(kind+"/"+state, func(t *testing.T) {
					type observation struct {
						total   float64
						ru      [3]float64
						calls   int
						group   string
						metrics [3]float64
					}
					run := func(legacy bool) observation {
						fixture := newStatementRUSimpleSelectFixture(t)
						// Install occurred with a Reader: classify this new plan at terminal.
						setStatementRUFullReportForTest(fixture.owner, false)
						vars := fixture.stmt.Ctx.GetSessionVars()
						sqlType := "select"
						switch kind {
						case "point", "batch":
							plan := newStatementRUPointLookupPlanForTest(fixture, kind == "batch")
							fixture.stmt.Plan = plan
							var reads atomic.Int64
							vars.StmtCtx.RuntimeStatsColl.RegisterStats(plan.ID(), &statementRUPointScalarCountingStats{
								reads: &reads,
								stats: statementRUPointResponseStatsForTestFromResponse(&kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 50}, 19),
							})
						case "commit":
							fixture.stmt.Plan = &plannercore.Simple{Statement: &ast.CommitStmt{}}
							sqlType = "commit"
						}
						if state == "wrapped" {
							fixture.stmt.Plan = &plannercore.Execute{Plan: &plannercore.Explain{Analyze: true, TargetPlan: fixture.stmt.Plan}}
						}
						reporter := &statementRUReporterForTest{}
						fixture.stmt.Ctx = &statementRUReportingContextForTest{Context: fixture.stmt.Ctx.(*mock.Context), reporter: reporter}
						vars.StmtCtx.ResourceGroupName = "result-terminal-test"
						var terminalErr error
						switch state {
						case "unknown": // Preserve the unknown outcome.
						case "failed":
							fixture.stmt.RecordStatementRUFinalOutcome(false)
						default:
							fixture.stmt.RecordStatementRUFinalOutcome(true)
						}
						switch state {
						case "aborted":
							fixture.stmt.abortStatementRU()
						case "no EOF":
							fixture.owner.rootEOF.Store(false)
						case "error":
							terminalErr = context.Canceled
						case "nil plan":
							fixture.stmt.Plan = nil
						case "nil context":
							fixture.stmt.Ctx = nil
						case "restricted":
							vars.InRestrictedSQL = true
						case "cursor":
							setStatementRUInstallFlagForTest(fixture.owner, statementRUInstallCursor, true)
						case "TTL":
							setStatementRUInstallFlagForTest(fixture.owner, statementRUInstallTTL, true)
							vars.InRestrictedSQL = true
						}
						before := [3]float64{testutil.ToFloat64(metrics.RUV2Total), testutil.ToFloat64(metrics.RUV2TTLTotal), testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues(sqlType))}
						var total float64
						if legacy {
							total = fixture.stmt.finishStatementRUWithSnapshot(fixture.owner, terminalErr)
						} else {
							total = fixture.stmt.finishStatementRU(terminalErr)
						}
						require.Zero(t, fixture.owner.calculationSetup())
						require.False(t, fixture.owner.fullReportAtInstall())
						require.Zero(t, fixture.stmt.finishStatementRU(nil))
						after := [3]float64{testutil.ToFloat64(metrics.RUV2Total), testutil.ToFloat64(metrics.RUV2TTLTotal), testutil.ToFloat64(metrics.RUV2BySQLType.WithLabelValues(sqlType))}
						for i := range after {
							after[i] -= before[i]
						}
						if state == "success" || state == "wrapped" || state == "TTL" {
							require.Positive(t, total)
							require.Equal(t, 1, reporter.calls)
						} else {
							require.Zero(t, total)
							require.Zero(t, reporter.calls)
						}
						return observation{total, reporter.ru, reporter.calls, reporter.group, after}
					}
					want, got := run(true), run(false)
					require.Equal(t, want, got)
					requireStatementRUBitsEqual(t, want.total, got.total)
					requireStatementRUBitsEqual(t, want.ru, got.ru)
				})
			}
		}
	})

	t.Run("commit result projection preserves finalizer bits", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		for _, weight := range []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, 1, math.MaxFloat64, -1, math.Inf(1), math.NaN()} {
			weights := ruv2.DefaultWeights()
			weights.WriteByte = weight
			config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = weights })
			for _, writes := range []statementRUWriteSnapshot{{}, {keys: 3, bytes: 150}, {keys: 1, bytes: 1<<53 + 1}} {
				setup := statementRUCalculationSetup{frontendCompileBytes: 6}
				calculator := newStatementRUCalculator(setup)
				calculator.units.WriteKeys, calculator.units.WriteBytes = float64(writes.keys), float64(writes.bytes)
				want, wantOK := statementRUReferenceFinalize(calculator, weights)
				got, ok := calculateStatementRUCommitResult(writes, setup)
				require.Equal(t, wantOK, ok)
				if ok {
					require.Equal(t, "commit", got.sqlType)
					requireStatementRUBitsEqual(t, want.result, got.result)
					requireStatementRUBitsEqual(t, want.engineRU, got.engineRU)
				} else {
					require.Equal(t, statementRUResultSnapshot{}, got)
				}
			}
		}
	})

	t.Run("point payload preserves multiple provider order and coverage", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = ruv2.DefaultWeights() })
		complete := statementRUPointResponseStatsForTestFromResponse(&kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 50}, 19)
		unavailable := statementRUPointResponseStatsForTestFromResponse(&kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2}, 19)
		incomplete := statementRUPointResponseStatsForTestFromResponse(nil, 19)
		invalid := complete
		invalid.Invalidate()
		overflow := statementRUPointResponseStatsForTestFromResponse(&kvrpcpb.ScanDetailV2{}, math.MaxUint64)
		for _, snapshots := range [][]util.PointResponseStats{
			nil, {{}}, {complete}, {complete, complete}, {{}, complete}, {complete, {}},
			{unavailable}, {incomplete}, {complete, incomplete}, {incomplete, complete},
			{invalid, complete}, {complete, invalid}, {overflow, complete, complete},
			{{PayloadBytes: 1}},
		} {
			coll := execdetails.NewRuntimeStatsColl(nil)
			var reads atomic.Int64
			for i, snapshot := range snapshots {
				coll.RegisterStats(1, &statementRUPointScalarCountingStats{kind: -1001 - i, stats: snapshot, reads: &reads})
			}
			// Distinct provider types prevent registration from pre-merging them.
			if len(snapshots) != 0 {
				root, exists := coll.GetRootStatsIfExists(1)
				require.True(t, exists)
				_, groups := root.MergeStats()
				require.Len(t, groups, len(snapshots))
				for i, group := range groups {
					require.Equal(t, -1001-i, group.Tp())
				}
			}
			want, wantOK := calculateStatementRUPointLookup(1, coll, nil, statementRUCalculationSetup{frontendCompileBytes: 17}, true)
			wantReads := reads.Swap(0)
			got, ok := calculateStatementRUPointPayload(1, coll, nil, 17, true)
			require.Equal(t, wantOK, ok)
			require.Equal(t, wantReads, reads.Load())
			requireStatementRUBitsEqual(t, want.result.TotalRU, got.totalRU)
			requireStatementRUBitsEqual(t, want.engineRU, got.engineRU)
		}
		coll := execdetails.NewRuntimeStatsColl(nil)
		coll.RegisterStats(1, (*runtimeStatsWithSnapshot)(nil))
		got, ok := calculateStatementRUPointPayload(1, coll, nil, 0, true)
		require.False(t, ok)
		requireStatementRUBitsEqual(t, statementRUPointPayload{}, got)
	})

	t.Run("point payload publication retains signed TiFlash zero and TTL", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		for _, batch := range []bool{false, true} {
			for _, ttl := range []bool{false, true} {
				for _, cpuZero := range []float64{math.Copysign(0, -1), 0} {
					weights := ruv2.StmtWeights{}
					for field := range reflect.TypeFor[ruv2.StmtWeights]().NumField() {
						reflect.ValueOf(&weights).Elem().Field(field).SetFloat(math.Copysign(0, -1))
					}
					weights.CPUWork, weights.FrontendCompileByte, weights.WriteKey = cpuZero, 1, 1
					config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = weights })
					fixture := newStatementRUSimpleSelectFixture(t)
					plan := newStatementRUPointLookupPlanForTest(fixture, batch)
					fixture.stmt.Plan = plan
					fixture.owner.setCalculationSetup(statementRUCalculationSetup{frontendCompileBytes: 1})
					setStatementRUInstallFlagForTest(fixture.owner, statementRUInstallFull, false)
					setStatementRUInstallFlagForTest(fixture.owner, statementRUInstallTTL, ttl)
					baseCtx := fixture.stmt.Ctx.(*mock.Context)
					reporter := &statementRUReporterForTest{}
					fixture.stmt.Ctx = &statementRUReportingContextForTest{Context: baseCtx, reporter: reporter}
					baseCtx.GetSessionVars().StmtCtx.ResourceGroupName = "payload-test"
					var calibrations atomic.Int64
					observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) { calibrations.Add(1) })
					before := testutil.ToFloat64(metrics.RUV2Total)
					beforeTTL := testutil.ToFloat64(metrics.RUV2TTLTotal)
					fixture.stmt.RecordStatementRUFinalOutcome(true)
					require.Equal(t, float64(1), fixture.stmt.finishStatementRU(nil))
					require.Zero(t, fixture.stmt.finishStatementRU(nil))
					require.Equal(t, 1, reporter.calls)
					require.Equal(t, "payload-test", reporter.group)
					requireStatementRUBitsEqual(t, [3]float64{0, 1, cpuZero}, reporter.ru)
					require.Equal(t, before+1, testutil.ToFloat64(metrics.RUV2Total))
					if ttl {
						beforeTTL++
					}
					require.Equal(t, beforeTTL, testutil.ToFloat64(metrics.RUV2TTLTotal))
					require.Zero(t, calibrations.Load())
					require.Zero(t, fixture.owner.calculationSetup())
				}
			}
		}
	})

	t.Run("point scalar result preserves prior terminal bits", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		base := ruv2.StmtUnits{OperatorNum: 1, FrontendCompileBytes: 4, ScanBytes: 75, NetBytes: 48}
		check := func(units ruv2.StmtUnits, weights ruv2.StmtWeights) {
			t.Helper()
			config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = weights })
			want, wantOK := statementRUReferenceFinalize(statementRUCalculator{
				units:   units,
				compute: [statementRUEngineCount]statementRUComputeUnits{{operatorNum: 1}},
			}, weights)
			got, ok := finalizeStatementRUPointPayload(units.ScanBytes, units.NetBytes, units.FrontendCompileBytes)
			require.Equal(t, wantOK, ok)
			if !ok {
				requireStatementRUBitsEqual(t, statementRUPointPayload{}, got)
				return
			}
			requireStatementRUBitsEqual(t, want.result.TotalRU, got.totalRU)
			requireStatementRUBitsEqual(t, want.engineRU, got.engineRU)
		}
		weights := ruv2.DefaultWeights()
		check(base, weights)
		for _, value := range []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64,
			math.Nextafter(1, 0), math.Nextafter(1, 2), 1 << 53, math.MaxFloat64,
			-math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1), math.NaN()} {
			for field := range 3 {
				units := base
				fields := []*float64{&units.FrontendCompileBytes, &units.ScanBytes, &units.NetBytes}
				*fields[field] = value
				check(units, weights)
			}
			for field := range reflect.TypeFor[ruv2.StmtWeights]().NumField() {
				changed := weights
				reflect.ValueOf(&changed).Elem().Field(field).SetFloat(value)
				check(base, changed)
				check(ruv2.StmtUnits{OperatorNum: 1}, changed)
			}
		}
		// Exercise combinations of independently rounded weighted products.
		state := uint64(0x91e10da5c79e7b1d)
		next := func() float64 {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			bits := state & 0x7fffffffffffffff
			if bits>>52 == 0x7ff {
				bits ^= uint64(1) << 52
			}
			return math.Float64frombits(bits)
		}
		for range 256 {
			changed := weights
			for field := range reflect.TypeFor[ruv2.StmtWeights]().NumField() {
				reflect.ValueOf(&changed).Elem().Field(field).SetFloat(next())
			}
			check(ruv2.StmtUnits{OperatorNum: 1, ScanBytes: next(), NetBytes: next(), FrontendCompileBytes: next()}, changed)
		}
		// Engine totals must not be summed to construct the statement total.
		check(ruv2.StmtUnits{OperatorNum: 1, ScanBytes: 1 << 53, FrontendCompileBytes: 17}, weights)
		for _, signS := range []float64{0, math.Copysign(0, -1)} {
			for _, signN := range []float64{0, math.Copysign(0, -1)} {
				for _, signF := range []float64{0, math.Copysign(0, -1)} {
					check(ruv2.StmtUnits{OperatorNum: 1, ScanBytes: signS, NetBytes: signN, FrontendCompileBytes: signF}, weights)
				}
			}
		}
		negativeWeights := weights
		for field := range reflect.TypeFor[ruv2.StmtWeights]().NumField() {
			reflect.ValueOf(&negativeWeights).Elem().Field(field).SetFloat(math.Copysign(0, -1))
		}
		for _, operatorWeight := range []float64{math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, 1} {
			negativeWeights.OperatorNum = operatorWeight
			check(base, negativeWeights)
			check(ruv2.StmtUnits{OperatorNum: 1}, negativeWeights)
			// Exercise the real direct caller with the same signed-zero weights.
			config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = negativeWeights })
			for _, full := range []bool{false, true} {
				got, ok := calculateStatementRUPointLookup(1, nil, nil, statementRUCalculationSetup{fullReport: full}, true)
				want, wantOK := statementRUReferenceFinalize(statementRUCalculator{
					units:   ruv2.StmtUnits{OperatorNum: 1},
					compute: [statementRUEngineCount]statementRUComputeUnits{{operatorNum: 1}},
				}, negativeWeights)
				require.Equal(t, wantOK, ok)
				payload, payloadOK := calculateStatementRUPointPayload(1, nil, nil, 0, true)
				require.Equal(t, wantOK, payloadOK)
				requireStatementRUBitsEqual(t, want.result.TotalRU, payload.totalRU)
				requireStatementRUBitsEqual(t, want.engineRU, payload.engineRU)
				requireStatementRUBitsEqual(t, want.units, got.units)
				requireStatementRUBitsEqual(t, want.result, got.result)
				requireStatementRUBitsEqual(t, want.engineRU, got.engineRU)
				if full {
					requireStatementRUReportConservation(t, got)
				} else {
					require.Nil(t, got.report)
				}
			}
		}
		check(ruv2.StmtUnits{OperatorNum: 1, FrontendCompileBytes: 1 << 53, ScanBytes: 1, NetBytes: 1}, weights)
		check(ruv2.StmtUnits{OperatorNum: 1, FrontendCompileBytes: math.SmallestNonzeroFloat64}, ruv2.StmtWeights{FrontendCompileByte: 0.5, OperatorNum: math.SmallestNonzeroFloat64})
		check(ruv2.StmtUnits{OperatorNum: 1, ScanBytes: math.MaxFloat64, NetBytes: math.MaxFloat64}, weights)
	})

	t.Run("point scalar preserves collection and failure order", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = ruv2.DefaultWeights() })
		complete := statementRUPointResponseStatsForTestFromResponse(&kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 50}, 19)
		unsupported := statementRUPointResponseStatsForTestFromResponse(nil, 19)
		invalid := complete
		invalid.Invalidate()
		for _, tc := range []struct {
			name        string
			planID      int
			eof         bool
			bytes       int64
			bypass      bool
			frontend    float64
			stats       util.PointResponseStats
			panicOnRead bool
			wantFailure statementRUFailureReason
			wantReads   int64
		}{
			{name: "plan before EOF and metrics", planID: 0, bytes: -1, stats: complete, panicOnRead: true, wantFailure: statementRUInvalid},
			{name: "EOF before metrics and stats", planID: 1, bytes: -1, stats: complete, panicOnRead: true, wantFailure: statementRUNotFinished},
			{name: "metrics before provider", planID: 1, eof: true, bytes: -1, stats: complete, panicOnRead: true, wantFailure: statementRUInvalid},
			{name: "unsupported before frontend", planID: 1, eof: true, frontend: math.NaN(), stats: unsupported, wantFailure: statementRUUnsupported, wantReads: 1},
			{name: "bypass ignores negative metrics", planID: 1, eof: true, bytes: -1, bypass: true, stats: complete, wantReads: 1},
			{name: "bypass still checks coverage", planID: 1, eof: true, bytes: -1, bypass: true, stats: unsupported, wantFailure: statementRUUnsupported, wantReads: 1},
			{name: "invalid after provider", planID: 1, eof: true, stats: invalid, wantFailure: statementRUInvalid, wantReads: 1},
			{name: "frontend checked after evidence", planID: 1, eof: true, frontend: math.NaN(), stats: complete, wantFailure: statementRUInvalid, wantReads: 1},
			{name: "complete retains cop bytes", planID: 1, eof: true, bytes: 29, frontend: 17, stats: complete, wantReads: 1},
			{name: "zero provider retains cop bytes", planID: 1, eof: true, bytes: 29, frontend: 17, wantReads: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, full := range []bool{false, true} {
					metrics := execdetails.NewRUV2Metrics()
					metrics.AddTiKVCoprocessorResponseBytes(tc.bytes)
					metrics.SetBypass(tc.bypass)
					var reads atomic.Int64
					coll := execdetails.NewRuntimeStatsColl(nil)
					coll.RegisterStats(1, &statementRUPointScalarCountingStats{stats: tc.stats, reads: &reads, panicOnRead: tc.panicOnRead})
					setup := statementRUCalculationSetup{frontendCompileBytes: tc.frontend, fullReport: full}
					got, ok := calculateStatementRUPointPayload(tc.planID, coll, metrics, tc.frontend, tc.eof)
					require.Equal(t, tc.wantFailure == "", ok)
					require.Equal(t, tc.wantReads, reads.Load())
					require.Equal(t, tc.bytes, metrics.TiKVCoprocessorResponseBytes())
					require.Equal(t, tc.bypass, metrics.Bypass())
					want, wantOK := calculateStatementRUPointLookup(tc.planID, coll, metrics, setup, tc.eof)
					require.Equal(t, tc.wantFailure, want.failure)
					require.Equal(t, wantOK, ok)
					requireStatementRUBitsEqual(t, want.result.TotalRU, got.totalRU)
					requireStatementRUBitsEqual(t, want.engineRU, got.engineRU)
					require.Equal(t, 2*tc.wantReads, reads.Load())
				}
			})
		}
	})

	t.Run("point scalar keeps separate byte conversions", func(t *testing.T) {
		t.Cleanup(config.RestoreFunc())
		weights := ruv2.DefaultWeights()
		config.UpdateGlobal(func(cfg *config.Config) { cfg.RUV2.StmtWeights = weights })
		for _, tc := range []struct {
			cop     int64
			payload uint64
		}{
			{29, 19}, {1<<53 + 1, 1}, {1<<53 - 1, 3}, {math.MaxInt64, math.MaxUint64},
		} {
			metrics := execdetails.NewRUV2Metrics()
			metrics.AddTiKVCoprocessorResponseBytes(tc.cop)
			stats := statementRUPointResponseStatsForTestFromResponse(&kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 50}, tc.payload)
			coll := execdetails.NewRuntimeStatsColl(nil)
			coll.RegisterStats(1, &statementRUPointResponseStatsForTest{stats: stats})
			units := ruv2.StmtUnits{OperatorNum: 1, FrontendCompileBytes: 17, ScanBytes: 75, NetBytes: float64(tc.cop) + float64(tc.payload)}
			want, wantOK := statementRUReferenceFinalize(statementRUCalculator{units: units, compute: [statementRUEngineCount]statementRUComputeUnits{{operatorNum: 1}}}, weights)
			require.True(t, wantOK)
			got, ok := calculateStatementRUPointPayload(1, coll, metrics, 17, true)
			require.True(t, ok)
			requireStatementRUBitsEqual(t, want.result.TotalRU, got.totalRU)
			requireStatementRUBitsEqual(t, want.engineRU, got.engineRU)
			if tc.cop == 1<<53+1 {
				require.NotEqual(t, math.Float64bits(float64(uint64(tc.cop)+tc.payload)), math.Float64bits(units.NetBytes))
			}
		}
	})

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
		node := &statementRUTextCountingStmt{StmtNode: fixture.stmt.StmtNode}
		fixture.stmt.StmtNode = node
		vars := fixture.stmt.Ctx.GetSessionVars()
		for _, originalSQL := range []string{"", statementRUSimpleSelectSQLForTest} {
			vars.StmtCtx.OriginalSQL = originalSQL
			for _, hit := range []bool{false, true, false} {
				vars.FoundInPlanCache = hit
				node.originalTextCalls = 0
				bytes := statementRUFrontendCompileBytes(fixture.stmt)
				if hit {
					require.Zero(t, bytes)
					require.Zero(t, node.originalTextCalls, "cache hits must not inspect SQL text for compile units")
				} else {
					require.Positive(t, bytes)
					require.Equal(t, 1, node.originalTextCalls)
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
		setStatementRUFullReportForTest(stmt.statementRUOwner, true)
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
			fixture.owner.calculationSetup(),
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
			statementRUResultSnapshot{},
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

// Shared by both comparison binaries; all assertions and setup stay untimed.
func prepareStatementRUCommitTerminalForBenchmark(b *testing.B, nonempty bool) *ExecStmt {
	b.Helper()
	b.Cleanup(config.RestoreFunc())
	config.UpdateGlobal(func(cfg *config.Config) {
		cfg.RUV2.ReportMode = config.RUReportModeResult
		cfg.RUV2.StmtWeights = ruv2.DefaultWeights()
	})
	ctx := mock.NewContext()
	node := &ast.CommitStmt{}
	node.SetText(nil, "commit")
	stmt := &ExecStmt{Ctx: ctx, GoCtx: context.Background(), StmtNode: node, Plan: &plannercore.Simple{Statement: node}}
	sc := ctx.GetSessionVars().StmtCtx
	sc.SetPlan(stmt.Plan)
	sc.SetFlatPlan(nil)
	var writes statementRUWriteSnapshot
	if nonempty {
		writes = statementRUWriteSnapshot{keys: 3, bytes: 150}
		sc.SyncExecDetails.MergeExecDetails(&util.CommitDetails{WriteKeys: 3, WriteSize: 150})
	}
	require.Equal(b, writes, snapshotStatementRUWrites(sc.GetExecDetails().CommitDetail))
	installStatementRUOwner(stmt)
	require.NotNil(b, stmt.statementRUOwner)
	require.False(b, stmt.statementRUOwner.calculationFullReport)
	require.Equal(b, float64(6), stmt.statementRUOwner.frontendCompileBytes)
	want, ok := statementRUReferenceFinalize(statementRUCalculator{units: ruv2.StmtUnits{
		FrontendCompileBytes: 6, WriteKeys: float64(writes.keys), WriteBytes: float64(writes.bytes),
	}}, ruv2.DefaultWeights())
	require.True(b, ok)
	stmt.recordStatementRURootEOF()
	stmt.RecordStatementRUFinalOutcome(true)
	require.Equal(b, want.result.TotalRU, stmt.finishStatementRU(nil))
	require.Zero(b, stmt.finishStatementRU(nil))
	require.Nil(b, sc.GetFlatPlan())
	return stmt
}

// Frozen owner-install-flags collector: independent of the candidate projection.
func statementRUSingleSnapshotReference(
	planID int,
	runtimeStatsColl *execdetails.RuntimeStatsColl,
) (float64, float64, statementRUOperatorState) {
	if runtimeStatsColl == nil {
		return 0, 0, statementRUOperatorComplete
	}
	rootStats, exists := runtimeStatsColl.GetRootStatsIfExists(planID)
	if !exists || rootStats == nil {
		return 0, 0, statementRUOperatorComplete
	}

	_, groups := rootStats.MergeStats()
	var aggregate util.PointResponseStats
	for _, group := range groups {
		stats, ok := statementRUPointResponseStatsSnapshot(group)
		if !ok {
			continue
		}
		if !mergeStatementRUPointResponseStats(&aggregate, stats) {
			return 0, 0, statementRUOperatorInvalid
		}
	}
	// No provider (an unexecuted point lookup) or a valid zero-value snapshot
	// (for example a transaction-buffer hit) contributes zero remote work.
	if !aggregate.PayloadComplete() {
		if aggregate.PayloadBytes != 0 ||
			aggregate.ScanDetail.TotalKeys != 0 || aggregate.ScanDetail.ProcessedKeys != 0 ||
			aggregate.ScanDetail.ProcessedKeysSize != 0 {
			return 0, 0, statementRUOperatorInvalid
		}
		return 0, 0, statementRUOperatorComplete
	}
	if !aggregate.ScanDetailComplete() {
		return 0, 0, statementRUOperatorUnsupported
	}

	scanEvidence := classifyStatementRUScanEvidence(
		aggregate.ScanDetail.TotalKeys,
		aggregate.ScanDetail.ProcessedKeys,
		aggregate.ScanDetail.ProcessedKeysSize,
	)
	switch scanEvidence.state {
	case statementRUScanEvidenceValid:

	case statementRUScanEvidenceUnavailable:
		// Complete response coverage does not give protobuf scalar fields
		// presence bits. Keep the best-effort value and add no scan bytes.
	default:
		return 0, 0, statementRUOperatorInvalid
	}
	return scanEvidence.scanBytes, float64(aggregate.PayloadBytes), statementRUOperatorComplete
}

// Obtain a real concrete SnapshotRuntimeStats through the public RPC path. Close
// the store before returning so its background work is outside benchmark timers.
func newStatementRUSingleSnapshotFixture(t testing.TB, scan *kvrpcpb.ScanDetailV2, detailsPresent, missingKey bool) *runtimeStatsWithSnapshot {
	t.Helper()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, store.Close())
		}
	})
	key, value := kv.Key("ru-single-snapshot"), []byte("payload")
	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Set(key, value))
	require.NoError(t, txn.Commit(context.Background()))
	stats := &txnsnapshot.SnapshotRuntimeStats{}
	snapshot := store.GetSnapshot(kv.MaxVersion)
	snapshot.SetOption(kv.CollectRuntimeStats, stats)
	var responses atomic.Int64
	snapshot.SetOption(kv.RPCInterceptor, interceptor.NewRPCInterceptor("statement-ru-single-snapshot", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, request *tikvrpc.Request) (*tikvrpc.Response, error) {
			response, err := next(target, request)
			if err == nil && response != nil {
				if get, ok := response.Resp.(*kvrpcpb.GetResponse); ok {
					responses.Add(1)
					get.ExecDetailsV2 = nil
					if detailsPresent {
						get.ExecDetailsV2 = &kvrpcpb.ExecDetailsV2{ScanDetailV2: scan}
					}
				}
			}
			return response, err
		}
	}))
	if missingKey {
		key = kv.Key("ru-single-snapshot-missing")
	}
	got, err := snapshot.Get(context.Background(), key)
	if missingKey {
		require.ErrorIs(t, err, kv.ErrNotExist)
	} else {
		require.NoError(t, err)
		require.Equal(t, value, got.Value)
	}
	require.Equal(t, int64(1), responses.Load())
	require.NoError(t, store.Close())
	closed = true
	return &runtimeStatsWithSnapshot{SnapshotRuntimeStats: stats}
}

func checkStatementRUSingleSnapshotOracle(t *testing.T, providers []execdetails.RuntimeStats, state statementRUOperatorState, scan, payload float64) {
	t.Helper()
	coll := execdetails.NewRuntimeStatsColl(nil)
	for _, provider := range providers {
		coll.RegisterStats(1001, provider)
	}
	wantScan, wantPayload, wantState := statementRUSingleSnapshotReference(1001, coll)
	gotScan, gotPayload, gotState := collectStatementRUPointPayload(1001, coll)
	require.Equal(t, state, wantState)
	require.Equal(t, math.Float64bits(scan), math.Float64bits(wantScan))
	require.Equal(t, math.Float64bits(payload), math.Float64bits(wantPayload))
	require.Equal(t, wantState, gotState)
	require.Equal(t, math.Float64bits(wantScan), math.Float64bits(gotScan))
	require.Equal(t, math.Float64bits(wantPayload), math.Float64bits(gotPayload))
	root, exists := coll.GetRootStatsIfExists(1001)
	if exists {
		_, groups := root.MergeStats()
		if len(groups) == 1 {
			if stats, ok := groups[0].(*runtimeStatsWithSnapshot); ok && stats != nil && stats.SnapshotRuntimeStats != nil {
				// Fixture creation, registration and premerging are outside the
				// measured closure. Guard steady per-call heap allocations, not
				// the terminal owner allocation or compiler escape diagnostics.
				allocs := testing.AllocsPerRun(1000, func() {
					gotScan, gotPayload, gotState = collectStatementRUPointPayload(1001, coll)
				})
				require.Zero(t, allocs, "single concrete snapshot collection must not allocate")
				require.Equal(t, wantState, gotState)
				require.Equal(t, math.Float64bits(wantScan), math.Float64bits(gotScan))
				require.Equal(t, math.Float64bits(wantPayload), math.Float64bits(gotPayload))
			}
		}
	}
	// No retention of a different root's current group.
	x, y, empty := collectStatementRUPointPayload(1002, coll)
	require.Equal(t, statementRUOperatorComplete, empty)
	require.Zero(t, x)
	require.Zero(t, y)
}

func testStatementRUSingleSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		scan                  *kvrpcpb.ScanDetailV2
		details, miss         bool
		state                 statementRUOperatorState
		wantScan, wantPayload float64
	}{
		{"complete", &kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 37}, true, false, statementRUOperatorComplete, 55.5, 7},
		{"known zero miss", &kvrpcpb.ScanDetailV2{}, true, true, statementRUOperatorComplete, 0, 0},
		{"unavailable scalar", &kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2}, true, false, statementRUOperatorComplete, 0, 7},
		{"contradictory", &kvrpcpb.ScanDetailV2{TotalVersions: 1, ProcessedVersions: 0, ProcessedVersionsSize: 37}, true, false, statementRUOperatorInvalid, 0, 0},
		{"processed exceeds total stays supported", &kvrpcpb.ScanDetailV2{TotalVersions: 1, ProcessedVersions: 2, ProcessedVersionsSize: 37}, true, false, statementRUOperatorComplete, 18.5, 7},
		{"missing scan", nil, true, false, statementRUOperatorUnsupported, 0, 0},
		{"missing details", nil, false, false, statementRUOperatorUnsupported, 0, 0},
		{"negative total", &kvrpcpb.ScanDetailV2{TotalVersions: math.MaxUint64, ProcessedVersions: 2, ProcessedVersionsSize: 37}, true, false, statementRUOperatorInvalid, 0, 0},
		{"negative processed", &kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: math.MaxUint64, ProcessedVersionsSize: 37}, true, false, statementRUOperatorInvalid, 0, 0},
		{"negative bytes", &kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: math.MaxUint64}, true, false, statementRUOperatorInvalid, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := newStatementRUSingleSnapshotFixture(t, tc.scan, tc.details, tc.miss)
			point := provider.GetPointResponseStats()
			require.True(t, point.IsValid())
			require.True(t, point.PayloadComplete())
			require.Equal(t, tc.details && tc.scan != nil, point.ScanDetailComplete())
			if tc.scan != nil {
				require.Equal(t, int64(tc.scan.TotalVersions), point.ScanDetail.TotalKeys)
				require.Equal(t, int64(tc.scan.ProcessedVersions), point.ScanDetail.ProcessedKeys)
				require.Equal(t, int64(tc.scan.ProcessedVersionsSize), point.ScanDetail.ProcessedKeysSize)
			}
			checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{provider}, tc.state, tc.wantScan, tc.wantPayload)
		})
	}
	t.Run("nil and zero concrete", func(t *testing.T) {
		checkStatementRUSingleSnapshotOracle(t, nil, statementRUOperatorComplete, 0, 0)
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{(*runtimeStatsWithSnapshot)(nil)}, statementRUOperatorInvalid, 0, 0)
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{&runtimeStatsWithSnapshot{}}, statementRUOperatorInvalid, 0, 0)
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{&runtimeStatsWithSnapshot{SnapshotRuntimeStats: &txnsnapshot.SnapshotRuntimeStats{}}}, statementRUOperatorComplete, 0, 0)
	})
	t.Run("current group includes premerged responses", func(t *testing.T) {
		complete := newStatementRUSingleSnapshotFixture(t, &kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 37}, true, false)
		second := complete.Clone()
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{complete, second}, statementRUOperatorComplete, 111, 14)
		missing := newStatementRUSingleSnapshotFixture(t, nil, true, false)
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{complete.Clone(), missing}, statementRUOperatorUnsupported, 0, 0)
	})
	t.Run("premerged and late overflow retain different failure order", func(t *testing.T) {
		a := newStatementRUSingleSnapshotFixture(t, &kvrpcpb.ScanDetailV2{TotalVersions: math.MaxInt64, ProcessedVersions: 1, ProcessedVersionsSize: 1}, true, false)
		b := a.Clone().(*runtimeStatsWithSnapshot)
		b.SnapshotRuntimeStats.Merge(a.SnapshotRuntimeStats)
		missing := newStatementRUSingleSnapshotFixture(t, nil, true, false)
		b.SnapshotRuntimeStats.Merge(missing.SnapshotRuntimeStats)
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{b}, statementRUOperatorInvalid, 0, 0)
		var reads atomic.Int64
		first := &statementRUPointScalarCountingStats{kind: -4001, stats: a.GetPointResponseStats(), reads: &reads}
		last := &statementRUPointScalarCountingStats{kind: -4002, stats: a.GetPointResponseStats(), reads: &reads}
		last.stats.Merge(missing.GetPointResponseStats())
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{first, last}, statementRUOperatorUnsupported, 0, 0)
		require.Equal(t, int64(4), reads.Load())
	})
	t.Run("single custom fallback and panic order", func(t *testing.T) {
		var reads atomic.Int64
		point := statementRUPointResponseStatsForTestFromResponse(&kvrpcpb.ScanDetailV2{TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 37}, 7)
		custom := &statementRUPointScalarCountingStats{kind: -5001, stats: point, reads: &reads}
		checkStatementRUSingleSnapshotOracle(t, []execdetails.RuntimeStats{custom}, statementRUOperatorComplete, 55.5, 7)
		require.Equal(t, int64(2), reads.Swap(0))
		bad := point
		bad.Invalidate()
		coll := execdetails.NewRuntimeStatsColl(nil)
		coll.RegisterStats(1001, &statementRUPointScalarCountingStats{kind: -5002, stats: bad, reads: &reads})
		coll.RegisterStats(1001, &statementRUPointScalarCountingStats{kind: -5003, stats: point, reads: &reads, panicOnRead: true})
		_, _, want := statementRUSingleSnapshotReference(1001, coll)
		require.Equal(t, int64(1), reads.Swap(0))
		_, _, got := collectStatementRUPointPayload(1001, coll)
		require.Equal(t, statementRUOperatorInvalid, got)
		require.Equal(t, want, got)
		require.Equal(t, int64(1), reads.Swap(0))
		coll = execdetails.NewRuntimeStatsColl(nil)
		coll.RegisterStats(1001, &statementRUPointScalarCountingStats{kind: -5004, stats: point, reads: &reads, panicOnRead: true})
		require.PanicsWithValue(t, "point scalar evidence read", func() { statementRUSingleSnapshotReference(1001, coll) })
		require.Equal(t, int64(1), reads.Swap(0))
		require.PanicsWithValue(t, "point scalar evidence read", func() { collectStatementRUPointPayload(1001, coll) })
		require.Equal(t, int64(1), reads.Load())
	})
}

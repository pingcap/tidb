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
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

const (
	statementRUSimpleSelectSQLForTest      = "select * from t"
	statementRUResultFailpointForTest      = "github.com/pingcap/tidb/pkg/executor/observeStatementRUResultForTest"
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
		state uint8,
		scanBytes, netBytes, frontendCompileBytes float64,
	) {
		observe(statementRUCalibrationSnapshot{
			State: statementRUCalibrationState(state),
			Units: statementRURawUnits{
				ScanBytes:            scanBytes,
				NetBytes:             netBytes,
				FrontendCompileBytes: frontendCompileBytes,
			},
		})
	})
}

func TestStatementRUResultFinalizationAndPublication(t *testing.T) {
	fixture := newStatementRUSimpleSelectFixture(t)
	var resultCount, calibrationCount atomic.Int64
	var result statementRUResultOnly
	var snapshot statementRUCalibrationSnapshot
	testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(_ uint64, totalRU float64) {
		resultCount.Add(1)
		result = statementRUResultOnly{TotalRU: totalRU}
		// ResultOnly is published first. Change both live evidence sources before
		// calibration is published to prove that neither consumer recalculates or
		// rereads statement state after the single value-only finalization.
		fixture.recordReaderScanDetail(fixture.stmt.Plan.(*physicalop.PhysicalTableReader), 0, 0, 990)
		fixture.stmt.Ctx.GetSessionVars().RUV2Metrics.AddTiKVCoprocessorResponseBytes(1000)
		fixture.stmt.finishStatementRUForTest(nil)
	})
	observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
		calibrationCount.Add(1)
		snapshot = published
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

	require.Equal(t, int64(1), resultCount.Load())
	require.Equal(t, int64(1), calibrationCount.Load())
	require.Equal(t, statementRUCalibrationComplete, snapshot.State)
	require.Equal(t, statementRURawUnits{
		ScanBytes:            10,
		NetBytes:             20,
		FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
	}, snapshot.Units)
	require.Equal(t, calculateStatementRUResultOnly(snapshot.Units), result)
	require.Zero(t, fixture.owner.calculationSetup)

	fixture.stmt.finishStatementRUForTest(nil)
	require.Equal(t, int64(1), resultCount.Load())
	require.Equal(t, int64(1), calibrationCount.Load())
	require.Equal(t, float64(10), snapshot.Units.ScanBytes)
	require.Equal(t, float64(20), snapshot.Units.NetBytes)
}

func TestStatementRUResultProjectionCompleteness(t *testing.T) {
	t.Run("frontend missing is zero only for ResultOnly", func(t *testing.T) {
		finalized := (statementRUCalculator{units: statementRURawUnits{
			ScanBytes: 10,
			NetBytes:  20,
		}}).finalize(true)
		require.True(t, finalized.hasResult)
		require.Equal(t, statementRUResultOnly{TotalRU: 30}, finalized.result)
		require.Equal(t, statementRUCalibrationIncomplete, finalized.calibrationState)
		require.Zero(t, finalized.units.FrontendCompileBytes)
	})

	t.Run("scan missing suppresses ResultOnly", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.stmt.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
		var resultCount atomic.Int64
		var snapshot statementRUCalibrationSnapshot
		testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(uint64, float64) {
			resultCount.Add(1)
		})
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			snapshot = published
		})
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Zero(t, resultCount.Load())
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Zero(t, snapshot.Units.ScanBytes)
		require.Equal(t, float64(20), snapshot.Units.NetBytes)
	})

	t.Run("net missing suppresses ResultOnly", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.stmt.Ctx.GetSessionVars().RUV2Metrics = execdetails.NewRUV2Metrics()
		var resultCount atomic.Int64
		var snapshot statementRUCalibrationSnapshot
		testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(uint64, float64) {
			resultCount.Add(1)
		})
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			snapshot = published
		})
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		require.Zero(t, resultCount.Load())
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Equal(t, float64(10), snapshot.Units.ScanBytes)
		require.Zero(t, snapshot.Units.NetBytes)
	})

	t.Run("early close suppresses ResultOnly", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.owner.rootEOF.Store(false)
		var resultCount, calibrationCount atomic.Int64
		var snapshot statementRUCalibrationSnapshot
		testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(uint64, float64) {
			resultCount.Add(1)
		})
		observeStatementRUCalibrationForTest(t, func(published statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
			snapshot = published
		})

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)

		require.Zero(t, resultCount.Load())
		require.Equal(t, int64(1), calibrationCount.Load())
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Equal(t, statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		}, snapshot.Units)
	})

	t.Run("invalid evidence suppresses both publications", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		fixture.recordReaderScanDetail(fixture.stmt.Plan.(*physicalop.PhysicalTableReader), 0, 0, -11)
		var resultCount, calibrationCount atomic.Int64
		testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(uint64, float64) {
			resultCount.Add(1)
		})
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(nil)
		fixture.stmt.finishStatementRUForTest(nil)

		require.Zero(t, resultCount.Load())
		require.Zero(t, calibrationCount.Load())
		require.Zero(t, fixture.owner.calculationSetup)
	})

	t.Run("terminal error publishes no uninitialized snapshot", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		var calibrationCount atomic.Int64
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUForTest(context.Canceled)
		fixture.stmt.finishStatementRUForTest(nil)
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
		fixture.stmt.RecordStatementRUFinalOutcome(true)
		require.NotPanics(t, func() { fixture.stmt.finishStatementRUForTest(nil) })
		fixture.stmt.finishStatementRUForTest(nil)
		require.Zero(t, calibrationCount.Load())
		require.Zero(t, fixture.owner.calculationSetup)
	})
}

func TestStatementRUPublisherIsolation(t *testing.T) {
	t.Run("result panic does not block calibration", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t)
		finalized := (statementRUCalculator{units: statementRURawUnits{
			ScanBytes:            10,
			NetBytes:             20,
			FrontendCompileBytes: float64(len(statementRUSimpleSelectSQLForTest)),
		}}).finalize(true)
		var calibrationCount atomic.Int64
		testfailpoint.EnableCall(t, statementRUResultFailpointForTest, func(uint64, float64) {
			panic("result observer")
		})
		observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
		})
		require.NotPanics(t, func() { publishStatementRUFinalizedSnapshot(fixture.stmt, finalized) })
		require.Equal(t, int64(1), calibrationCount.Load())
	})

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
				ScanBytes:            10,
				NetBytes:             20,
				FrontendCompileBytes: 15,
			},
		}
		finalized := calculator.finalize(true)
		require.True(t, finalized.hasResult)
		require.Equal(t, statementRUResultOnly{TotalRU: 45}, finalized.result)
	})

	t.Run("placeholder formula stays pinned", func(t *testing.T) {
		units := statementRURawUnits{ScanBytes: 10, NetBytes: 20, FrontendCompileBytes: 15}
		require.Equal(t, statementRUResultOnly{TotalRU: 45}, calculateStatementRUResultOnly(units))
	})

	t.Run("finalized and published payloads contain no live references", func(t *testing.T) {
		for _, value := range []any{
			statementRUCalculator{},
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
			"invalidEvidence",
		}, statementRUFieldNames(calculatorType))
		unitsType := reflect.TypeOf(statementRURawUnits{})
		require.Equal(t, []string{"ScanBytes", "NetBytes", "FrontendCompileBytes"}, statementRUFieldNames(unitsType))
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

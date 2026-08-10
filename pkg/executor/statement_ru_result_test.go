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
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const statementRUSimpleSelectSQLForTest = "select * from t"

type statementRUSimpleSelectFixture struct {
	stmt       *ExecStmt
	run        *statementRUSimpleSelectRun
	owner      *statementRUPlanWalkOwner
	scanDetail *util.ScanDetail
}

func newStatementRUSimpleSelectFixture(t testing.TB, frontendPresent, scanPresent bool) statementRUSimpleSelectFixture {
	t.Helper()
	ctx := mock.NewContext()
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
	if frontendPresent {
		selectStmt.SetText(nil, statementRUSimpleSelectSQLForTest)
	}
	stmt := &ExecStmt{
		Ctx:      ctx,
		GoCtx:    context.Background(),
		Plan:     reader,
		StmtNode: selectStmt,
	}
	ctx.GetSessionVars().StmtCtx.SetPlan(reader)
	installStatementRUSimpleSelectOwner(stmt)
	require.NotNil(t, stmt.statementRUPlanWalkOwner)
	owner := stmt.statementRUPlanWalkOwner
	require.True(t, owner.hasSimpleSelectRun)
	run := &owner.simpleSelectRun
	ctx.GetSessionVars().StmtCtx.SetFlatPlan(plannercore.FlattenPhysicalPlan(reader, false))

	scanDetail := &util.ScanDetail{}
	if scanPresent {
		scanDetail.TotalKeys = 1
		scanDetail.ProcessedKeys = 1
		scanDetail.ProcessedKeysSize = 10
	}
	ctx.GetSessionVars().StmtCtx.MergeCopExecDetails(
		&execdetails.CopExecDetails{ScanDetail: scanDetail},
		time.Millisecond,
	)
	stats := execdetails.NewRuntimeStatsColl(nil)
	one := uint64(1)
	stats.RecordOneCopTask(scan.ID(), kv.TiKV, &tipb.ExecutorExecutionSummary{
		TimeProcessedNs: &one,
		NumProducedRows: &one,
		NumIterations:   &one,
	})
	ctx.GetSessionVars().StmtCtx.RuntimeStatsColl = stats
	metrics := execdetails.NewRUV2Metrics()
	metrics.AddResourceManagerReadCnt(1)
	metrics.AddTiKVCoprocessorResponseBytes(20)
	ctx.GetSessionVars().RUV2Metrics = metrics
	stmt.recordStatementRURootEOF()
	stmt.sealStatementRUResultTermination()
	return statementRUSimpleSelectFixture{stmt: stmt, run: run, owner: owner, scanDetail: scanDetail}
}

func TestStatementRUResultFreezeAndPublication(t *testing.T) {
	fixture := newStatementRUSimpleSelectFixture(t, true, true)
	var resultCount, calibrationCount atomic.Int64
	var result statementRUResultOnly
	var snapshot statementRUCalibrationSnapshot
	weights := statementRUPlaceholderWeightSnapshot()
	fixture.run.resultPublisher = func(_ *zap.Logger, _ uint64, published statementRUResultOnly) {
		resultCount.Add(1)
		result = published
		fixture.stmt.finishStatementRUPlanWalk(nil)
	}
	fixture.run.calibrationPublisher = func(published statementRUCalibrationSnapshot) {
		calibrationCount.Add(1)
		snapshot = published
	}

	fixture.stmt.RecordStatementRUFinalOutcome(true)
	const callers = 32
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			fixture.stmt.finishStatementRUPlanWalk(nil)
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), resultCount.Load())
	require.Equal(t, int64(1), calibrationCount.Load())
	require.Equal(t, statementRUCalibrationComplete, snapshot.State)
	require.Equal(t, float64(10), snapshot.Units.ScanBytes)
	require.Equal(t, float64(20), snapshot.Units.NetBytes)
	require.Equal(t, float64(len(statementRUSimpleSelectSQLForTest)), snapshot.Units.FrontendCompileBytes)
	reconstructed, ok := weights.total(snapshot.Units)
	require.True(t, ok)
	require.Equal(t, reconstructed, result.TotalRU)
	require.False(t, fixture.owner.hasSimpleSelectRun)
	require.Nil(t, fixture.owner.simpleSelectRun.resultPublisher)
	require.Nil(t, fixture.owner.simpleSelectRun.calibrationPublisher)
	require.Nil(t, fixture.owner.visit)

	atomic.StoreInt64(&fixture.scanDetail.ProcessedKeysSize, 1000)
	fixture.stmt.Ctx.GetSessionVars().RUV2Metrics.AddTiKVCoprocessorResponseBytes(1000)
	fixture.stmt.finishStatementRUPlanWalk(nil)
	require.Equal(t, int64(1), resultCount.Load())
	require.Equal(t, int64(1), calibrationCount.Load())
	require.Equal(t, float64(10), snapshot.Units.ScanBytes)
	require.Equal(t, float64(20), snapshot.Units.NetBytes)
}

func TestStatementRUResultProjectionCompleteness(t *testing.T) {
	t.Run("frontend missing is zero only for ResultOnly", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, false, true)
		var results []statementRUResultOnly
		var snapshots []statementRUCalibrationSnapshot
		var reasons []statementRUFailureReason
		fixture.run.resultPublisher = func(_ *zap.Logger, _ uint64, result statementRUResultOnly) {
			results = append(results, result)
		}
		fixture.run.calibrationPublisher = func(snapshot statementRUCalibrationSnapshot) { snapshots = append(snapshots, snapshot) }
		fixture.run.failureRecorder = func(reason statementRUFailureReason) { reasons = append(reasons, reason) }

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUPlanWalk(nil)
		require.Equal(t, []statementRUResultOnly{{TotalRU: 30}}, results)
		require.Len(t, snapshots, 1)
		require.Equal(t, statementRUCalibrationIncomplete, snapshots[0].State)
		require.Zero(t, snapshots[0].Units.FrontendCompileBytes)
		require.Equal(t, []statementRUFailureReason{statementRUFailureIncompleteEvidence}, reasons)
	})

	t.Run("scan missing suppresses ResultOnly but publishes incomplete calibration", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, false)
		var resultCount, calibrationCount atomic.Int64
		var snapshot statementRUCalibrationSnapshot
		var reasons []statementRUFailureReason
		fixture.run.resultPublisher = func(*zap.Logger, uint64, statementRUResultOnly) { resultCount.Add(1) }
		fixture.run.calibrationPublisher = func(published statementRUCalibrationSnapshot) {
			calibrationCount.Add(1)
			snapshot = published
		}
		fixture.run.failureRecorder = func(reason statementRUFailureReason) { reasons = append(reasons, reason) }

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUPlanWalk(nil)
		fixture.stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, resultCount.Load())
		require.Equal(t, int64(1), calibrationCount.Load())
		require.Equal(t, statementRUCalibrationIncomplete, snapshot.State)
		require.Zero(t, snapshot.Units.ScanBytes)
		require.Equal(t, float64(20), snapshot.Units.NetBytes)
		require.Equal(t, []statementRUFailureReason{statementRUFailureIncompleteEvidence}, reasons)
	})

	t.Run("net missing suppresses ResultOnly but publishes incomplete calibration", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddResourceManagerReadCnt(1)
		fixture.stmt.Ctx.GetSessionVars().RUV2Metrics = metrics
		var resultCount atomic.Int64
		var snapshots []statementRUCalibrationSnapshot
		var reasons []statementRUFailureReason
		fixture.run.resultPublisher = func(*zap.Logger, uint64, statementRUResultOnly) { resultCount.Add(1) }
		fixture.run.calibrationPublisher = func(snapshot statementRUCalibrationSnapshot) {
			snapshots = append(snapshots, snapshot)
		}
		fixture.run.failureRecorder = func(reason statementRUFailureReason) { reasons = append(reasons, reason) }

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, resultCount.Load())
		require.Len(t, snapshots, 1)
		require.Equal(t, statementRUCalibrationIncomplete, snapshots[0].State)
		require.Equal(t, float64(10), snapshots[0].Units.ScanBytes)
		require.Zero(t, snapshots[0].Units.NetBytes)
		require.Equal(t, []statementRUFailureReason{statementRUFailureIncompleteEvidence}, reasons)
	})

	t.Run("early close suppresses ResultOnly but publishes incomplete calibration", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		fixture.owner.resultTermination.Store(uint32(statementRUResultTerminationEarlyClose))
		var resultCount atomic.Int64
		var snapshots []statementRUCalibrationSnapshot
		var reasons []statementRUFailureReason
		fixture.run.resultPublisher = func(*zap.Logger, uint64, statementRUResultOnly) { resultCount.Add(1) }
		fixture.run.calibrationPublisher = func(snapshot statementRUCalibrationSnapshot) {
			snapshots = append(snapshots, snapshot)
		}
		fixture.run.failureRecorder = func(reason statementRUFailureReason) { reasons = append(reasons, reason) }

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, resultCount.Load())
		require.Len(t, snapshots, 1)
		require.Equal(t, statementRUCalibrationIncomplete, snapshots[0].State)
		require.Equal(t, float64(10), snapshots[0].Units.ScanBytes)
		require.Equal(t, float64(20), snapshots[0].Units.NetBytes)
		require.Equal(t, []statementRUFailureReason{statementRUFailureIncompleteEvidence}, reasons)
	})

	t.Run("unsupported plan suppresses ResultOnly but publishes unsupported calibration", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		flat.Main = append(flat.Main, flat.Main[1])
		var resultCount atomic.Int64
		var snapshots []statementRUCalibrationSnapshot
		var reasons []statementRUFailureReason
		fixture.run.resultPublisher = func(*zap.Logger, uint64, statementRUResultOnly) { resultCount.Add(1) }
		fixture.run.calibrationPublisher = func(snapshot statementRUCalibrationSnapshot) {
			snapshots = append(snapshots, snapshot)
		}
		fixture.run.failureRecorder = func(reason statementRUFailureReason) { reasons = append(reasons, reason) }

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, resultCount.Load())
		require.Len(t, snapshots, 1)
		require.Equal(t, statementRUCalibrationUnsupported, snapshots[0].State)
		require.Equal(t, []statementRUFailureReason{statementRUFailureUnsupportedPlan}, reasons)
	})

	t.Run("foreign scan occurrence with the same plan ID is unsupported", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		reader := fixture.stmt.Plan.(*physicalop.PhysicalTableReader)
		ownedScan := reader.TablePlan.(*physicalop.PhysicalTableScan)
		foreignScan := *ownedScan
		replaced := false
		for _, operator := range flat.Main {
			if operator != nil && operator.Origin == ownedScan {
				operator.Origin = &foreignScan
				replaced = true
				break
			}
		}
		require.True(t, replaced)

		var resultCount atomic.Int64
		var snapshots []statementRUCalibrationSnapshot
		var reasons []statementRUFailureReason
		fixture.run.resultPublisher = func(*zap.Logger, uint64, statementRUResultOnly) { resultCount.Add(1) }
		fixture.run.calibrationPublisher = func(snapshot statementRUCalibrationSnapshot) {
			snapshots = append(snapshots, snapshot)
		}
		fixture.run.failureRecorder = func(reason statementRUFailureReason) { reasons = append(reasons, reason) }

		fixture.stmt.RecordStatementRUFinalOutcome(true)
		fixture.stmt.finishStatementRUPlanWalk(nil)
		require.Zero(t, resultCount.Load())
		require.Len(t, snapshots, 1)
		require.Equal(t, statementRUCalibrationUnsupported, snapshots[0].State)
		require.Equal(t, []statementRUFailureReason{statementRUFailureUnsupportedPlan}, reasons)
	})

	t.Run("ResultOnly-only does not construct a calibration snapshot", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		fixture.run.calibrationPublisher = nil
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		execDetail := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetExecDetails()
		publication := fixture.run.freeze(
			fixture.stmt,
			fixture.owner,
			flat,
			&statementRUTerminalExecDetailsView{execDetails: execDetail},
		)
		require.True(t, publication.hasResult)
		require.False(t, publication.hasCalibration)
		require.Zero(t, publication.calibration)
	})
}

func TestStatementRUPublisherIsolation(t *testing.T) {
	t.Run("result panic does not suppress calibration", func(t *testing.T) {
		var calibrationCount atomic.Int64
		var reasons []statementRUFailureReason
		publication := statementRUSimpleSelectPublication{
			resultPublisher: func(*zap.Logger, uint64, statementRUResultOnly) { panic("result") },
			result:          statementRUResultOnly{TotalRU: 1},
			hasResult:       true,
			calibrationPublisher: func(statementRUCalibrationSnapshot) {
				calibrationCount.Add(1)
			},
			calibration:     statementRUCalibrationSnapshot{State: statementRUCalibrationComplete},
			hasCalibration:  true,
			failureRecorder: func(reason statementRUFailureReason) { reasons = append(reasons, reason) },
		}
		require.NotPanics(t, publication.publish)
		require.Equal(t, int64(1), calibrationCount.Load())
		require.Equal(t, []statementRUFailureReason{statementRUFailureResultPublisherPanic}, reasons)
	})

	t.Run("calibration and failure recorder panics do not escape", func(t *testing.T) {
		var resultCount atomic.Int64
		publication := statementRUSimpleSelectPublication{
			resultPublisher: func(*zap.Logger, uint64, statementRUResultOnly) { resultCount.Add(1) },
			result:          statementRUResultOnly{TotalRU: 1},
			hasResult:       true,
			calibrationPublisher: func(statementRUCalibrationSnapshot) {
				panic("calibration")
			},
			calibration:    statementRUCalibrationSnapshot{State: statementRUCalibrationComplete},
			hasCalibration: true,
			failureRecorder: func(statementRUFailureReason) {
				panic("failure recorder")
			},
		}
		require.NotPanics(t, publication.publish)
		require.Equal(t, int64(1), resultCount.Load())
	})
}

func TestStatementRUResultValueContracts(t *testing.T) {
	t.Run("occurrence roles do not depend on visit order or flat indexes", func(t *testing.T) {
		fixture := newStatementRUSimpleSelectFixture(t, true, true)
		flat := fixture.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
		binding, owned := bindStatementRUSimpleSelectPlan(fixture.stmt.Plan)
		require.True(t, owned)

		var readerOperator, scanOperator *plannercore.FlatOperator
		for _, operator := range flat.Main {
			if operator == nil {
				continue
			}
			switch operator.Origin {
			case binding.reader:
				readerOperator = operator
			case binding.scan:
				scanOperator = operator
			}
		}
		require.NotNil(t, readerOperator)
		require.NotNil(t, scanOperator)

		readerOccurrence := *readerOperator
		readerOccurrence.ChildrenIdx = []int{100}
		readerOccurrence.ChildrenEndIdx = -1
		scanOccurrence := *scanOperator
		scanOccurrence.ChildrenIdx = []int{200}
		scanOccurrence.ChildrenEndIdx = 300

		accumulator := statementRUSimpleSelectAccumulator{}
		accumulator.start(flat)
		accumulator.observe(binding, statementRUPlanTreeMain, &scanOccurrence)
		accumulator.observe(binding, statementRUPlanTreeMain, &readerOccurrence)
		accumulator.finish()
		require.False(t, accumulator.unsupported)
	})

	t.Run("placeholder weights stay pinned", func(t *testing.T) {
		require.Equal(t, statementRUWeights{
			ScanBytes:            1,
			NetBytes:             1,
			FrontendCompileBytes: 1,
		}, statementRUPlaceholderWeightSnapshot())
	})

	t.Run("payloads contain no live references", func(t *testing.T) {
		for _, value := range []any{
			statementRURawUnits{},
			statementRUSimpleSelectAccumulator{},
			statementRUResultOnly{},
			statementRUCalibrationSnapshot{},
		} {
			requireStatementRUValueOnlyType(t, reflect.TypeOf(value))
		}
	})

	t.Run("publication contracts contain only approved scalar fields", func(t *testing.T) {
		unitsType := reflect.TypeOf(statementRURawUnits{})
		require.Equal(t, 3, unitsType.NumField())
		require.Equal(t, "ScanBytes", unitsType.Field(0).Name)
		require.Equal(t, "NetBytes", unitsType.Field(1).Name)
		require.Equal(t, "FrontendCompileBytes", unitsType.Field(2).Name)

		resultType := reflect.TypeOf(statementRUResultOnly{})
		require.Equal(t, 1, resultType.NumField())
		require.Equal(t, "TotalRU", resultType.Field(0).Name)

		snapshotType := reflect.TypeOf(statementRUCalibrationSnapshot{})
		require.Equal(t, 2, snapshotType.NumField())
		require.Equal(t, "State", snapshotType.Field(0).Name)
		require.Equal(t, "Units", snapshotType.Field(1).Name)
		require.Equal(t, unitsType, snapshotType.Field(1).Type)
	})

	t.Run("non-finite formula fails closed", func(t *testing.T) {
		weights := statementRUPlaceholderWeightSnapshot()
		_, ok := weights.total(statementRURawUnits{ScanBytes: math.Inf(1)})
		require.False(t, ok)
		_, ok = (statementRUWeights{ScanBytes: math.NaN()}).total(statementRURawUnits{ScanBytes: 1})
		require.False(t, ok)
	})
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

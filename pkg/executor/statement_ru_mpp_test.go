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
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/resourcegroup/ruv2"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestStatementRUMPPUnits(t *testing.T) {
	ctx := mock.NewContext()
	statsInfo := &property.StatsInfo{}
	scan := physicalop.PhysicalTableScan{Table: &model.TableInfo{}}.Init(ctx, 0)
	scan.SetSchema(expression.NewSchema())
	sort := physicalop.PhysicalSort{}.Init(ctx, statsInfo, 0)
	sort.SetChildren(scan)
	sender := physicalop.PhysicalExchangeSender{}.Init(ctx, statsInfo)
	sender.SetChildren(sort)
	reader := physicalop.PhysicalTableReader{StoreType: kv.TiFlash}.Init(ctx, 0)
	reader.TablePlan, reader.ReadReqType = sender, physicalop.MPP
	flat := plannercore.FlattenPhysicalPlan(reader, false)
	stats := execdetails.NewRuntimeStatsColl(nil)
	plans := []int{scan.ID(), sort.ID(), sender.ID()}
	record := func(plan base.Plan, rows, read, send, cross uint64) {
		id := fmt.Sprintf("%s_%d", plan.TP(), plan.ID())
		stats.RecordTiFlashExecutionSummaries(plans, []*tipb.ExecutorExecutionSummary{{
			ExecutorId: &id, NumProducedRows: &rows,
			DetailInfo:            &tipb.ExecutorExecutionSummary_TiflashScanContext{TiflashScanContext: &tipb.TiFlashScanContext{UserReadBytes: &read}},
			TiflashNetworkSummary: &tipb.TiFlashNetWorkSummary{InnerZoneSendBytes: &send, InterZoneSendBytes: &cross, InnerZoneReceiveBytes: &send, InterZoneReceiveBytes: &cross},
		}})
	}
	// Two task contributions: sort uses the merged 200 rows, scan only once at Reader.
	record(scan, 100, 100, 10, 5)
	record(scan, 100, 200, 10, 5)
	record(sort, 200, 0, 0, 0)
	record(sender, 200, 0, 100, 50)
	finalized, operators, ok := calculateStatementRUWithOperators(flat, stats, nil, statementRUWriteSnapshot{}, statementRUCalculationSetup{fullReport: true}, true)
	require.True(t, ok)
	require.Equal(t, ruv2.StmtUnits{CPUWork: 200 * math.Log2(200), ScanBytes: 300, NetBytes: 180, CrossAZNetBytes: 60, OperatorNum: 4}, finalized.units)
	require.Equal(t, float64(1), finalized.engineRU.TiDB)
	require.Zero(t, finalized.engineRU.TiKV)
	require.InDelta(t, finalized.result.TotalRU, finalized.engineRU.TiDB+finalized.engineRU.TiFlash, 1e-9)
	require.InDelta(t, finalized.result.TotalRU, operators.TotalRU, 1e-9)
	var reportUnits ruv2.StmtUnits
	for _, ops := range finalized.report.units {
		for _, units := range ops {
			reportUnits = reportUnits.Add(units)
		}
	}
	require.Equal(t, finalized.units, reportUnits)
	weights := ruv2.DefaultWeights()
	// Compare raw-unit results without the TiFlash RU multiplier.
	unscaledResult, valid := ruv2.Calculate(finalized.units, weights)
	require.True(t, valid)
	weights.CrossAZNetByte = 2
	result, valid := ruv2.Calculate(finalized.units, weights)
	require.True(t, valid)
	require.InDelta(t, unscaledResult.TotalRU+120, result.TotalRU, 1e-9)

	// Missing stats still produce a best-effort value.
	partial, ok := calculateStatementRU(flat, nil, nil, statementRUWriteSnapshot{}, statementRUCalculationSetup{}, true)
	require.True(t, ok)
	require.Equal(t, float64(31), partial.result.TotalRU) // 1 TiDB operator + 3 TiFlash operators * 10
	require.Equal(t, statementRUCalibrationIncomplete, partial.calibrationState)
	_, ok = calculateStatementRU(flat, stats, nil, statementRUWriteSnapshot{}, statementRUCalculationSetup{}, false)
	require.False(t, ok)
	// Columnar scan bytes follow the same Reader ownership and TiFlash attribution.
	for _, readBytes := range []uint64{0, 300} {
		stats = execdetails.NewRuntimeStatsColl(nil)
		record(scan, 200, readBytes, 0, 0)
		expected, ok := calculateStatementRU(flat, stats, nil, statementRUWriteSnapshot{}, statementRUCalculationSetup{fullReport: true}, true)
		require.True(t, ok)
		columnarStats := execdetails.NewRuntimeStatsColl(nil)
		id, rows := fmt.Sprintf("TableScan_%d", scan.ID()), uint64(200)
		columnarStats.RecordTiFlashExecutionSummaries(plans, []*tipb.ExecutorExecutionSummary{{
			ExecutorId: &id, NumProducedRows: &rows,
			DetailInfo: &tipb.ExecutorExecutionSummary_ColumnarScanContext{ColumnarScanContext: &tipb.ColumnarScanContext{UserReadBytes: &readBytes}},
		}})
		actual, ok := calculateStatementRU(flat, columnarStats, nil, statementRUWriteSnapshot{}, statementRUCalculationSetup{fullReport: true}, true)
		require.True(t, ok)
		require.Equal(t, expected.units, actual.units)
		require.Equal(t, expected.engineRU, actual.engineRU)
		require.Equal(t, expected.report.units, actual.report.units)
		require.Equal(t, float64(readBytes), actual.units.ScanBytes)
		require.Equal(t, statementRUCalibrationIncomplete, actual.calibrationState)
	}
	reader.ReadReqType = physicalop.BatchCop
	_, ok = calculateStatementRU(plannercore.FlattenPhysicalPlan(reader, false), nil, nil, statementRUWriteSnapshot{}, statementRUCalculationSetup{}, true)
	require.False(t, ok)
}

func TestStatementRUMPPHashState(t *testing.T) {
	ctx := mock.NewContext()
	stats := execdetails.NewRuntimeStatsColl(nil)
	baseAgg := physicalop.BasePhysicalAgg{
		GroupByItems: []expression.Expression{&expression.Column{}}, AggFuncs: []*aggregation.AggFuncDesc{{}},
	}
	agg := baseAgg.InitForHash(ctx, &property.StatsInfo{}, 0, nil)
	op := &plannercore.FlatOperator{Origin: agg, StoreType: kv.TiFlash, ReqType: physicalop.MPP}
	id, one, eight := fmt.Sprintf("HashAgg_%d", agg.ID()), uint64(1), uint64(8)
	for _, hash := range []*tipb.TiFlashHashTableStats{
		{Size_: &one}, {Size_: &eight, SizeKind: tipb.TiFlashHashTableSizeKind_TIFLASH_HASH_TABLE_SIZE_KIND_BUILD_ROW_COUNT.Enum()},
	} {
		stats.RecordTiFlashExecutionSummaries([]int{agg.ID()}, []*tipb.ExecutorExecutionSummary{{ExecutorId: &id, TiflashHashTableStats: hash}})
	}
	calculator := &statementRUCalculator{}
	require.Equal(t, statementRUOperatorComplete, collectStatementRUAggregationUnits(op, []statementRUOperatorResult{{outputRows: 20}}, 3, stats, calculator))
	require.Equal(t, ruv2.StmtUnits{CPUWork: 40, HashStateRows: 9}, calculator.units)
}

func TestStatementRUMPPPublication(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(c *config.Config) {
		c.RUV2.ReportMode = config.RUReportModeFull
		c.RUV2.StmtWeights.NetByte = 2
	})
	fixture := newStatementRUSimpleSelectFixture(t)
	ctx := fixture.stmt.Ctx.(*mock.Context)
	sc := ctx.GetSessionVars().StmtCtx
	left := physicalop.PhysicalTableScan{Table: &model.TableInfo{}}.Init(ctx, 0)
	right := physicalop.PhysicalTableScan{Table: &model.TableInfo{}}.Init(ctx, 0)
	left.SetSchema(expression.NewSchema())
	right.SetSchema(expression.NewSchema())
	broadcast := physicalop.PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_Broadcast}.Init(ctx, &property.StatsInfo{})
	broadcast.SetChildren(right)
	receiver := physicalop.PhysicalExchangeReceiver{}.Init(ctx, &property.StatsInfo{})
	receiver.SetChildren(broadcast)
	join := physicalop.PhysicalHashJoin{BasePhysicalJoin: physicalop.BasePhysicalJoin{JoinType: base.InnerJoin}, EqualConditions: []*expression.ScalarFunction{{}}}.Init(ctx, &property.StatsInfo{}, 0)
	join.SetChildren(left, receiver)
	sender := physicalop.PhysicalExchangeSender{}.Init(ctx, &property.StatsInfo{})
	sender.SetChildren(join)
	reader := physicalop.PhysicalTableReader{StoreType: kv.TiFlash}.Init(ctx, 0)
	reader.TablePlan, reader.ReadReqType = sender, physicalop.MPP
	fixture.stmt.Plan = reader
	sc.SetPlan(reader)
	flat := plannercore.FlattenPhysicalPlan(reader, false)
	sc.SetFlatPlan(flat)
	ids := []int{left.ID(), right.ID(), broadcast.ID(), receiver.ID(), join.ID(), sender.ID()}
	for i, plan := range []base.Plan{left, right, broadcast, receiver, join, sender} {
		// Broadcast receives twice the source rows; the join must use 5+14, not 5+7.
		rows := []uint64{5, 7, 7, 14, 9, 9}[i]
		read := []uint64{100, 200, 0, 0, 0, 0}[i]
		cross := []uint64{0, 0, 10, 10, 0, 20}[i]
		same, hash := uint64(0), uint64(8)
		id := fmt.Sprintf("%s_%d", plan.TP(), plan.ID())
		summary := &tipb.ExecutorExecutionSummary{ExecutorId: &id, NumProducedRows: &rows,
			DetailInfo:            &tipb.ExecutorExecutionSummary_TiflashScanContext{TiflashScanContext: &tipb.TiFlashScanContext{UserReadBytes: &read}},
			TiflashNetworkSummary: &tipb.TiFlashNetWorkSummary{InnerZoneSendBytes: &same, InterZoneSendBytes: &cross}}
		if plan == join {
			summary.TiflashHashTableStats = &tipb.TiFlashHashTableStats{Size_: &hash, SizeKind: tipb.TiFlashHashTableSizeKind_TIFLASH_HASH_TABLE_SIZE_KIND_BUILD_ROW_COUNT.Enum()}
		}
		sc.RuntimeStatsColl.RecordTiFlashExecutionSummaries(ids, []*tipb.ExecutorExecutionSummary{summary})
	}
	finalized, operators, ok := calculateStatementRUWithOperators(flat, sc.RuntimeStatsColl, ctx.GetSessionVars().RUV2Metrics, statementRUWriteSnapshot{}, fixture.owner.calculationSetup, true)
	require.True(t, ok)
	require.Equal(t, float64(19), finalized.units.CPUWork)
	require.Equal(t, float64(300), finalized.units.ScanBytes)
	require.Equal(t, float64(50), finalized.units.NetBytes)
	require.Equal(t, float64(30), finalized.units.CrossAZNetBytes)
	require.Equal(t, float64(9), finalized.units.JoinOutputRows)
	require.Equal(t, float64(8), finalized.units.HashStateRows)
	require.Equal(t, float64(40), finalized.engineRU.TiKV)
	require.Equal(t, float64(4020), finalized.engineRU.TiFlash) // (19+300+60+9+8+6)*10
	requireStatementRUReportConservation(t, finalized)
	require.InDelta(t, finalized.result.TotalRU, operators.TotalRU, 1e-9)
	require.InDelta(t, finalized.result.TotalRU, operators.Main[0].CumRU, 1e-9)
	var selfRU float64
	for _, operator := range operators.Main {
		selfRU += operator.SelfRU
	}
	require.InDelta(t, finalized.result.TotalRU, selfRU, 1e-9)
	reporter := &statementRUReporterForTest{}
	fixture.stmt.Ctx = &statementRUReportingContextForTest{Context: ctx, reporter: reporter}
	var before [statementRUEngineCount]float64
	for i, name := range statementRUEngineNames {
		before[i] = testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(name))
	}
	observed := 0
	observeStatementRUCalibrationForTest(t, func(snapshot statementRUCalibrationSnapshot) {
		observed++
		require.Equal(t, finalized.units, snapshot.Units)
	})
	fixture.stmt.RecordStatementRUFinalOutcome(true)
	fixture.stmt.finishStatementRU(nil)
	fixture.stmt.finishStatementRU(nil)
	require.Equal(t, 1, observed)
	require.Equal(t, 1, reporter.calls)
	require.Equal(t, [3]float64{finalized.engineRU.TiKV, finalized.engineRU.TiDB, finalized.engineRU.TiFlash}, reporter.ru)
	for i, want := range []float64{finalized.engineRU.TiDB, finalized.engineRU.TiKV, finalized.engineRU.TiFlash} {
		require.InDelta(t, want, testutil.ToFloat64(metrics.RUV2ByEngine.WithLabelValues(statementRUEngineNames[i]))-before[i], 1e-9)
	}
}

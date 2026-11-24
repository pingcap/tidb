// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestPlanRiskCrossEngineProbe(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	outer := PhysicalTableReader{StoreType: kv.TiFlash}.Init(ctx, 0)
	outer.SetStats(&property.StatsInfo{RowCount: 100})
	inner := PhysicalIndexReader{}.Init(ctx, 0)
	inner.SetStats(&property.StatsInfo{RowCount: 10})
	scan := PhysicalIndexScan{}.Init(ctx, 0)
	scan.SetStats(inner.StatsInfo())
	inner.indexPlan = scan
	join := PhysicalIndexJoin{basePhysicalJoin: basePhysicalJoin{InnerChildIdx: 1}}.Init(ctx, &property.StatsInfo{RowCount: 1000}, 0)
	join.SetChildren(outer, inner)
	inner.SetProbeParents([]base.PhysicalPlan{join})
	scan.SetProbeParents([]base.PhysicalPlan{join})

	profile := AssessPhysicalPlanRisk(join)
	require.False(t, profile.Risk)
	require.Len(t, profile.Signals, 1)
	signal := profile.Signals[0]
	require.False(t, signal.Risk)
	require.Equal(t, "remote_probe_volume_below_threshold", signal.Reason)
	require.Equal(t, "cross_engine_index_probe", signal.Kind)
	require.Equal(t, "covering_index", signal.InnerAccess)
	require.Equal(t, float64(100), *signal.OuterRows.AllProbes)
	require.Equal(t, float64(10), *signal.InnerRows.PerProbe)
	require.Equal(t, float64(1000), *signal.InnerRows.AllProbes)
	// Merely having both engines in sibling inputs is not a probe risk.
	hashJoin := PhysicalHashJoin{}.Init(ctx, &property.StatsInfo{RowCount: 1000}, 0)
	hashJoin.SetChildren(outer, inner)
	require.False(t, AssessPhysicalPlanRisk(hashJoin).Risk)
	// Batched IndexJoin does not get Apply's per-row semi-join short circuit.
	join.JoinType = logicalop.SemiJoin
	signal = AssessPhysicalPlanRisk(join).Signals[0]
	require.True(t, signal.SemiJoin)
	require.False(t, signal.MayShortCircuit)
	outer.StatsInfo().RowCount = RiskCrossEngineMinOuterRows
	inner.StatsInfo().RowCount = RiskCrossEngineMinInnerRows / RiskCrossEngineMinOuterRows
	profile = AssessPhysicalPlanRisk(join)
	require.True(t, profile.Risk)
	require.True(t, profile.Signals[0].Risk)
	require.Equal(t, "remote_probe_volume", profile.Signals[0].Reason)
	inner.StatsInfo().RowCount = 10
	// Covering access and a small driver do not by themselves trigger risk.
	outer.StatsInfo().RowCount = 1
	require.False(t, AssessPhysicalPlanRisk(join).Risk)
	require.Equal(t, float64(10), *AssessPhysicalPlanRisk(join).Signals[0].InnerRows.AllProbes)

	// Handle inner-at-left joins and both batching variants.
	join.InnerChildIdx = 0
	join.SetChildren(inner, outer)
	require.Len(t, AssessPhysicalPlanRisk(join).Signals, 1)
	hash := PhysicalIndexHashJoin{PhysicalIndexJoin: *join}.Init(ctx)
	require.Len(t, AssessPhysicalPlanRisk(hash).Signals, 1)
	merge := PhysicalIndexMergeJoin{PhysicalIndexJoin: *join}.Init(ctx)
	require.Len(t, AssessPhysicalPlanRisk(merge).Signals, 1)
	outer.StoreType = kv.TiKV
	require.False(t, AssessPhysicalPlanRisk(join).Risk)
	outer.StoreType = kv.TiDB
	require.False(t, AssessPhysicalPlanRisk(join).Risk)
}

func TestPlanRiskExperimentalBoundaries(t *testing.T) {
	ptr := func(v float64) *float64 { return &v }
	cross := PlanRiskSignal{Kind: "cross_engine_index_probe", InnerAccess: "covering_index",
		OuterRows: PlanRiskRows{AllProbes: ptr(30000)}, InnerReadRows: PlanRiskRows{AllProbes: ptr(3000000)}}
	hit, _ := riskSignalDecision(cross)
	require.True(t, hit) // Fanout may be large even when outer is below the old cutoff.
	cross.OuterRows.AllProbes, cross.InnerReadRows.AllProbes = ptr(200000), ptr(200000)
	hit, _ = riskSignalDecision(cross)
	require.False(t, hit)
	cross.InnerAccess = "index_lookup"
	hit, _ = riskSignalDecision(cross)
	require.True(t, hit) // Double-read probes are not treated as covering accesses.
	cross.InnerReadRows.AllProbes = nil
	hit, _ = riskSignalDecision(cross)
	require.True(t, hit) // Large known call count can trigger despite unknown fanout.

	apply := PlanRiskSignal{Kind: "serial_correlated_apply_probe", ProbeShape: "nested_probe", OuterRows: PlanRiskRows{AllProbes: ptr(50000)},
		InnerRows: PlanRiskRows{PerProbe: ptr(1)}, NestedOuterRows: &PlanRiskRows{PerProbe: ptr(1)}}
	hit, _ = riskSignalDecision(apply)
	require.True(t, hit) // Nested probes are not waived by one output row.
	apply.ProbeShape, apply.EffectiveOuterCalls, apply.EffectiveReadRows = "single_reader", ptr(1500), ptr(1500)
	hit, reason := riskSignalDecision(apply)
	require.False(t, hit) // Never turn 1,500 bounded point reads into a full MPP scan on call count alone.
	require.Equal(t, "simple_reader_requires_read_volume", reason)
	apply.ProbeShape, apply.EffectiveOuterCalls, apply.EffectiveReadRows = "nested_probe", nil, nil
	apply.ApplyCanUseCache = true
	apply.CacheMissLowerBound = ptr(42500)
	hit, _ = riskSignalDecision(apply)
	require.True(t, hit) // 15% potential cache hits do not waive the remaining calls.
	apply.CacheMissLowerBound = ptr(10)
	hit, reason = riskSignalDecision(apply)
	require.False(t, hit)
	require.Equal(t, "cache_reuse_uncertain", reason)
	apply.CacheMissLowerBound = nil
	hit, reason = riskSignalDecision(apply)
	require.False(t, hit)
	require.Equal(t, "cache_misses_unknown", reason)
	apply.ApplyCanUseCache = false
	apply.EffectiveOuterCalls = ptr(RiskApplyMinOuterRows - 1)
	hit, _ = riskSignalDecision(apply)
	require.False(t, hit)
}

func TestRiskModeMPPAlternative(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	scan := PhysicalTableScan{}.Init(ctx, 0)
	scan.SetStats(&property.StatsInfo{RowCount: 50})
	sender := PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_Hash}.Init(ctx, &property.StatsInfo{RowCount: 100})
	sender.SetChildren(scan)
	reader := PhysicalTableReader{StoreType: kv.TiFlash}.Init(ctx, 0)
	reader.tablePlan, reader.ReadReqType = sender, MPP
	require.True(t, IsRiskModeMPPAlternative(reader))
	mppJoin := PhysicalHashJoin{}.Init(ctx, &property.StatsInfo{RowCount: 100}, 0)
	mppJoin.SetChildren(scan, scan)
	reader.tablePlan = mppJoin
	require.False(t, NeedsTiFlashAlternative(reader))
	reader.tablePlan = sender
	sender.ExchangeType = tipb.ExchangeType_Broadcast
	require.True(t, IsRiskModeMPPAlternative(reader))
	scan.StatsInfo().RowCount = float64(ctx.GetSessionVars().BroadcastJoinThresholdCount + 1)
	require.False(t, IsRiskModeMPPAlternative(reader))
	scan.StatsInfo().RowCount = 50
	sender.ExchangeType = tipb.ExchangeType_Hash
	reader.ReadReqType = Cop
	require.False(t, IsRiskModeMPPAlternative(reader))
	reader.ReadReqType = MPP
	reader.StoreType = kv.TiKV
	require.False(t, IsRiskModeMPPAlternative(reader))
	reader.StoreType = kv.TiFlash
	rootAgg := basePhysicalAgg{}.initForHash(ctx, &property.StatsInfo{RowCount: 100}, 0)
	rootAgg.SetChildren(reader)
	reader.SetStats(&property.StatsInfo{RowCount: 50})
	require.True(t, IsRiskModeMPPAlternative(rootAgg))
	reader.StatsInfo().RowCount = 1000000
	require.False(t, IsRiskModeMPPAlternative(rootAgg))
	require.False(t, IsRiskModeMPPAlternative(nil))
}

func TestPlanRiskSerialApply(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	cor := &expression.CorrelatedColumn{Column: expression.Column{UniqueID: 1}}
	outer := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	outer.SetStats(&property.StatsInfo{RowCount: 3})
	relation := PhysicalSelection{Conditions: []expression.Expression{cor}}.Init(ctx, &property.StatsInfo{RowCount: 15}, 0)
	probe := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	probe.SetStats(&property.StatsInfo{RowCount: 1})
	nested := PhysicalIndexJoin{basePhysicalJoin: basePhysicalJoin{InnerChildIdx: 1}}.Init(ctx, &property.StatsInfo{RowCount: 12}, 0)
	nested.SetChildren(relation, probe)
	apply := PhysicalApply{OuterSchema: []*expression.CorrelatedColumn{cor}}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	apply.InnerChildIdx = 1
	apply.JoinType = logicalop.SemiJoin
	apply.SetChildren(outer, nested)
	nested.SetProbeParents([]base.PhysicalPlan{apply})
	relation.SetProbeParents([]base.PhysicalPlan{apply})
	probe.SetProbeParents([]base.PhysicalPlan{apply, nested})

	profile := AssessPhysicalPlanRisk(apply)
	require.Len(t, profile.Signals, 1)
	signal := profile.Signals[0]
	require.Equal(t, "serial_correlated_apply_probe", signal.Kind)
	require.True(t, signal.MayShortCircuit)
	require.Equal(t, 1, signal.ApplyConcurrency)
	require.Equal(t, float64(15), *signal.NestedOuterRows.PerProbe)
	// 45 already includes the Apply invocations; do not multiply by 3 again.
	require.Equal(t, float64(45), *signal.NestedOuterRows.AllProbes)
	require.Equal(t, float64(45), *signal.NestedInnerRows.AllProbes)
	require.Equal(t, float64(12), nested.StatsInfo().RowCount)
	_, err := json.Marshal(profile)
	require.NoError(t, err)

	apply.Concurrency = 5
	require.False(t, AssessPhysicalPlanRisk(apply).Risk)
	apply.Concurrency = 0
	apply.OuterSchema = []*expression.CorrelatedColumn{{Column: expression.Column{UniqueID: 2}}}
	require.False(t, AssessPhysicalPlanRisk(apply).Risk)
	// An unrelated nested join must not match merely by being in the inner tree.
	apply.OuterSchema = []*expression.CorrelatedColumn{cor}
	relation.Conditions = nil
	require.False(t, AssessPhysicalPlanRisk(apply).Risk)
}

func TestPlanRiskUnknownRows(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	p := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	require.Nil(t, riskRows(p).PerProbe)
	for _, v := range []float64{math.Inf(1), math.NaN(), -1} {
		p.SetStats(&property.StatsInfo{RowCount: v})
		require.Nil(t, riskRows(p).PerProbe)
		require.Nil(t, riskRows(p).AllProbes)
	}
	require.False(t, AssessPhysicalPlanRisk(nil).Risk)
}

func TestPlanRiskDemandAndBlocking(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	outer := PhysicalTableReader{StoreType: kv.TiFlash}.Init(ctx, 0)
	outer.SetStats(&property.StatsInfo{RowCount: 200000})
	inner := PhysicalIndexReader{}.Init(ctx, 0)
	inner.SetStats(&property.StatsInfo{RowCount: 20})
	scan := PhysicalIndexScan{}.Init(ctx, 0)
	scan.SetStats(inner.StatsInfo())
	inner.indexPlan = scan
	join := PhysicalIndexJoin{basePhysicalJoin: basePhysicalJoin{InnerChildIdx: 1}}.Init(ctx, &property.StatsInfo{RowCount: 4000000}, 0)
	join.SetChildren(outer, inner)
	inner.SetProbeParents([]base.PhysicalPlan{join})
	scan.SetProbeParents([]base.PhysicalPlan{join})
	require.True(t, AssessPhysicalPlanRisk(join).Risk)
	limit := PhysicalLimit{Count: 10}.Init(ctx, &property.StatsInfo{RowCount: 10}, 0)
	limit.SetChildren(join)
	require.False(t, AssessPhysicalPlanRisk(limit).Risk)
	// A selective filter may consume all rows before finding ten matches.
	filter := PhysicalSelection{}.Init(ctx, &property.StatsInfo{RowCount: 10}, 0)
	filter.SetChildren(join)
	limit.SetChildren(filter)
	require.True(t, AssessPhysicalPlanRisk(limit).Risk)
	// A blocking aggregate consumes its child regardless of the outer LIMIT.
	agg := basePhysicalAgg{}.initForHash(ctx, &property.StatsInfo{RowCount: 1}, 0)
	agg.SetChildren(join)
	limit.SetChildren(agg)
	require.True(t, AssessPhysicalPlanRisk(limit).Risk)
	sort := PhysicalSort{}.Init(ctx, &property.StatsInfo{RowCount: 4000000}, 0)
	sort.SetChildren(join)
	limit.SetChildren(sort)
	require.True(t, AssessPhysicalPlanRisk(limit).Risk)
	require.False(t, NeedsTiFlashAlternative(outer))
	require.True(t, NeedsTiFlashAlternative(join))
}

func TestPlanRiskApplyCacheNDV(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	col := &expression.Column{UniqueID: 1}
	outer := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	outer.SetSchema(expression.NewSchema(col))
	outer.SetStats(&property.StatsInfo{RowCount: 100000, ColNDVs: map[int64]float64{1: 85000}})
	apply := PhysicalApply{CanUseCache: true, OuterSchema: []*expression.CorrelatedColumn{{Column: *col}}}.Init(ctx, &property.StatsInfo{RowCount: 1}, 0)
	bound := riskCacheMissLowerBound(apply, outer)
	require.NotNil(t, bound)
	require.Equal(t, float64(85000), *bound)
	delete(outer.StatsInfo().ColNDVs, 1)
	require.Nil(t, riskCacheMissLowerBound(apply, outer))
}

func TestPlanRiskDirectAggregateApply(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	cor := &expression.CorrelatedColumn{Column: expression.Column{UniqueID: 1}}
	outer := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	outer.SetStats(&property.StatsInfo{RowCount: 50000})
	scan := PhysicalTableScan{AccessCondition: []expression.Expression{cor}}.Init(ctx, 0)
	scan.SetStats(&property.StatsInfo{RowCount: 1})
	reader := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	reader.SetStats(&property.StatsInfo{RowCount: 1})
	reader.tablePlan, reader.TablePlans = scan, []base.PhysicalPlan{scan}
	agg := basePhysicalAgg{}.initForHash(ctx, &property.StatsInfo{RowCount: 1}, 0)
	agg.SetChildren(reader)
	apply := PhysicalApply{OuterSchema: []*expression.CorrelatedColumn{cor}}.Init(ctx, &property.StatsInfo{RowCount: 50000}, 0)
	apply.InnerChildIdx = 1
	apply.SetChildren(outer, agg)
	reader.SetProbeParents([]base.PhysicalPlan{apply})
	scan.SetProbeParents([]base.PhysicalPlan{apply})
	agg.SetProbeParents([]base.PhysicalPlan{apply})
	profile := AssessPhysicalPlanRisk(apply)
	require.False(t, profile.Risk)
	require.Len(t, profile.Signals, 1)
	require.Equal(t, "simple_reader_requires_read_volume", profile.Signals[0].Reason)
	require.Equal(t, "serial_correlated_apply_probe", profile.Signals[0].Kind)
	require.Equal(t, float64(1), *profile.Signals[0].InnerRows.PerProbe)
	require.Equal(t, float64(50000), *profile.Signals[0].InnerReadRows.AllProbes)
	require.Equal(t, float64(1), *profile.Signals[0].InnerReadRows.PerProbe)
	// A single aggregate output can conceal substantial remote work even with
	// few calls. Use access rows, never the aggregate's output cardinality.
	outer.StatsInfo().RowCount = 10
	scan.StatsInfo().RowCount = 200000
	profile = AssessPhysicalPlanRisk(apply)
	require.True(t, profile.Risk)
	require.Equal(t, "remote_read_volume", profile.Signals[0].Reason)
	otherScan := PhysicalTableScan{AccessCondition: []expression.Expression{cor}}.Init(ctx, 0)
	otherScan.SetStats(&property.StatsInfo{RowCount: 1})
	other := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	other.SetStats(&property.StatsInfo{RowCount: 1})
	other.tablePlan, other.TablePlans = otherScan, []base.PhysicalPlan{otherScan}
	otherScan.SetProbeParents([]base.PhysicalPlan{apply})
	other.SetProbeParents([]base.PhysicalPlan{apply})
	join := PhysicalHashJoin{}.Init(ctx, &property.StatsInfo{RowCount: 200000}, 0)
	join.SetChildren(reader, other)
	agg.SetChildren(join)
	profile = AssessPhysicalPlanRisk(apply)
	require.Len(t, profile.Signals, 1) // One decision per Apply, not per reader.
	require.Len(t, profile.Signals[0].RemoteOperators, 2)
	require.Equal(t, float64(2000010), *profile.Signals[0].InnerReadRows.AllProbes)
	require.Equal(t, float64(2000010), *EstimatePhysicalPlanReadRows(agg))
}

func TestRiskModeMissingAndEmptySchema(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	scan := PhysicalTableScan{}.Init(ctx, 0)
	scan.SetStats(&property.StatsInfo{RowCount: 30000, HistColl: &statistics.HistColl{Pseudo: true}})
	require.False(t, riskSmallRootInput(scan)) // A missing schema must not panic.
	scan.SetSchema(expression.NewSchema())
	require.False(t, riskSmallRootInput(scan)) // Zero columns cannot bypass the byte budget.
	scan.StatsInfo().RowCount = 50
	require.True(t, riskSmallRootInput(scan))
	ctx.GetSessionVars().BroadcastJoinThresholdCount = -1
	require.False(t, riskSmallRootInput(scan)) // No finite fallback for zero-width rows.
	require.Nil(t, EstimatePhysicalPlanReadRows(nil))
}

func TestRiskModeSmallBroadcastBytes(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	scan := PhysicalTableScan{}.Init(ctx, 0)
	scan.SetSchema(expression.NewSchema(&expression.Column{UniqueID: 1}))
	scan.SetStats(&property.StatsInfo{RowCount: 30000, HistColl: &statistics.HistColl{Pseudo: true}})
	require.Greater(t, scan.StatsInfo().RowCount, float64(ctx.GetSessionVars().BroadcastJoinThresholdCount))
	require.True(t, riskSmallRootInput(scan)) // Byte budget, not an extra 10K-row cap.
	scan.StatsInfo().RowCount = 30000000
	require.False(t, riskSmallRootInput(scan))
	scan.StatsInfo().RowCount = 30000
	ctx.GetSessionVars().BroadcastJoinThresholdCount = -1
	require.True(t, riskSmallRootInput(scan))
	scan.StatsInfo().HistColl = nil
	require.False(t, riskSmallRootInput(scan)) // No finite usable budget remains.
	require.Equal(t, "index_merge_lookup", riskInnerAccess(&PhysicalIndexMergeReader{tablePlan: scan}))
}

func TestPlanRiskUnorderedApplyLimitDemand(t *testing.T) {
	ctx := mock.NewContextDeprecated().GetPlanCtx()
	cor := &expression.CorrelatedColumn{Column: expression.Column{UniqueID: 1}}
	outer := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	outer.SetStats(&property.StatsInfo{RowCount: 100000})
	scan := PhysicalTableScan{AccessCondition: []expression.Expression{cor}}.Init(ctx, 0)
	scan.SetStats(&property.StatsInfo{RowCount: 40})
	reader := PhysicalTableReader{StoreType: kv.TiKV}.Init(ctx, 0)
	reader.SetStats(scan.StatsInfo())
	reader.tablePlan, reader.TablePlans = scan, []base.PhysicalPlan{scan}
	// The physical Apply's output was already scaled by ExpectedCnt, while
	// its unordered driver still has the full cardinality.
	apply := PhysicalApply{
		OuterSchema: []*expression.CorrelatedColumn{cor}, UnorderedOutputPerOuter: 0.8,
	}.Init(ctx, &property.StatsInfo{RowCount: 10}, 0)
	apply.InnerChildIdx, apply.JoinType = 1, logicalop.SemiJoin
	apply.SetChildren(outer, reader)
	reader.SetProbeParents([]base.PhysicalPlan{apply})
	scan.SetProbeParents([]base.PhysicalPlan{apply})

	limit := PhysicalLimit{Count: 10}.Init(ctx, &property.StatsInfo{RowCount: 10}, 0)
	assertCalls := func(plan base.PhysicalPlan, calls float64, risky bool) {
		t.Helper()
		profile := AssessPhysicalPlanRisk(plan)
		require.Len(t, profile.Signals, 1)
		require.InDelta(t, calls, *profile.Signals[0].EffectiveOuterCalls, 0.001)
		require.Equal(t, risky, profile.Risk)
	}
	limit.SetChildren(apply)
	assertCalls(limit, 12.5, false)
	apply.UnorderedOutputPerOuter = 0.0001
	assertCalls(limit, 100000, true)
	apply.JoinType = logicalop.LeftOuterSemiJoin
	assertCalls(limit, 10, false)
	apply.JoinType, apply.UnorderedOutputPerOuter = logicalop.SemiJoin, 0.8

	limit.Offset = 50000
	assertCalls(limit, 62512.5, true)
	limit.Offset = 0
	sort := PhysicalSort{}.Init(ctx, &property.StatsInfo{RowCount: 80000}, 0)
	sort.SetChildren(apply)
	limit.SetChildren(sort)
	assertCalls(limit, 100000, true)

	union := PhysicalUnionAll{}.Init(ctx, &property.StatsInfo{RowCount: 10}, 0)
	union.SetChildren(apply)
	limit.SetChildren(union)
	assertCalls(limit, 12.5, false)

	upper := PhysicalApply{}.Init(ctx, &property.StatsInfo{RowCount: 10}, 0)
	upper.InnerChildIdx, upper.JoinType = 1, logicalop.LeftOuterJoin
	upper.SetChildren(apply, reader)
	limit.SetChildren(upper)
	assertCalls(limit, 12.5, false)

	// A demand for join output does not cap the work of its inner subtree.
	upper.SetChildren(outer, apply)
	assertCalls(limit, 100000, true)
}

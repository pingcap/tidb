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
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tipb/go-tipb"
)

// PlanRiskRows separates one invocation from the all-probe estimate displayed
// by EXPLAIN. Neither value is a measured row count, RPC count, or I/O cost.
type PlanRiskRows struct {
	PerProbe  *float64 `json:"per_probe"`
	AllProbes *float64 `json:"all_probes"`
}

// PlanRiskSignal records a matched structure and whether it triggers the
// experimental risk policy. A match alone does not imply slow execution.
type PlanRiskSignal struct {
	ProbeShape          string        `json:"probe_shape,omitempty"`
	RemoteOperators     []string      `json:"remote_operators,omitempty"`
	InnerReadRows       PlanRiskRows  `json:"inner_read_rows"`
	EffectiveReadRows   *float64      `json:"effective_read_rows,omitempty"`
	EffectiveOuterCalls *float64      `json:"effective_outer_calls"`
	EffectiveInnerRows  *float64      `json:"effective_inner_rows,omitempty"`
	CacheMissLowerBound *float64      `json:"cache_miss_lower_bound,omitempty"`
	Demand              *float64      `json:"output_demand,omitempty"`
	Kind                string        `json:"kind"`
	Risk                bool          `json:"risk"`
	Reason              string        `json:"reason"`
	Operator            string        `json:"operator"`
	NestedOperator      string        `json:"nested_operator,omitempty"`
	OuterRows           PlanRiskRows  `json:"outer_rows"`
	InnerRows           PlanRiskRows  `json:"inner_rows"`
	NestedOuterRows     *PlanRiskRows `json:"nested_outer_rows,omitempty"`
	NestedInnerRows     *PlanRiskRows `json:"nested_inner_rows,omitempty"`
	InnerAccess         string        `json:"inner_access,omitempty"`
	ApplyConcurrency    int           `json:"apply_concurrency,omitempty"`
	ApplyCanUseCache    bool          `json:"apply_can_use_cache,omitempty"`
	SemiJoin            bool          `json:"semi_join"`
	MayShortCircuit     bool          `json:"may_short_circuit"`
}

// PhysicalPlanRisk is a diagnostic profile, not a replacement for CBO cost.
// Risk=false means no supported trigger fired, not that the plan is risk-free.
type PhysicalPlanRisk struct {
	Risk    bool             `json:"risk"`
	Signals []PlanRiskSignal `json:"signals"`
}

// AssessPhysicalPlanRisk inspects a completed physical plan, after probe parents
// and per-operator Apply concurrency have been populated. It neither changes
// the plan nor estimates spill, statistical reliability, or execution latency.
func AssessPhysicalPlanRisk(plan base.PhysicalPlan) PhysicalPlanRisk {
	profile := PhysicalPlanRisk{Signals: make([]PlanRiskSignal, 0)}
	var visit func(base.PhysicalPlan, *float64)
	visit = func(p base.PhysicalPlan, demand *float64) {
		if p == nil {
			return
		}
		if join := riskIndexJoin(p); join != nil && len(p.Children()) == 2 {
			outer, inner := p.Children()[1-join.InnerChildIdx], p.Children()[join.InnerChildIdx]
			_, outerFlash := riskStorageEngines(outer, false)
			innerKV, _ := riskStorageEngines(inner, false)
			if outerFlash && innerKV {
				reads := riskReadRows(inner)
				profile.Signals = append(profile.Signals, PlanRiskSignal{
					Kind: "cross_engine_index_probe", Operator: p.ExplainID().String(),
					OuterRows: riskRows(outer), InnerRows: riskRows(inner),
					EffectiveOuterCalls: riskEffectiveOuter(p, outer, demand), Demand: demand,
					EffectiveInnerRows: riskScaleInner(p, outer, inner, demand),
					InnerReadRows:      reads, EffectiveReadRows: riskEffectiveReads(p, outer, reads, demand),
					InnerAccess: riskInnerAccess(inner), SemiJoin: riskSemiJoin(join.JoinType),
				})
			}
		}
		if apply, ok := p.(*PhysicalApply); ok && apply.Concurrency <= 1 && len(p.Children()) == 2 {
			outer, inner := p.Children()[1-apply.InnerChildIdx], p.Children()[apply.InnerChildIdx]
			// The whole correlated inner execution is restarted for each outer row.
			// Recognize a direct remote reader too, not just nested joins.
			if riskDependsOnApply(inner, apply) {
				signalIndex := -1
				var remote func(base.PhysicalPlan)
				remote = func(n base.PhysicalPlan) {
					if n == nil {
						return
					}
					if _, cte := n.(*PhysicalCTE); cte {
						return
					}
					k, f := riskStorageEngines(n, false)
					join := riskIndexJoin(n)
					_, nestedApply := n.(*PhysicalApply)
					_, reader := riskReader(n)
					if (join != nil || nestedApply || reader) && (k || f) {
						signal := PlanRiskSignal{Kind: "serial_correlated_apply_probe", Operator: p.ExplainID().String(),
							NestedOperator: n.ExplainID().String(), OuterRows: riskRows(outer), InnerRows: riskRows(inner),
							ApplyConcurrency: max(1, apply.Concurrency), ApplyCanUseCache: apply.CanUseCache,
							SemiJoin: riskSemiJoin(apply.JoinType), MayShortCircuit: riskSemiJoin(apply.JoinType),
							EffectiveOuterCalls: riskEffectiveOuter(p, outer, demand), Demand: demand, InnerAccess: riskInnerAccess(n),
							CacheMissLowerBound: riskCacheMissLowerBound(apply, outer),
						}
						if (join != nil || nestedApply) && len(n.Children()) == 2 {
							idx := 1
							if join != nil {
								idx = join.InnerChildIdx
							} else {
								idx = n.(*PhysicalApply).InnerChildIdx
							}
							no, ni := riskRows(n.Children()[1-idx]), riskRows(n.Children()[idx])
							signal.NestedOuterRows, signal.NestedInnerRows = &no, &ni
							signal.InnerAccess = riskInnerAccess(n.Children()[idx])
							signal.ProbeShape = "nested_probe"
						} else if signal.InnerAccess == "index_lookup" || signal.InnerAccess == "index_merge_lookup" {
							signal.ProbeShape = "double_read"
						} else {
							signal.ProbeShape = "single_reader"
						}
						signal.RemoteOperators = []string{n.ExplainID().String()}
						if signalIndex < 0 {
							signal.InnerReadRows = riskReadRows(inner)
							signal.EffectiveReadRows = riskEffectiveReads(p, outer, signal.InnerReadRows, demand)
							signalIndex = len(profile.Signals)
							profile.Signals = append(profile.Signals, signal)
						} else {
							existing := &profile.Signals[signalIndex]
							existing.RemoteOperators = append(existing.RemoteOperators, signal.RemoteOperators...)
							if signal.ProbeShape == "nested_probe" || (signal.ProbeShape == "double_read" && existing.ProbeShape == "single_reader") {
								existing.ProbeShape = signal.ProbeShape
								existing.NestedOperator, existing.InnerAccess = signal.NestedOperator, signal.InnerAccess
								existing.NestedOuterRows, existing.NestedInnerRows = signal.NestedOuterRows, signal.NestedInnerRows
							}
						}
						return
					}
					for _, child := range n.Children() {
						remote(child)
					}
				}
				remote(inner)
			}
		}
		if cte, ok := p.(*PhysicalCTE); ok {
			visit(cte.SeedPlan, nil)
			visit(cte.RecurPlan, nil)
			return
		}
		// Reader pushdowns contain no root Apply or IndexJoin operators.
		childDemand := riskChildDemand(p, demand)
		// A streaming probe join can pass consumption demand to its driver.
		// The inner subtree is invoked repeatedly and has a separate demand scope.
		if idx, ok := riskProbeOuterIndex(p); ok && len(p.Children()) == 2 {
			outer := p.Children()[idx]
			if demand != nil {
				childDemand = riskOuterDemandPerProbe(p, outer, demand)
			}
			visit(outer, childDemand)
			visit(p.Children()[1-idx], nil)
			return
		}
		for _, child := range p.Children() {
			visit(child, childDemand)
		}
	}
	visit(plan, nil)
	for i := range profile.Signals {
		signal := &profile.Signals[i]
		signal.Risk, signal.Reason = riskSignalDecision(*signal)
		profile.Risk = profile.Risk || signal.Risk
	}
	return profile
}

// Experimental call/read-volume boundaries; not calibrated latency estimates.
const (
	RiskCrossEngineMinOuterRows = 1 << 16
	RiskCrossEngineMinInnerRows = 1 << 20
	RiskApplyMinOuterRows       = 1 << 10
)

func riskSignalDecision(s PlanRiskSignal) (bool, string) {
	calls := s.EffectiveOuterCalls
	if calls == nil {
		calls = s.OuterRows.AllProbes
	}
	if !riskFiniteRows(calls) {
		return false, "unknown_outer_calls"
	}
	switch s.Kind {
	case "cross_engine_index_probe":
		rows := s.EffectiveReadRows
		if rows == nil {
			rows = s.InnerReadRows.AllProbes
		}
		// Covering probes need no row-data fetch; double-read/table probes have
		// a separate call-volume trigger. Either large fanout or many calls can fire.
		callThreshold := float64(RiskCrossEngineMinInnerRows)
		if s.InnerAccess == "index_lookup" || s.InnerAccess == "index_merge_lookup" || s.InnerAccess == "table_reader" || s.InnerAccess == "point_get" {
			callThreshold = RiskCrossEngineMinOuterRows
		}
		if *calls >= callThreshold || (riskFiniteRows(rows) && *rows >= RiskCrossEngineMinInnerRows) {
			return true, "remote_probe_volume"
		}
		return false, "remote_probe_volume_below_threshold"
	case "serial_correlated_apply_probe":
		effective := *calls
		if s.ApplyCanUseCache {
			// NDV is a best-case miss bound, not a prediction of actual hit rate.
			// Capacity, eviction and ordering can still make every call miss.
			if !riskFiniteRows(s.CacheMissLowerBound) {
				return false, "cache_misses_unknown"
			}
			effective = min(effective, *s.CacheMissLowerBound)
		}
		if effective >= RiskApplyMinOuterRows && (s.ProbeShape == "nested_probe" || s.ProbeShape == "double_read") {
			return true, "serial_remote_calls"
		}
		reads := s.EffectiveReadRows
		if riskFiniteRows(reads) && *calls > 0 && *reads*(effective / *calls) >= RiskCrossEngineMinInnerRows {
			return true, "remote_read_volume"
		}
		if s.ApplyCanUseCache {
			return false, "cache_reuse_uncertain"
		}
		if s.ProbeShape == "single_reader" {
			return false, "simple_reader_requires_read_volume"
		}
		return false, "serial_calls_below_threshold"
	}
	return false, "unsupported_pattern"
}

func riskFiniteRows(v *float64) bool {
	return v != nil && *v >= 0 && !math.IsNaN(*v) && !math.IsInf(*v, 0)
}

// Demand is estimated output consumption per invocation through a streaming path. A
// filter may require many input rows; a blocking operator always clears it.
func riskChildDemand(p base.PhysicalPlan, demand *float64) *float64 {
	switch x := p.(type) {
	case *PhysicalLimit:
		count := float64(x.Count)
		if demand != nil {
			count = min(count, *demand)
		}
		needed := float64(x.Offset) + count
		return &needed
	case *PhysicalProjection, *PhysicalUnionAll:
		return demand
	case *PhysicalSelection:
		if demand != nil && len(p.Children()) == 1 {
			out, in := riskRows(p).PerProbe, riskRows(p.Children()[0]).PerProbe
			if riskFiniteRows(out) && riskFiniteRows(in) && *out > 0 {
				needed := min(*in, *demand**in / *out)
				return &needed
			}
		}
	}
	return nil
}

func riskProbeOuterIndex(p base.PhysicalPlan) (int, bool) {
	if apply, ok := p.(*PhysicalApply); ok {
		return 1 - apply.InnerChildIdx, true
	}
	if join := riskIndexJoin(p); join != nil {
		return 1 - join.InnerChildIdx, true
	}
	return 0, false
}

func riskOuterDemandPerProbe(p, outer base.PhysicalPlan, demand *float64) *float64 {
	rows := riskRows(outer).PerProbe
	if demand == nil || !riskFiniteRows(rows) {
		return rows
	}
	if apply, ok := p.(*PhysicalApply); ok {
		switch apply.JoinType {
		case logicalop.LeftOuterJoin, logicalop.LeftOuterSemiJoin, logicalop.AntiLeftOuterSemiJoin:
			// Each outer input contributes at least one output.
			needed := min(*rows, *demand)
			return &needed
		}
		if rate := apply.UnorderedOutputPerOuter; rate > 0 && !math.IsInf(rate, 0) {
			needed := min(*rows, *demand/rate)
			return &needed
		}
	}
	output := riskRows(p).PerProbe
	if !riskFiniteRows(output) || *output <= 0 {
		return rows
	}
	needed := min(*rows, *demand**rows / *output)
	return &needed
}

// Convert local demand to total calls only at the point where we record the
// risk. Propagating total calls would charge the enclosing probe parents twice.
func riskEffectiveOuter(p, outer base.PhysicalPlan, demand *float64) *float64 {
	rows := riskRows(outer)
	if demand == nil {
		return rows.AllProbes
	}
	needed := riskOuterDemandPerProbe(p, outer, demand)
	if !riskFiniteRows(rows.AllProbes) || !riskFiniteRows(rows.PerProbe) ||
		!riskFiniteRows(needed) || *rows.PerProbe == 0 {
		return rows.AllProbes
	}
	calls := *rows.AllProbes * (*needed / *rows.PerProbe)
	return &calls
}

func riskScaleInner(p, outer, inner base.PhysicalPlan, demand *float64) *float64 {
	rows, calls, effective := riskRows(inner).AllProbes, riskRows(outer).AllProbes, riskEffectiveOuter(p, outer, demand)
	if !riskFiniteRows(rows) || !riskFiniteRows(calls) || !riskFiniteRows(effective) || *calls == 0 {
		return nil
	}
	scaled := *rows * (*effective / *calls)
	return &scaled
}

func riskCacheMissLowerBound(apply *PhysicalApply, outer base.PhysicalPlan) *float64 {
	if !apply.CanUseCache || outer.StatsInfo() == nil || outer.Schema() == nil || len(apply.OuterSchema) == 0 {
		return nil
	}
	cols := make([]*expression.Column, 0, len(apply.OuterSchema))
	for _, col := range apply.OuterSchema {
		if _, ok := outer.StatsInfo().ColNDVs[col.UniqueID]; !ok {
			return nil
		}
		cols = append(cols, &col.Column)
	}
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(cols, outer.Schema(), outer.StatsInfo())
	rows := riskRows(outer).AllProbes
	if !riskFiniteRows(rows) || ndv < 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) {
		return nil
	}
	// Do not multiply by parent probes: the cache lifetime may cross invocations.
	ndv = min(ndv, *rows)
	return &ndv
}

// Count access rows at storage scans, before selections/aggregations. Index
// and table legs of a lookup are separate accesses, not output cardinalities.
func riskReadRows(p base.PhysicalPlan) PlanRiskRows {
	seen := make(map[base.PhysicalPlan]bool)
	total, found, unknown := 0.0, false, false
	var visit func(base.PhysicalPlan)
	visit = func(n base.PhysicalPlan) {
		if n == nil || seen[n] {
			return
		}
		seen[n] = true
		switch x := n.(type) {
		case *PhysicalTableScan, *PhysicalIndexScan, *PointGetPlan, *BatchPointGetPlan:
			r := riskRows(n)
			if riskFiniteRows(r.PerProbe) && riskFiniteRows(r.AllProbes) {
				total += *r.AllProbes
				found = true
			} else {
				unknown = true
			}
			return
		case *PhysicalTableReader:
			visit(x.tablePlan)
			return
		case *PhysicalIndexReader:
			visit(x.indexPlan)
			return
		case *PhysicalIndexLookUpReader:
			visit(x.indexPlan)
			visit(x.tablePlan)
			return
		case *PhysicalIndexMergeReader:
			for _, partial := range x.partialPlans {
				visit(partial)
			}
			visit(x.tablePlan)
			return
		case *PhysicalCTE:
			unknown = true
			return
		}
		for _, child := range n.Children() {
			visit(child)
		}
	}
	visit(p)
	if !found || unknown || !riskFiniteRows(&total) {
		return PlanRiskRows{}
	}
	result := PlanRiskRows{AllProbes: &total}
	// Different leaves can have different probe parents. Normalize the sum to
	// this subtree's invocation count; never sum their raw per-probe estimates.
	scope := riskRows(p)
	if riskFiniteRows(scope.PerProbe) && riskFiniteRows(scope.AllProbes) && *scope.PerProbe > 0 && *scope.AllProbes > 0 {
		per := total / (*scope.AllProbes / *scope.PerProbe)
		if riskFiniteRows(&per) {
			result.PerProbe = &per
		}
	}
	return result
}

func riskEffectiveReads(p, outer base.PhysicalPlan, innerReadRows PlanRiskRows, demand *float64) *float64 {
	reads, calls, effective := innerReadRows.AllProbes, riskRows(outer).AllProbes, riskEffectiveOuter(p, outer, demand)
	if !riskFiniteRows(reads) || !riskFiniteRows(calls) || !riskFiniteRows(effective) || *calls == 0 {
		return nil
	}
	value := *reads * (*effective / *calls)
	return &value
}

// EstimatePhysicalPlanReadRows sums estimated storage-scan access rows. Lookup
// index/table legs count separately; exchanges do not multiply scan rows. Nil
// means unknown. This is not a byte, RPC, unique-row, or RU estimate.
func EstimatePhysicalPlanReadRows(p base.PhysicalPlan) *float64 {
	return riskReadRows(p).AllProbes
}

// NeedsTiFlashAlternative limits the extra engine search to actual join/apply
// plans. A normal single-table access already compares its engine paths.
func NeedsTiFlashAlternative(p base.PhysicalPlan) bool {
	tikv, _ := riskStorageEngines(p, true)
	return tikv && riskContainsJoin(p)
}

func riskContainsJoin(p base.PhysicalPlan) bool {
	if p == nil {
		return false
	}
	if riskIndexJoin(p) != nil {
		return true
	}
	switch x := p.(type) {
	case *PhysicalApply, *PhysicalHashJoin, *PhysicalMergeJoin:
		return true
	case *PhysicalTableReader:
		return riskContainsJoin(x.tablePlan)
	case *PhysicalCTE:
		return riskContainsJoin(x.SeedPlan) || riskContainsJoin(x.RecurPlan)
	}
	for _, child := range p.Children() {
		if riskContainsJoin(child) {
			return true
		}
	}
	return false
}

func riskReader(p base.PhysicalPlan) (base.PhysicalPlan, bool) {
	switch x := p.(type) {
	case *PhysicalTableReader:
		return x.tablePlan, x.StoreType != kv.TiDB
	case *PhysicalIndexReader:
		return x.indexPlan, true
	case *PhysicalIndexLookUpReader:
		return x.indexPlan, true
	case *PhysicalIndexMergeReader, *PointGetPlan, *BatchPointGetPlan:
		return p, true
	}
	return nil, false
}

// IsRiskModeMPPAlternative requires MPP storage/joins, permitting small final
// root aggregations and broadcasts within existing configured budgets. CTEs
// remain excluded. This does not rule out runtime skew or memory pressure.
func IsRiskModeMPPAlternative(plan base.PhysicalPlan) bool {
	hasMPP := false
	var check func(base.PhysicalPlan, bool) bool
	check = func(p base.PhysicalPlan, inMPP bool) bool {
		if p == nil {
			return false
		}
		switch x := p.(type) {
		case *PhysicalApply, *PhysicalIndexJoin, *PhysicalIndexHashJoin, *PhysicalIndexMergeJoin,
			*PhysicalIndexReader, *PhysicalIndexLookUpReader, *PhysicalIndexMergeReader,
			*PointGetPlan, *BatchPointGetPlan, *PhysicalCTE, *PhysicalCTETable:
			return false
		case *PhysicalTableReader:
			if x.StoreType != kv.TiFlash || x.ReadReqType != MPP {
				return false
			}
			hasMPP = true
			return check(x.tablePlan, true)
		case *PhysicalTableScan:
			if !inMPP {
				return false
			}
		case *PhysicalHashJoin, *PhysicalMergeJoin:
			if !inMPP {
				return false
			}
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			if !inMPP && (len(p.Children()) != 1 || !riskSmallRootInput(p.Children()[0])) {
				return false
			}
		case *PhysicalExchangeSender:
			if !inMPP || (x.ExchangeType == tipb.ExchangeType_Broadcast && (len(p.Children()) != 1 || !riskSmallRootInput(p.Children()[0]))) {
				return false
			}
		}
		for _, child := range p.Children() {
			if !check(child, inMPP) {
				return false
			}
		}
		return true
	}
	return check(plan, false) && hasMPP
}

// Reuse existing session broadcast budgets, requiring a finite usable bound.
// These limits bound small final root work; they do not predict spilling.
func riskSmallRootInput(p base.PhysicalPlan) bool {
	if p == nil || p.StatsInfo() == nil || !riskFiniteRows(riskRows(p).AllProbes) {
		return false
	}
	sv := p.SCtx().GetSessionVars()
	// Follow the planner's byte-size path when histograms are available; only
	// use the row-count fallback without histograms. Reject unlimited budgets.
	if p.StatsInfo().HistColl != nil {
		if p.Schema() == nil || sv.BroadcastJoinThresholdSize <= 0 {
			return false
		}
		if len(p.Schema().Columns) == 0 {
			// Zero estimated width is not free execution. Bound tuple/state
			// overhead with the existing row budget instead of passing 0 bytes.
			rows := riskRows(p).AllProbes
			return sv.BroadcastJoinThresholdCount > 0 && *rows < float64(sv.BroadcastJoinThresholdCount)
		}
	} else if sv.BroadcastJoinThresholdCount <= 0 {
		return false
	}
	return checkChildFitBC(p)
}

func riskRows(p base.PhysicalPlan) PlanRiskRows {
	if p == nil || p.StatsInfo() == nil {
		return PlanRiskRows{}
	}
	finite := func(v float64) *float64 {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return nil
		}
		return &v
	}
	return PlanRiskRows{PerProbe: finite(p.StatsInfo().RowCount), AllProbes: finite(p.GetEstRowCountForDisplay())}
}

func riskIndexJoin(p base.PhysicalPlan) *PhysicalIndexJoin {
	switch x := p.(type) {
	case *PhysicalIndexJoin:
		return x
	case *PhysicalIndexHashJoin:
		return &x.PhysicalIndexJoin
	case *PhysicalIndexMergeJoin:
		return &x.PhysicalIndexJoin
	default:
		return nil
	}
}

func riskSemiJoin(t logicalop.JoinType) bool {
	return t == logicalop.SemiJoin || t == logicalop.AntiSemiJoin ||
		t == logicalop.LeftOuterSemiJoin || t == logicalop.AntiLeftOuterSemiJoin
}

func riskDependsOnApply(p base.PhysicalPlan, apply *PhysicalApply) bool {
	for _, col := range coreusage.ExtractCorrelatedCols4PhysicalPlan(p) {
		for _, outer := range apply.OuterSchema {
			if col.UniqueID == outer.UniqueID {
				return true
			}
		}
	}
	return false
}

func riskStorageEngines(p base.PhysicalPlan, includeCTEProducers bool) (tikv, tiflash bool) {
	if p == nil {
		return
	}
	switch x := p.(type) {
	case *PhysicalTableReader:
		return x.StoreType == kv.TiKV, x.StoreType == kv.TiFlash
	case *PhysicalIndexReader, *PhysicalIndexLookUpReader, *PhysicalIndexMergeReader, *PointGetPlan, *BatchPointGetPlan:
		return true, false
	case *PhysicalCTE:
		// A producer's engine is not evidence of repeated reads by its consumer.
		if !includeCTEProducers {
			return false, false
		}
		seedKV, seedFlash := riskStorageEngines(x.SeedPlan, true)
		recurKV, recurFlash := riskStorageEngines(x.RecurPlan, true)
		return seedKV || recurKV, seedFlash || recurFlash
	}
	for _, child := range p.Children() {
		k, f := riskStorageEngines(child, includeCTEProducers)
		tikv, tiflash = tikv || k, tiflash || f
	}
	return
}

func riskInnerAccess(p base.PhysicalPlan) string {
	switch x := p.(type) {
	case *PhysicalIndexReader:
		return "covering_index"
	case *PhysicalIndexLookUpReader:
		return "index_lookup"
	case *PhysicalIndexMergeReader:
		if x.tablePlan != nil {
			return "index_merge_lookup"
		}
		return "covering_index"
	case *PhysicalTableReader:
		return "table_reader"
	case *PointGetPlan, *BatchPointGetPlan:
		return "point_get"
	}
	if len(p.Children()) == 1 {
		return riskInnerAccess(p.Children()[0])
	}
	return "other"
}

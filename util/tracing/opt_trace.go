// Copyright 2021 PingCAP, Inc.
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

package tracing

import "fmt"

// PlanTrace indicates for the Plan trace information
type PlanTrace struct {
	ID         int          `json:"id"`
	TP         string       `json:"type"`
	Children   []*PlanTrace `json:"-"`
	ChildrenID []int        `json:"children"`
	Cost       float64      `json:"cost"`
	Selected   bool         `json:"selected"`
	ProperType string       `json:"property"`
	// ExplainInfo should be implemented by each implemented Plan
	ExplainInfo string `json:"info"`
}

// LogicalOptimizeTracer indicates the trace for the whole logicalOptimize processing
type LogicalOptimizeTracer struct {
	FinalLogicalPlan []*PlanTrace                 `json:"final"`
	Steps            []*LogicalRuleOptimizeTracer `json:"steps"`
	// curRuleTracer indicates the current rule Tracer during optimize by rule
	curRuleTracer *LogicalRuleOptimizeTracer
}

// AppendRuleTracerBeforeRuleOptimize add plan tracer before optimize
func (tracer *LogicalOptimizeTracer) AppendRuleTracerBeforeRuleOptimize(index int, name string, before *PlanTrace) {
	ruleTracer := buildLogicalRuleOptimizeTracerBeforeOptimize(index, name, before)
	tracer.Steps = append(tracer.Steps, ruleTracer)
	tracer.curRuleTracer = ruleTracer
}

// AppendRuleTracerStepToCurrent add rule optimize step to current
func (tracer *LogicalOptimizeTracer) AppendRuleTracerStepToCurrent(id int, tp, reason, action string) {
	index := len(tracer.curRuleTracer.Steps)
	tracer.curRuleTracer.Steps = append(tracer.curRuleTracer.Steps, LogicalRuleOptimizeTraceStep{
		ID:     id,
		TP:     tp,
		Reason: reason,
		Action: action,
		Index:  index,
	})
}

// RecordFinalLogicalPlan add plan trace after logical optimize
func (tracer *LogicalOptimizeTracer) RecordFinalLogicalPlan(final *PlanTrace) {
	tracer.FinalLogicalPlan = toFlattenPlanTrace(final)
	tracer.removeUselessStep()
}

// TODO: use a switch to control it
func (tracer *LogicalOptimizeTracer) removeUselessStep() {
	newSteps := make([]*LogicalRuleOptimizeTracer, 0)
	for _, step := range tracer.Steps {
		if len(step.Steps) > 0 {
			newSteps = append(newSteps, step)
		}
	}
	tracer.Steps = newSteps
}

// LogicalRuleOptimizeTracer indicates the trace for the LogicalPlan tree before and after
// logical rule optimize
type LogicalRuleOptimizeTracer struct {
	Index    int                            `json:"index"`
	Before   []*PlanTrace                   `json:"before"`
	RuleName string                         `json:"name"`
	Steps    []LogicalRuleOptimizeTraceStep `json:"steps"`
}

// buildLogicalRuleOptimizeTracerBeforeOptimize build rule tracer before rule optimize
func buildLogicalRuleOptimizeTracerBeforeOptimize(index int, name string, before *PlanTrace) *LogicalRuleOptimizeTracer {
	return &LogicalRuleOptimizeTracer{
		Index:    index,
		Before:   toFlattenPlanTrace(before),
		RuleName: name,
		Steps:    make([]LogicalRuleOptimizeTraceStep, 0),
	}
}

// LogicalRuleOptimizeTraceStep indicates the trace for the detailed optimize changing in
// logical rule optimize
type LogicalRuleOptimizeTraceStep struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
	ID     int    `json:"id"`
	TP     string `json:"type"`
	Index  int    `json:"index"`
}

// toFlattenLogicalPlanTrace transform LogicalPlanTrace into FlattenLogicalPlanTrace
func toFlattenPlanTrace(root *PlanTrace) []*PlanTrace {
	wrapper := &flattenWrapper{flatten: make([]*PlanTrace, 0)}
	flattenLogicalPlanTrace(root, wrapper)
	return wrapper.flatten
}

type flattenWrapper struct {
	flatten []*PlanTrace
}

func flattenLogicalPlanTrace(node *PlanTrace, wrapper *flattenWrapper) {
	newNode := &PlanTrace{
		ID:          node.ID,
		TP:          node.TP,
		ChildrenID:  make([]int, 0),
		Cost:        node.Cost,
		ExplainInfo: node.ExplainInfo,
	}
	if len(node.Children) < 1 {
		wrapper.flatten = append(wrapper.flatten, newNode)
		return
	}
	for _, child := range node.Children {
		newNode.ChildrenID = append(newNode.ChildrenID, child.ID)
	}
	for _, child := range node.Children {
		flattenLogicalPlanTrace(child, wrapper)
	}
	wrapper.flatten = append(wrapper.flatten, newNode)
}

// CETraceRecord records an expression and related cardinality estimation result.
type CETraceRecord struct {
	TableID   int64  `json:"-"`
	TableName string `json:"table_name"`
	Type      string `json:"type"`
	Expr      string `json:"expr"`
	RowCount  uint64 `json:"row_count"`
}

// DedupCETrace deduplicate a slice of *CETraceRecord and return the deduplicated slice
func DedupCETrace(records []*CETraceRecord) []*CETraceRecord {
	ret := make([]*CETraceRecord, 0, len(records))
	exists := make(map[CETraceRecord]struct{}, len(records))
	for _, rec := range records {
		if _, ok := exists[*rec]; !ok {
			ret = append(ret, rec)
			exists[*rec] = struct{}{}
		}
	}
	return ret
}

// PhysicalOptimizeTracer indicates the trace for the whole physicalOptimize processing
type PhysicalOptimizeTracer struct {
	// final indicates the final physical plan trace
	Final               []*PlanTrace          `json:"final"`
	SelectedCandidates  []*CandidatePlanTrace `json:"selected_candidates"`
	DiscardedCandidates []*CandidatePlanTrace `json:"discarded_candidates"`
	// (logical plan) -> physical plan codename -> physical plan
	State map[string]map[string]*PlanTrace `json:"-"`
}

// RecordFinalPlanTrace records final physical plan trace
func (tracer *PhysicalOptimizeTracer) RecordFinalPlanTrace(root *PlanTrace) {
	tracer.Final = toFlattenPlanTrace(root)
	tracer.buildCandidatesInfo()
}

// CandidatePlanTrace indicates info for candidate
type CandidatePlanTrace struct {
	*PlanTrace
	MappingLogicalPlan string `json:"mapping"`
}

func newCandidatePlanTrace(trace *PlanTrace, logicalPlanKey string, bestKey map[string]struct{}) *CandidatePlanTrace {
	selected := false
	if _, ok := bestKey[CodecPlanName(trace.TP, trace.ID)]; ok {
		selected = true
	}
	c := &CandidatePlanTrace{
		MappingLogicalPlan: logicalPlanKey,
	}
	c.PlanTrace = trace
	c.Selected = selected
	for i, child := range c.Children {
		if _, ok := bestKey[CodecPlanName(child.TP, child.ID)]; ok {
			child.Selected = true
		}
		c.Children[i] = child
	}
	return c
}

// buildCandidatesInfo builds candidates info
func (tracer *PhysicalOptimizeTracer) buildCandidatesInfo() {
	if tracer == nil || len(tracer.State) < 1 {
		return
	}
	sCandidates := make([]*CandidatePlanTrace, 0)
	dCandidates := make([]*CandidatePlanTrace, 0)
	bestKeys := map[string]struct{}{}
	for _, node := range tracer.Final {
		bestKeys[CodecPlanName(node.TP, node.ID)] = struct{}{}
	}
	for logicalKey, pps := range tracer.State {
		for _, pp := range pps {
			c := newCandidatePlanTrace(pp, logicalKey, bestKeys)
			if c.Selected {
				sCandidates = append(sCandidates, c)
			} else {
				dCandidates = append(dCandidates, c)
			}
		}
	}
	tracer.SelectedCandidates = sCandidates
	tracer.DiscardedCandidates = dCandidates
}

// CodecPlanName returns tp_id of plan.
func CodecPlanName(tp string, id int) string {
	return fmt.Sprintf("%v_%v", tp, id)
}

// OptimizeTracer indicates tracer for optimizer
type OptimizeTracer struct {
	// Logical indicates logical plan
	Logical *LogicalOptimizeTracer `json:"logical"`
	// Physical indicates physical plan
	Physical *PhysicalOptimizeTracer `json:"physical"`
	// FinalPlan indicates the plan after post optimize
	FinalPlan []*PlanTrace `json:"final"`
	// IsFastPlan indicates whether the plan is generated by fast plan
	IsFastPlan bool
}

// SetFastPlan sets fast plan
func (tracer *OptimizeTracer) SetFastPlan(final *PlanTrace) {
	tracer.FinalPlan = toFlattenPlanTrace(final)
	tracer.IsFastPlan = true
}

// RecordFinalPlan records plan after post optimize
func (tracer *OptimizeTracer) RecordFinalPlan(final *PlanTrace) {
	tracer.FinalPlan = toFlattenPlanTrace(final)
}

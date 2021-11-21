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

// LogicalPlanTrace indicates for the LogicalPlan trace information
type LogicalPlanTrace struct {
	ID       int
	TP       string
	Children []*LogicalPlanTrace

	// ExplainInfo should be implemented by each implemented LogicalPlan
	ExplainInfo string
}

// LogicalOptimizeTracer indicates the trace for the whole logicalOptimize processing
type LogicalOptimizeTracer struct {
	Steps []*LogicalRuleOptimizeTracer
	// curRuleTracer indicates the current rule Tracer during optimize by rule
	curRuleTracer *LogicalRuleOptimizeTracer
}

// AppendRuleTracerBeforeRuleOptimize add plan tracer before optimize
func (tracer *LogicalOptimizeTracer) AppendRuleTracerBeforeRuleOptimize(name string, before *LogicalPlanTrace) {
	ruleTracer := buildLogicalRuleOptimizeTracerBeforeOptimize(name, before)
	tracer.Steps = append(tracer.Steps, ruleTracer)
	tracer.curRuleTracer = ruleTracer
}

// AppendRuleTracerStepToCurrent add rule optimize step to current
func (tracer *LogicalOptimizeTracer) AppendRuleTracerStepToCurrent(id int, tp, reason, action string) {
	tracer.curRuleTracer.Steps = append(tracer.curRuleTracer.Steps, LogicalRuleOptimizeTraceStep{
		ID:     id,
		TP:     tp,
		Reason: reason,
		Action: action,
	})
}

// TrackLogicalPlanAfterRuleOptimize add plan trace after optimize
func (tracer *LogicalOptimizeTracer) TrackLogicalPlanAfterRuleOptimize(after *LogicalPlanTrace) {
	tracer.curRuleTracer.After = after
}

// LogicalRuleOptimizeTracer indicates the trace for the LogicalPlan tree before and after
// logical rule optimize
type LogicalRuleOptimizeTracer struct {
	Before   *LogicalPlanTrace
	After    *LogicalPlanTrace
	RuleName string
	Steps    []LogicalRuleOptimizeTraceStep
}

// buildLogicalRuleOptimizeTracerBeforeOptimize build rule tracer before rule optimize
func buildLogicalRuleOptimizeTracerBeforeOptimize(name string, before *LogicalPlanTrace) *LogicalRuleOptimizeTracer {
	return &LogicalRuleOptimizeTracer{
		Before:   before,
		RuleName: name,
		Steps:    make([]LogicalRuleOptimizeTraceStep, 0),
	}
}

// LogicalRuleOptimizeTraceStep indicates the trace for the detailed optimize changing in
// logical rule optimize
type LogicalRuleOptimizeTraceStep struct {
	Action string
	Reason string
	ID     int
	TP     string
}

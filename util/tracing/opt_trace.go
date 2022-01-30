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
	ID       int                 `json:"id"`
	TP       string              `json:"type"`
	Children []*LogicalPlanTrace `json:"children"`

	// ExplainInfo should be implemented by each implemented LogicalPlan
	ExplainInfo string `json:"info"`
}

// LogicalOptimizeTracer indicates the trace for the whole logicalOptimize processing
type LogicalOptimizeTracer struct {
	FinalLogicalPlan *LogicalPlanTrace            `json:"final"`
	Steps            []*LogicalRuleOptimizeTracer `json:"steps"`
	// curRuleTracer indicates the current rule Tracer during optimize by rule
	curRuleTracer *LogicalRuleOptimizeTracer
}

// AppendRuleTracerBeforeRuleOptimize add plan tracer before optimize
func (tracer *LogicalOptimizeTracer) AppendRuleTracerBeforeRuleOptimize(index int, name string, before *LogicalPlanTrace) {
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
func (tracer *LogicalOptimizeTracer) RecordFinalLogicalPlan(final *LogicalPlanTrace) {
	tracer.FinalLogicalPlan = final
}

// LogicalRuleOptimizeTracer indicates the trace for the LogicalPlan tree before and after
// logical rule optimize
type LogicalRuleOptimizeTracer struct {
	Index    int                            `json:"index"`
	Before   *LogicalPlanTrace              `json:"before"`
	RuleName string                         `json:"name"`
	Steps    []LogicalRuleOptimizeTraceStep `json:"steps"`
}

// buildLogicalRuleOptimizeTracerBeforeOptimize build rule tracer before rule optimize
func buildLogicalRuleOptimizeTracerBeforeOptimize(index int, name string, before *LogicalPlanTrace) *LogicalRuleOptimizeTracer {
	return &LogicalRuleOptimizeTracer{
		Index:    index,
		Before:   before,
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

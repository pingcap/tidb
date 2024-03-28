// Copyright 2024 PingCAP, Inc.
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

package util

import "github.com/pingcap/tidb/pkg/util/tracing"

// optTracer define those basic element for logical optimizing trace and physical optimizing trace.
// logicalOptRule inside the accommodated pkg `util` should only be depended on by logical `rule` pkg.
//
//  rule related -----> core/util

// LogicalOptimizeOp  is logical optimizing option for tracing.
type LogicalOptimizeOp struct {
	// tracer is goring to track optimize steps during rule optimizing
	tracer *tracing.LogicalOptimizeTracer
}

// TracerIsNil returns whether inside tracer is nil
func (op *LogicalOptimizeOp) TracerIsNil() bool {
	return op.tracer == nil
}

// DefaultLogicalOptimizeOption returns the default LogicalOptimizeOp.
func DefaultLogicalOptimizeOption() *LogicalOptimizeOp {
	return &LogicalOptimizeOp{}
}

// WithEnableOptimizeTracer attach the customized tracer to current LogicalOptimizeOp.
func (op *LogicalOptimizeOp) WithEnableOptimizeTracer(tracer *tracing.LogicalOptimizeTracer) *LogicalOptimizeOp {
	op.tracer = tracer
	return op
}

// AppendBeforeRuleOptimize just appends a before-rule plan tracer.
func (op *LogicalOptimizeOp) AppendBeforeRuleOptimize(index int, name string, build func() *tracing.PlanTrace) {
	if op == nil || op.tracer == nil {
		return
	}
	op.tracer.AppendRuleTracerBeforeRuleOptimize(index, name, build())
}

// AppendStepToCurrent appends a step of current action.
func (op *LogicalOptimizeOp) AppendStepToCurrent(id int, tp string, reason, action func() string) {
	if op == nil || op.tracer == nil {
		return
	}
	op.tracer.AppendRuleTracerStepToCurrent(id, tp, reason(), action())
}

// RecordFinalLogicalPlan records the final logical plan.
func (op *LogicalOptimizeOp) RecordFinalLogicalPlan(build func() *tracing.PlanTrace) {
	if op == nil || op.tracer == nil {
		return
	}
	op.tracer.RecordFinalLogicalPlan(build())
}

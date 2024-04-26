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

package optimizetrace

import (
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// optimizetrace and costusage is isolated from util because core/base depended on them for
// interface definition. Ideally, the dependency chain should be:
//
// `base` <- `util`/`util.coreusage` <- `core`
//    ^ +---------------^                  |
//    +------------------------------------+
//
// since `base` depended on optimizetrace and costusage for definition, we should separate
// them out of `util`/`util.coreusage` to avoid import cycle.
//
// util.optimizetrace/util.costusage  <- `base` <- `util`/`util.coreusage` <- `core`
//   				^   		            ^                                    ||
//   				|   		            +------------------------------------+|
//                  +-------------------------------------------------------------+
//
// optTracer define those basic element for logical optimizing trace and physical optimizing trace.
//
//********************** below logical optimize trace related *************************

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

//********************** below physical optimize trace related *************************

// PhysicalOptimizeOp  is logical optimizing option for tracing.
type PhysicalOptimizeOp struct {
	// tracer is goring to track optimize steps during physical optimizing
	tracer *tracing.PhysicalOptimizeTracer
}

// DefaultPhysicalOptimizeOption is default physical optimizing option.
func DefaultPhysicalOptimizeOption() *PhysicalOptimizeOp {
	return &PhysicalOptimizeOp{}
}

// WithEnableOptimizeTracer is utility func to append the PhysicalOptimizeTracer into current PhysicalOptimizeOp.
func (op *PhysicalOptimizeOp) WithEnableOptimizeTracer(tracer *tracing.PhysicalOptimizeTracer) *PhysicalOptimizeOp {
	op.tracer = tracer
	return op
}

// AppendCandidate is utility func to append the CandidatePlanTrace into current PhysicalOptimizeOp.
func (op *PhysicalOptimizeOp) AppendCandidate(c *tracing.CandidatePlanTrace) {
	op.tracer.AppendCandidate(c)
}

// GetTracer returns the current op's PhysicalOptimizeTracer.
func (op *PhysicalOptimizeOp) GetTracer() *tracing.PhysicalOptimizeTracer {
	return op.tracer
}

// NewDefaultPlanCostOption returns PlanCostOption
func NewDefaultPlanCostOption() *PlanCostOption {
	return &PlanCostOption{}
}

// PlanCostOption indicates option during GetPlanCost
type PlanCostOption struct {
	CostFlag uint64
	tracer   *PhysicalOptimizeOp
}

// GetTracer returns the current op's PhysicalOptimizeOp.
func (op *PlanCostOption) GetTracer() *PhysicalOptimizeOp {
	return op.tracer
}

// WithCostFlag set cost flag
func (op *PlanCostOption) WithCostFlag(flag uint64) *PlanCostOption {
	if op == nil {
		return nil
	}
	op.CostFlag = flag
	return op
}

// CostFlagTrace is a mirror of costusage.CostFlagTrace, leveraging
// initialization assigment to avoid import cycle.
var CostFlagTrace uint64

// WithOptimizeTracer set tracer
func (op *PlanCostOption) WithOptimizeTracer(v *PhysicalOptimizeOp) *PlanCostOption {
	if op == nil {
		return nil
	}
	op.tracer = v
	if v != nil && v.tracer != nil {
		op.CostFlag |= CostFlagTrace
	}
	return op
}

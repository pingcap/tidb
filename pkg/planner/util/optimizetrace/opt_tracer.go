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

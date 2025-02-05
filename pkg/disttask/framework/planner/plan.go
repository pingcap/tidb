// Copyright 2023 PingCAP, Inc.
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

package planner

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// PlanCtx is the context for planning.
type PlanCtx struct {
	Ctx context.Context

	// integrate with current distribute framework
	SessionCtx sessionctx.Context
	TaskID     int64
	TaskKey    string
	TaskType   proto.TaskType
	ThreadCnt  int

	// PreviousSubtaskMetas is subtask metas of previous steps.
	// We can remove this field if we find a better way to pass the result between steps.
	PreviousSubtaskMetas map[proto.Step][][]byte
	GlobalSort           bool
	NextTaskStep         proto.Step
	ExecuteNodesCnt      int

	Store kv.StorageWithPD
}

// LogicalPlan represents a logical plan in distribute framework.
// A normal flow of distribute framework is: logical plan -> physical plan -> pipelines.
// To integrate with current distribute framework, the flow becomes:
// logical plan -> task meta -> physical plan -> subtaskmetas -> pipelines.
type LogicalPlan interface {
	ToTaskMeta() ([]byte, error)
	FromTaskMeta([]byte) error
	ToPhysicalPlan(PlanCtx) (*PhysicalPlan, error)
}

// PhysicalPlan is a DAG of processors in distribute framework.
// Each processor is a node process the task with a pipeline,
// and receive/pass the result to other processors via input and output links.
type PhysicalPlan struct {
	Processors []ProcessorSpec
}

// AddProcessor adds a node to the DAG.
func (p *PhysicalPlan) AddProcessor(processor ProcessorSpec) {
	p.Processors = append(p.Processors, processor)
}

// ToSubtaskMetas converts the physical plan to a list of subtask metas.
func (p *PhysicalPlan) ToSubtaskMetas(ctx PlanCtx, step proto.Step) ([][]byte, error) {
	subtaskMetas := make([][]byte, 0, len(p.Processors))
	for _, processor := range p.Processors {
		if processor.Step != step {
			continue
		}
		subtaskMeta, err := processor.Pipeline.ToSubtaskMeta(ctx)
		if err != nil {
			return nil, err
		}
		subtaskMetas = append(subtaskMetas, subtaskMeta)
	}
	return subtaskMetas, nil
}

// ProcessorSpec is the specification of a processor.
// A processor is a node in the DAG.
// It contains input links from other processors, as well as output links to other processors.
// It also contains an pipeline which is the actual logic of the processor.
type ProcessorSpec struct {
	ID       int
	Input    InputSpec
	Pipeline PipelineSpec
	Output   OutputSpec
	// We can remove this field if we find a better way to pass the result between steps.
	Step proto.Step
}

// InputSpec is the specification of an input.
type InputSpec struct {
	ColumnTypes []byte
	Links       []LinkSpec
}

// OutputSpec is the specification of an output.
type OutputSpec struct {
	Links []LinkSpec
}

// LinkSpec is the specification of a link.
// Link connects pipelines between different nodes.
type LinkSpec struct {
	ProcessorID int
	// Support endpoint for communication between processors.
	// Endpoint string
}

// PipelineSpec is the specification of an pipeline.
type PipelineSpec interface {
	// ToSubtaskMeta converts the pipeline to a subtask meta
	ToSubtaskMeta(PlanCtx) ([]byte, error)
}

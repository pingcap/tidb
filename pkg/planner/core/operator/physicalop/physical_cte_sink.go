// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalCTESink is the physical operator for CTE sink.
type PhysicalCTESink struct {
	BasePhysicalPlan

	IDForStorage    int
	TargetTasks     []*kv.MPPTask
	Tasks           []*kv.MPPTask
	CompressionMode vardef.ExchangeCompressionMode
	// Current MPP side doesn't implement the UNION ALL executor. The Sink will be duplicated x times if it has UNION ALL inside it.
	// The DuplicatedSinkNum is used to indicate how many times the CTE sink has been copied.
	DuplicatedSinkNum   uint32
	DuplicatedSourceNum uint32
}

// Init only assigns type and context.
func (p PhysicalCTESink) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTESink {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypePhysicalCTESink, &p, 0)
	p.SetStats(stats)
	return &p
}

// GetCompressionMode returns the compression mode of this operator.
func (p *PhysicalCTESink) GetCompressionMode() vardef.ExchangeCompressionMode {
	return p.CompressionMode
}

// GetSelfTasks returns the tasks held by this operator.
func (p *PhysicalCTESink) GetSelfTasks() []*kv.MPPTask {
	return p.Tasks
}

// SetSelfTasks sets the tasks held by this operator.
func (p *PhysicalCTESink) SetSelfTasks(tasks []*kv.MPPTask) {
	p.Tasks = tasks
}

// SetTargetTasks sets the target tasks of this operator.
func (p *PhysicalCTESink) SetTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = tasks
}

// AppendTargetTasks appends target tasks to this operator.
func (p *PhysicalCTESink) AppendTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = append(p.TargetTasks, tasks...)
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTESink) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalCTESink)
	np.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.BasePhysicalPlan = *base
	np.CompressionMode = p.CompressionMode
	np.IDForStorage = p.IDForStorage
	return np, nil
}

// ToPB implements the base.PhysicalPlan.<3rd> interface.
func (p *PhysicalCTESink) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	childPb, err := p.Children()[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, err
	}
	fieldTypes := make([]*tipb.FieldType, 0, len(p.Schema().Columns))
	for _, column := range p.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, kv.TiFlash)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldTypes = append(fieldTypes, pbType)
	}
	cteSink := &tipb.CTESink{
		CteId:        uint32(p.IDForStorage),
		Child:        childPb,
		FieldTypes:   fieldTypes,
		CteSinkNum:   p.DuplicatedSinkNum,
		CteSourceNum: p.DuplicatedSourceNum,
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:         tipb.ExecType_TypeCTESink,
		CteSink:    cteSink,
		ExecutorId: &executorID,
	}, nil
}

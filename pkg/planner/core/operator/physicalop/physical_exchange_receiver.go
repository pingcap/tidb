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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalExchangeReceiver accepts connection and receives data passively.
type PhysicalExchangeReceiver struct {
	BasePhysicalPlan

	Tasks []*kv.MPPTask
	frags []*Fragment
}

// Clone implment op.PhysicalPlan interface.
func (p *PhysicalExchangeReceiver) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalExchangeReceiver)
	np.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.BasePhysicalPlan = *base
	return np, nil
}

// MemoryUsage return the memory usage of PhysicalExchangeReceiver
func (p *PhysicalExchangeReceiver) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice*2 + int64(cap(p.Tasks)+cap(p.frags))*size.SizeOfPointer
	for _, frag := range p.frags {
		sum += frag.MemoryUsage()
	}
	return
}

// Init only assigns type and context.
func (p PhysicalExchangeReceiver) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalExchangeReceiver {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeExchangeReceiver, 0)
	p.SetStats(stats)
	return &p
}

// GetExchangeSender return the connected sender of this receiver. We assume that its child must be a receiver.
func (p *PhysicalExchangeReceiver) GetExchangeSender() *PhysicalExchangeSender {
	return p.Children()[0].(*PhysicalExchangeSender)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExchangeReceiver) ExplainInfo() (res string) {
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		res = fmt.Sprintf("stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return res
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalExchangeReceiver) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer1PhysicalExchangeReceiver(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan.
func (p *PhysicalExchangeReceiver) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer2PhysicalExchangeReceiver(p, taskType, option, args...)
}

// ToPB generates the pb structure.
func (p *PhysicalExchangeReceiver) ToPB(ctx *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	encodedTask := make([][]byte, 0, len(p.Tasks))

	for _, task := range p.Tasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	fieldTypes := make([]*tipb.FieldType, 0, len(p.Schema().Columns))
	for _, column := range p.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, kv.TiFlash)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldTypes = append(fieldTypes, pbType)
	}
	ecExec := &tipb.ExchangeReceiver{
		EncodedTaskMeta: encodedTask,
		FieldTypes:      fieldTypes,
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeReceiver,
		ExchangeReceiver:              ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

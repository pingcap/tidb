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
	"bytes"
	"fmt"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalExchangeSender dispatches data to upstream tasks. That means push mode processing.
type PhysicalExchangeSender struct {
	BasePhysicalPlan
	// TargetTasks are the tasks that this fragment will send data to.
	// Tasks held by itself will send data to the TargetTasks.
	TargetTasks          []*kv.MPPTask
	TargetCTEReaderTasks [][]*kv.MPPTask
	ExchangeType         tipb.ExchangeType
	HashCols             []*property.MPPPartitionColumn
	// Tasks record actual tasks that this fragment has.
	// It will tell which nodes this fragment is running on.
	Tasks           []*kv.MPPTask
	CompressionMode vardef.ExchangeCompressionMode
}

// Init only assigns type and context.
func (p PhysicalExchangeSender) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalExchangeSender {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeExchangeSender, 0)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalExchangeSender) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalExchangeSender)
	np.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, perrors.Trace(err)
	}
	np.BasePhysicalPlan = *base
	np.ExchangeType = p.ExchangeType
	np.HashCols = p.HashCols
	np.CompressionMode = p.CompressionMode
	return np, nil
}

// MemoryUsage return the memory usage of PhysicalExchangeSender
func (p *PhysicalExchangeSender) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice*3 + size.SizeOfInt32 +
		int64(cap(p.TargetTasks)+cap(p.HashCols)+cap(p.Tasks))*size.SizeOfPointer
	for _, hCol := range p.HashCols {
		sum += hCol.MemoryUsage()
	}
	return
}

// GetCompressionMode returns the compression mode of this exchange sender.
func (p *PhysicalExchangeSender) GetCompressionMode() vardef.ExchangeCompressionMode {
	return p.CompressionMode
}

// GetSelfTasks returns mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) GetSelfTasks() []*kv.MPPTask {
	return p.Tasks
}

// SetSelfTasks sets mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) SetSelfTasks(tasks []*kv.MPPTask) {
	p.Tasks = tasks
}

// SetTargetTasks sets mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) SetTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = tasks
}

// AppendTargetTasks appends mpp tasks for current PhysicalExchangeSender.
func (p *PhysicalExchangeSender) AppendTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = append(p.TargetTasks, tasks...)
}

// ExplainInfo implements Plan interface.
func (p *PhysicalExchangeSender) ExplainInfo() string {
	buffer := bytes.NewBufferString("ExchangeType: ")
	switch p.ExchangeType {
	case tipb.ExchangeType_PassThrough:
		fmt.Fprintf(buffer, "PassThrough")
	case tipb.ExchangeType_Broadcast:
		fmt.Fprintf(buffer, "Broadcast")
	case tipb.ExchangeType_Hash:
		fmt.Fprintf(buffer, "HashPartition")
	}
	if p.CompressionMode != vardef.ExchangeCompressionModeNONE {
		fmt.Fprintf(buffer, ", Compression: %s", p.CompressionMode.Name())
	}
	if p.ExchangeType == tipb.ExchangeType_Hash {
		fmt.Fprintf(buffer, ", Hash Cols: %s", property.ExplainColumnList(p.SCtx().GetExprCtx().GetEvalCtx(), p.HashCols))
	}
	if len(p.Tasks) > 0 {
		fmt.Fprintf(buffer, ", tasks: [")
		for idx, task := range p.Tasks {
			if idx != 0 {
				fmt.Fprintf(buffer, ", ")
			}
			fmt.Fprintf(buffer, "%v", task.ID)
		}
		fmt.Fprintf(buffer, "]")
	}
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		fmt.Fprintf(buffer, ", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return buffer.String()
}

// ResolveIndicesItself resolve indices for PhysicalPlan itself
func (p *PhysicalExchangeSender) ResolveIndicesItself() (err error) {
	return p.ResolveIndicesItselfWithSchema(p.Children()[0].Schema())
}

// ResolveIndicesItselfWithSchema is added for test usage
func (p *PhysicalExchangeSender) ResolveIndicesItselfWithSchema(inputSchema *expression.Schema) (err error) {
	for i, hashCol := range p.HashCols {
		newHashCol, err := hashCol.ResolveIndices(inputSchema)
		if err != nil {
			return err
		}
		p.HashCols[i] = newHashCol
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalExchangeSender) ResolveIndices() (err error) {
	err = p.BasePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	return p.ResolveIndicesItself()
}

// ToPB generates the pb structure.
func (p *PhysicalExchangeSender) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	child, err := p.Children()[0].ToPB(ctx, kv.TiFlash)
	if err != nil {
		return nil, perrors.Trace(err)
	}

	encodedTask := make([][]byte, 0, len(p.TargetTasks))

	for _, task := range p.TargetTasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, perrors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	encodedUpstreamCTETask := make([]*tipb.EncodedBytesSlice, 0, len(p.TargetCTEReaderTasks))
	for _, cteRTasks := range p.TargetCTEReaderTasks {
		encodedTasksForOneCTEReader := &tipb.EncodedBytesSlice{
			EncodedTasks: make([][]byte, 0, len(cteRTasks)),
		}
		for _, task := range cteRTasks {
			encodedStr, err := task.ToPB().Marshal()
			if err != nil {
				return nil, err
			}
			encodedTasksForOneCTEReader.EncodedTasks = append(encodedTasksForOneCTEReader.EncodedTasks, encodedStr)
		}
		encodedUpstreamCTETask = append(encodedUpstreamCTETask, encodedTasksForOneCTEReader)
	}

	hashCols := make([]expression.Expression, 0, len(p.HashCols))
	hashColTypes := make([]*tipb.FieldType, 0, len(p.HashCols))
	for _, col := range p.HashCols {
		hashCols = append(hashCols, col.Col)
		tp, err := expression.ToPBFieldTypeWithCheck(col.Col.RetType, storeType)
		if err != nil {
			return nil, perrors.Trace(err)
		}
		tp.Collate = col.CollateID
		hashColTypes = append(hashColTypes, tp)
	}
	allFieldTypes := make([]*tipb.FieldType, 0, len(p.Schema().Columns))
	for _, column := range p.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, storeType)
		if err != nil {
			return nil, perrors.Trace(err)
		}
		allFieldTypes = append(allFieldTypes, pbType)
	}
	hashColPb, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), hashCols, ctx.GetClient())
	if err != nil {
		return nil, perrors.Trace(err)
	}
	ecExec := &tipb.ExchangeSender{
		Tp:                  p.ExchangeType,
		EncodedTaskMeta:     encodedTask,
		PartitionKeys:       hashColPb,
		Child:               child,
		Types:               hashColTypes,
		AllFieldTypes:       allFieldTypes,
		Compression:         p.CompressionMode.ToTipbCompressionMode(),
		UpstreamCteTaskMeta: encodedUpstreamCTETask,
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeExchangeSender,
		ExchangeSender:                ecExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

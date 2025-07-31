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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalWindow is the physical operator of window function.
type PhysicalWindow struct {
	PhysicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.SortItem
	OrderBy         []property.SortItem
	Frame           *logicalop.WindowFrame

	// on which store the window function executes.
	StoreTp kv.StoreType
}

// Init initializes PhysicalWindow.
func (p PhysicalWindow) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalWindow {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeWindow, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalWindow) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corCols = append(corCols, expression.ExtractCorColumns(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			}
		}
	}
	return corCols
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalWindow) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalWindow)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.PartitionBy = make([]property.SortItem, 0, len(p.PartitionBy))
	for _, it := range p.PartitionBy {
		cloned.PartitionBy = append(cloned.PartitionBy, it.Clone())
	}
	cloned.OrderBy = make([]property.SortItem, 0, len(p.OrderBy))
	for _, it := range p.OrderBy {
		cloned.OrderBy = append(cloned.OrderBy, it.Clone())
	}
	cloned.WindowFuncDescs = make([]*aggregation.WindowFuncDesc, 0, len(p.WindowFuncDescs))
	for _, it := range p.WindowFuncDescs {
		cloned.WindowFuncDescs = append(cloned.WindowFuncDescs, it.Clone())
	}
	if p.Frame != nil {
		cloned.Frame = p.Frame.Clone()
	}

	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalWindow
func (p *PhysicalWindow) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfSlice*3 + int64(cap(p.WindowFuncDescs))*size.SizeOfPointer +
		size.SizeOfUint8

	for _, windowFunc := range p.WindowFuncDescs {
		sum += windowFunc.MemoryUsage()
	}
	for _, item := range p.PartitionBy {
		sum += item.MemoryUsage()
	}
	for _, item := range p.OrderBy {
		sum += item.MemoryUsage()
	}
	return
}

func (p *PhysicalWindow) formatFrameBound(buffer *bytes.Buffer, bound *logicalop.FrameBound) {
	if bound.Type == ast.CurrentRow {
		buffer.WriteString("current row")
		return
	}
	if bound.UnBounded {
		buffer.WriteString("unbounded")
	} else if len(bound.CalcFuncs) > 0 {
		evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
		sf := bound.CalcFuncs[0].(*expression.ScalarFunction)
		switch sf.FuncName.L {
		case ast.DateAdd, ast.DateSub:
			// For `interval '2:30' minute_second`.
			fmt.Fprintf(buffer, "interval %s %s", sf.GetArgs()[1].ExplainInfo(evalCtx), sf.GetArgs()[2].ExplainInfo(evalCtx))
		case ast.Plus, ast.Minus:
			// For `1 preceding` of range frame.
			fmt.Fprintf(buffer, "%s", sf.GetArgs()[1].ExplainInfo(evalCtx))
		}
	} else {
		switch p.SCtx().GetSessionVars().EnableRedactLog {
		case perrors.RedactLogDisable:
			fmt.Fprintf(buffer, "%d", bound.Num)
		case perrors.RedactLogMarker:
			fmt.Fprintf(buffer, "‹%d›", bound.Num)
		case perrors.RedactLogEnable:
			fmt.Fprintf(buffer, "?")
		}
	}
	if bound.Type == ast.Preceding {
		buffer.WriteString(" preceding")
	} else {
		buffer.WriteString(" following")
	}
}

// ExplainInfo implements Plan interface.
func (p *PhysicalWindow) ExplainInfo() string {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString("")
	FormatWindowFuncDescs(ectx, buffer, p.WindowFuncDescs, p.Schema())
	buffer.WriteString(" over(")
	isFirst := true
	buffer = util.ExplainPartitionBy(ectx, buffer, p.PartitionBy, false)
	if len(p.PartitionBy) > 0 {
		isFirst = false
	}
	if len(p.OrderBy) > 0 {
		if !isFirst {
			buffer.WriteString(" ")
		}
		buffer.WriteString("order by ")
		evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
		for i, item := range p.OrderBy {
			if item.Desc {
				fmt.Fprintf(buffer, "%s desc", item.Col.ExplainInfo(evalCtx))
			} else {
				fmt.Fprintf(buffer, "%s", item.Col.ExplainInfo(evalCtx))
			}

			if i+1 < len(p.OrderBy) {
				buffer.WriteString(", ")
			}
		}
		isFirst = false
	}
	if p.Frame != nil {
		if !isFirst {
			buffer.WriteString(" ")
		}
		if p.Frame.Type == ast.Rows {
			buffer.WriteString("rows")
		} else {
			buffer.WriteString("range")
		}
		buffer.WriteString(" between ")
		p.formatFrameBound(buffer, p.Frame.Start)
		buffer.WriteString(" and ")
		p.formatFrameBound(buffer, p.Frame.End)
	}
	buffer.WriteString(")")
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		fmt.Fprintf(buffer, ", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return buffer.String()
}

// FormatWindowFuncDescs formats window function descriptions.
func FormatWindowFuncDescs(ctx expression.EvalContext, buffer *bytes.Buffer, descs []*aggregation.WindowFuncDesc, schema *expression.Schema) *bytes.Buffer {
	winFuncStartIdx := len(schema.Columns) - len(descs)
	for i, desc := range descs {
		if i != 0 {
			buffer.WriteString(", ")
		}
		fmt.Fprintf(buffer, "%v->%v", desc.StringWithCtx(ctx, perrors.RedactLogDisable), schema.Columns[winFuncStartIdx+i])
	}
	return buffer
}

// ResolveIndices implements Plan interface.
func (p *PhysicalWindow) ResolveIndices() (err error) {
	err = p.PhysicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for i := range len(p.Schema().Columns) - len(p.WindowFuncDescs) {
		col := p.Schema().Columns[i]
		newCol, err := col.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
		p.Schema().Columns[i] = newCol.(*expression.Column)
	}
	for i, item := range p.PartitionBy {
		newCol, err := item.Col.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
		p.PartitionBy[i].Col = newCol.(*expression.Column)
	}
	for i, item := range p.OrderBy {
		newCol, err := item.Col.ResolveIndices(p.Children()[0].Schema())
		if err != nil {
			return err
		}
		p.OrderBy[i].Col = newCol.(*expression.Column)
	}
	for _, desc := range p.WindowFuncDescs {
		for i, arg := range desc.Args {
			desc.Args[i], err = arg.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	if p.Frame != nil {
		for i := range p.Frame.Start.CalcFuncs {
			p.Frame.Start.CalcFuncs[i], err = p.Frame.Start.CalcFuncs[i].ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
		for i := range p.Frame.End.CalcFuncs {
			p.Frame.End.CalcFuncs[i], err = p.Frame.End.CalcFuncs[i].ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalWindow) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalWindow(p, tasks...)
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalWindow) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()

	windowExec := &tipb.Window{}

	windowExec.FuncDesc = make([]*tipb.Expr, 0, len(p.WindowFuncDescs))
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	for _, desc := range p.WindowFuncDescs {
		windowExec.FuncDesc = append(windowExec.FuncDesc, aggregation.WindowFuncToPBExpr(evalCtx, client, desc))
	}
	for _, item := range p.PartitionBy {
		windowExec.PartitionBy = append(windowExec.PartitionBy, expression.SortByItemToPB(evalCtx, client, item.Col.Clone(), item.Desc))
	}
	for _, item := range p.OrderBy {
		windowExec.OrderBy = append(windowExec.OrderBy, expression.SortByItemToPB(evalCtx, client, item.Col.Clone(), item.Desc))
	}

	if p.Frame != nil {
		windowExec.Frame = &tipb.WindowFrame{
			Type: tipb.WindowFrameType(p.Frame.Type),
		}
		if p.Frame.Start != nil {
			start, err := p.Frame.Start.ToPB(ctx)
			if err != nil {
				return nil, err
			}
			windowExec.Frame.Start = start
		}
		if p.Frame.End != nil {
			end, err := p.Frame.End.ToPB(ctx)
			if err != nil {
				return nil, err
			}
			windowExec.Frame.End = end
		}
	}

	var err error
	windowExec.Child, err = p.Children()[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, perrors.Trace(err)
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeWindow,
		Window:                        windowExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

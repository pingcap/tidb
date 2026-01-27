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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalCTE is for CTE.
type PhysicalCTE struct {
	PhysicalSchemaProducer

	SeedPlan  base.PhysicalPlan
	RecurPlan base.PhysicalPlan
	CTE       *logicalop.CTEClass
	CteAsName ast.CIStr
	CteName   ast.CIStr
}

// ExhaustPhysicalPlans4LogicalCTE will be called by LogicalCTE in logicalOp pkg.
func ExhaustPhysicalPlans4LogicalCTE(p *logicalop.LogicalCTE, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	pcte := PhysicalCTE{CTE: p.Cte}.Init(p.SCtx(), p.StatsInfo())
	pcte.SetSchema(p.Schema())
	pcte.SetChildrenReqProps([]*property.PhysicalProperty{prop.CloneEssentialFields()})
	return []base.PhysicalPlan{(*PhysicalCTEStorage)(pcte)}, true, nil
}

// Init only assigns type and context.
func (p PhysicalCTE) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTE {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeCTE, &p, 0)
	p.SetStats(stats)
	return &p
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := coreusage.ExtractCorrelatedCols4PhysicalPlan(p.SeedPlan)
	if p.RecurPlan != nil {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(p.RecurPlan)...)
	}
	return corCols
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalCTE) OperatorInfo(_ bool) string {
	return fmt.Sprintf("data:%s", (*CTEDefinition)(p).ExplainID())
}

// ExplainInfo implements Plan interface.
func (p *PhysicalCTE) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainID overrides the ExplainID.
func (p *PhysicalCTE) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.StringerFunc(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.ID())
	})
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTE) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalCTE)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	if p.SeedPlan != nil {
		cloned.SeedPlan, err = p.SeedPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	if p.RecurPlan != nil {
		cloned.RecurPlan, err = p.RecurPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	cloned.CteAsName, cloned.CteName = p.CteAsName, p.CteName
	cloned.CTE = p.CTE
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalCTE
func (p *PhysicalCTE) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.CteAsName.MemoryUsage()
	if p.SeedPlan != nil {
		sum += p.SeedPlan.MemoryUsage()
	}
	if p.RecurPlan != nil {
		sum += p.RecurPlan.MemoryUsage()
	}
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// GetPlanCostVer2 implements PhysicalPlan interface.
func (p *PhysicalCTE) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalCTE(p, taskType, option)
}

// AccessObject implements DataAccesser interface.
func (p *PhysicalCTE) AccessObject() base.AccessObject {
	if p.CteName == p.CteAsName {
		return access.OtherAccessObject(fmt.Sprintf("CTE:%s", p.CteName.L))
	}
	return access.OtherAccessObject(fmt.Sprintf("CTE:%s AS %s", p.CteName.L, p.CteAsName.L))
}

// CTEDefinition is CTE definition for explain.
type CTEDefinition PhysicalCTE

// ExplainInfo overrides the ExplainInfo
func (p *CTEDefinition) ExplainInfo() string {
	var res string
	if p.RecurPlan != nil {
		res = "Recursive CTE"
	} else {
		res = "Non-Recursive CTE"
	}
	if p.CTE.HasLimit {
		offset, count := p.CTE.LimitBeg, p.CTE.LimitEnd-p.CTE.LimitBeg
		switch p.SCtx().GetSessionVars().EnableRedactLog {
		case errors.RedactLogMarker:
			res += fmt.Sprintf(", limit(offset:‹%v›, count:‹%v›)", offset, count)
		case errors.RedactLogDisable:
			res += fmt.Sprintf(", limit(offset:%v, count:%v)", offset, count)
		case errors.RedactLogEnable:
			res += ", limit(offset:?, count:?)"
		}
	}
	return res
}

// ExplainID overrides the ExplainID.
func (p *CTEDefinition) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.StringerFunc(func() string {
		return "CTE_" + strconv.Itoa(p.CTE.IDForStorage)
	})
}

// MemoryUsage return the memory usage of CTEDefinition
func (p *CTEDefinition) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.CteAsName.MemoryUsage()
	if p.SeedPlan != nil {
		sum += p.SeedPlan.MemoryUsage()
	}
	if p.RecurPlan != nil {
		sum += p.RecurPlan.MemoryUsage()
	}
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// PhysicalCTEStorage is used for representing CTE storage, or CTE producer in other words.
type PhysicalCTEStorage PhysicalCTE

// ExplainInfo overrides the ExplainInfo
func (*PhysicalCTEStorage) ExplainInfo() string {
	return "Non-Recursive CTE Storage"
}

// ExplainID overrides the ExplainID.
func (p *PhysicalCTEStorage) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.StringerFunc(func() string {
		return "CTE_" + strconv.Itoa(p.CTE.IDForStorage)
	})
}

// MemoryUsage return the memory usage of CTEDefinition
func (p *PhysicalCTEStorage) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.CteAsName.MemoryUsage()
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTEStorage) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned, err := (*PhysicalCTE)(p).Clone(newCtx)
	if err != nil {
		return nil, err
	}
	return (*PhysicalCTEStorage)(cloned.(*PhysicalCTE)), nil
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalCTEStorage) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalCTEStorage(p, tasks...)
}

func findBestTask4LogicalCTE(super base.LogicalPlan, prop *property.PhysicalProperty) (t base.Task, err error) {
	if prop.IndexJoinProp != nil {
		// even enforce hint can not work with this.
		return base.InvalidTask, nil
	}
	var childLen int
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalCTE](super)
	if ge != nil {
		childLen = ge.InputsLen()
	} else {
		childLen = p.ChildLen()
	}
	if childLen > 0 {
		// pass the super here to iterate among ge or logical plan both.
		return utilfuncp.FindBestTask4BaseLogicalPlan(super, prop)
	}
	if !checkOpSelfSatisfyPropTaskTypeRequirement(p, prop) {
		// Currently all plan cannot totally push down to TiKV.
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !prop.CanAddEnforcer {
		return base.InvalidTask, nil
	}
	// The physical plan has been build when derive stats.
	pcte := PhysicalCTE{SeedPlan: p.Cte.SeedPartPhysicalPlan, RecurPlan: p.Cte.RecursivePartPhysicalPlan, CTE: p.Cte, CteAsName: p.CteAsName, CteName: p.CteName}.Init(p.SCtx(), p.StatsInfo())
	pcte.SetSchema(p.Schema())
	if prop.IsFlashProp() && prop.CTEProducerStatus == property.AllCTECanMpp {
		if prop.MPPPartitionTp != property.AnyType {
			return base.InvalidTask, nil
		}
		t = NewMppTask(pcte, prop.MPPPartitionTp, prop.MPPPartitionCols, p.StatsInfo().HistColl, nil)
	} else {
		rt := &RootTask{}
		rt.SetPlan(pcte)
		t = rt
	}
	if prop.CanAddEnforcer {
		t = EnforceProperty(prop, t, p.Plan.SCtx(), nil)
	}
	return t, nil
}

func checkOpSelfSatisfyPropTaskTypeRequirement(p base.LogicalPlan, prop *property.PhysicalProperty) bool {
	switch prop.TaskTp {
	case property.MppTaskType:
		// when parent operator ask current op to be mppTaskType, check operator itself here.
		return logicalop.CanSelfBeingPushedToCopImpl(p, kv.TiFlash)
	case property.CopSingleReadTaskType, property.CopMultiReadTaskType:
		return logicalop.CanSelfBeingPushedToCopImpl(p, kv.TiKV)
	default:
		return true
	}
}

// PhysicalCTESink is used for representing CTE sink in TiFlash MPP.
// It corresponds to tipb.CTESink.
type PhysicalCTESink struct {
	BasePhysicalPlan

	IDForStorage int

	// TargetTasks are kept for base.MPPSink interface compatibility.
	TargetTasks []*kv.MPPTask
	Tasks       []*kv.MPPTask

	CompressionMode vardef.ExchangeCompressionMode

	CteSinkNum   uint32
	CteSourceNum uint32
}

// Init only assigns type and context.
func (p PhysicalCTESink) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTESink {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypePhysicalCTESink, &p, 0)
	p.SetStats(stats)
	return &p
}

// GetCompressionMode returns the compression mode.
func (p *PhysicalCTESink) GetCompressionMode() vardef.ExchangeCompressionMode {
	return p.CompressionMode
}

// GetSelfTasks returns mpp tasks for current PhysicalCTESink.
func (p *PhysicalCTESink) GetSelfTasks() []*kv.MPPTask {
	return p.Tasks
}

// SetSelfTasks sets mpp tasks for current PhysicalCTESink.
func (p *PhysicalCTESink) SetSelfTasks(tasks []*kv.MPPTask) {
	p.Tasks = tasks
}

// SetTargetTasks sets mpp tasks for current PhysicalCTESink.
func (p *PhysicalCTESink) SetTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = tasks
}

// AppendTargetTasks appends mpp tasks for current PhysicalCTESink.
func (p *PhysicalCTESink) AppendTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = append(p.TargetTasks, tasks...)
}

// Clone implements base.PhysicalPlan interface.
func (p *PhysicalCTESink) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalCTESink)
	np.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.BasePhysicalPlan = *base
	np.IDForStorage = p.IDForStorage
	np.CompressionMode = p.CompressionMode
	np.CteSinkNum = p.CteSinkNum
	np.CteSourceNum = p.CteSourceNum
	return np, nil
}

// MemoryUsage returns the memory usage of PhysicalCTESink.
func (p *PhysicalCTESink) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}
	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfInt + size.SizeOfInt32*2 +
		size.SizeOfSlice*2 + int64(cap(p.TargetTasks)+cap(p.Tasks))*size.SizeOfPointer
	return
}

// ToPB generates the pb structure.
func (p *PhysicalCTESink) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	childPb, err := p.Children()[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	fieldTypes := make([]*tipb.FieldType, 0, len(p.Schema().Columns))
	for _, column := range p.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldTypes = append(fieldTypes, pbType)
	}

	cteSink := &tipb.CTESink{
		CteId:        uint32(p.IDForStorage),
		CteSourceNum: p.CteSourceNum,
		CteSinkNum:   p.CteSinkNum,
		Child:        childPb,
		FieldTypes:   fieldTypes,
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:         tipb.ExecType_TypeCTESink,
		CteSink:    cteSink,
		ExecutorId: &executorID,
	}, nil
}

// PhysicalCTESource is used for representing CTE source in TiFlash MPP.
// It corresponds to tipb.CTESource.
type PhysicalCTESource struct {
	PhysicalSchemaProducer

	IDForStorage int

	CteSinkNum   uint32
	CteSourceNum uint32
}

// Init only assigns type, context and schema.
func (p PhysicalCTESource) Init(ctx base.PlanContext, stats *property.StatsInfo, schema *expression.Schema) *PhysicalCTESource {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypePhysicalCTESource, &p, 0)
	p.SetStats(stats)
	p.SetSchema(schema)
	return &p
}

// Clone implements base.PhysicalPlan interface.
func (p *PhysicalCTESource) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalCTESource)
	np.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.PhysicalSchemaProducer = *base
	np.IDForStorage = p.IDForStorage
	np.CteSinkNum = p.CteSinkNum
	np.CteSourceNum = p.CteSourceNum
	return np, nil
}

// MemoryUsage returns the memory usage of PhysicalCTESource.
func (p *PhysicalCTESource) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}
	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt + size.SizeOfInt32*2
	return
}

// ToPB generates the pb structure.
func (p *PhysicalCTESource) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	fieldTypes := make([]*tipb.FieldType, 0, len(p.Schema().Columns))
	for _, column := range p.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldTypes = append(fieldTypes, pbType)
	}
	cteSource := &tipb.CTESource{
		CteId:        uint32(p.IDForStorage),
		CteSourceNum: p.CteSourceNum,
		CteSinkNum:   p.CteSinkNum,
		FieldTypes:   fieldTypes,
	}
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:         tipb.ExecType_TypeCTESource,
		CteSource:  cteSource,
		ExecutorId: &executorID,
	}, nil
}

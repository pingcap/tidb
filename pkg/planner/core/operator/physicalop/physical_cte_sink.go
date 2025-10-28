package physicalop

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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

func (p *PhysicalCTESink) GetCompressionMode() vardef.ExchangeCompressionMode {
	return p.CompressionMode
}

func (p *PhysicalCTESink) GetSelfTasks() []*kv.MPPTask {
	return p.Tasks
}

func (p *PhysicalCTESink) SetSelfTasks(tasks []*kv.MPPTask) {
	p.Tasks = tasks
}

func (p *PhysicalCTESink) SetTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = tasks
}

func (p *PhysicalCTESink) AppendTargetTasks(tasks []*kv.MPPTask) {
	p.TargetTasks = append(p.TargetTasks, tasks...)
}

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

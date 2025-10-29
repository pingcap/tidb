package physicalop

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalCTESource is the physical operator for CTE source.
type PhysicalCTESource struct {
	PhysicalSchemaProducer

	IDForStorage int
	Tasks        []*kv.MPPTask
	frags        []*Fragment

	// Current MPP side doesn't implement the UNION ALL executor. The Sink will be duplicated x times if it has UNION ALL inside it.
	// The DuplicatedSinkNum is used to indicate how many times the CTE sink has been copied.
	DuplicatedSinkNum   uint32
	DuplicatedSourceNum uint32
}

func (p PhysicalCTESource) Init(ctx base.PlanContext, stats *property.StatsInfo, schema *expression.Schema) *PhysicalCTESource {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypePhysicalCTESource, &p, 0)
	p.SetStats(stats)
	p.SetSchema(schema)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTESource) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	np := new(PhysicalCTESource)
	np.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, np)
	if err != nil {
		return nil, errors.Trace(err)
	}
	np.PhysicalSchemaProducer = *base
	np.IDForStorage = p.IDForStorage
	return np, nil
}

func (p *PhysicalCTESource) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	cteSource := &tipb.CTESource{
		CteId:        uint32(p.IDForStorage),
		CteSinkNum:   p.DuplicatedSinkNum,
		CteSourceNum: p.DuplicatedSourceNum,
	}
	fieldTypes := make([]*tipb.FieldType, 0, len(p.Schema().Columns))
	for _, column := range p.Schema().Columns {
		pbType, err := expression.ToPBFieldTypeWithCheck(column.RetType, kv.TiFlash)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fieldTypes = append(fieldTypes, pbType)
	}
	cteSource.FieldTypes = fieldTypes
	executorID := p.ExplainID().String()
	return &tipb.Executor{
		Tp:         tipb.ExecType_TypeCTESource,
		CteSource:  cteSource,
		ExecutorId: &executorID,
	}, nil
}

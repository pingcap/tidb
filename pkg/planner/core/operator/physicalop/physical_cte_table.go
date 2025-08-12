package physicalop

import (
	"strconv"

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalCTETable is for CTE table.
type PhysicalCTETable struct {
	PhysicalSchemaProducer

	IDForStorage int
}

// Init only assigns type and context.
func (p PhysicalCTETable) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTETable {
	p.Plan = baseimpl.NewBasePlan(ctx, plancodec.TypeCTETable, 0)
	p.SetStats(stats)
	return &p
}

// ExplainInfo overrides the ExplainInfo
func (p *PhysicalCTETable) ExplainInfo() string {
	return "Scan on CTE_" + strconv.Itoa(p.IDForStorage)
}

// MemoryUsage return the memory usage of PhysicalCTETable
func (p *PhysicalCTETable) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt
}

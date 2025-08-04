package physicalop

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PushedDownLimit is the limit operator pushed down into PhysicalIndexLookUpReader.
type PushedDownLimit struct {
	Offset uint64
	Count  uint64
}

// Clone clones this pushed-down list.
func (p *PushedDownLimit) Clone() *PushedDownLimit {
	if p == nil {
		return nil
	}
	cloned := new(PushedDownLimit)
	*cloned = *p
	return cloned
}

const pushedDownLimitSize = size.SizeOfUint64 * 2

// MemoryUsage return the memory usage of PushedDownLimit
func (p *PushedDownLimit) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return pushedDownLimitSize
}

// PhysPlanPartInfo indicates partition helper info in physical plan.
type PhysPlanPartInfo struct {
	PruningConds   []expression.Expression
	PartitionNames []ast.CIStr
	Columns        []*expression.Column
	ColumnNames    types.NameSlice
}

const emptyPartitionInfoSize = int64(unsafe.Sizeof(PhysPlanPartInfo{}))

func (pi *PhysPlanPartInfo) cloneForPlanCache() *PhysPlanPartInfo {
	if pi == nil {
		return nil
	}
	cloned := new(PhysPlanPartInfo)
	cloned.PruningConds = utilfuncp.CloneExpressionsForPlanCache(pi.PruningConds, nil)
	cloned.PartitionNames = pi.PartitionNames
	cloned.Columns = utilfuncp.CloneColumnsForPlanCache(pi.Columns, nil)
	cloned.ColumnNames = pi.ColumnNames
	return cloned
}

// MemoryUsage return the memory usage of PhysPlanPartInfo
func (pi *PhysPlanPartInfo) MemoryUsage() (sum int64) {
	if pi == nil {
		return
	}

	sum = emptyPartitionInfoSize
	for _, cond := range pi.PruningConds {
		sum += cond.MemoryUsage()
	}
	for _, cis := range pi.PartitionNames {
		sum += cis.MemoryUsage()
	}
	for _, col := range pi.Columns {
		sum += col.MemoryUsage()
	}
	for _, colName := range pi.ColumnNames {
		sum += colName.MemoryUsage()
	}
	return
}

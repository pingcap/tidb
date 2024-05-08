package tablesample

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/size"
)

// TableSampleInfo contains the information for PhysicalTableSample.
type TableSampleInfo struct {
	AstNode    *ast.TableSample
	FullSchema *expression.Schema
	Partitions []table.PartitionedTable
}

// MemoryUsage return the memory usage of TableSampleInfo
func (t *TableSampleInfo) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfPointer*2 + size.SizeOfSlice + int64(cap(t.Partitions))*size.SizeOfInterface
	if t.AstNode != nil {
		sum += int64(unsafe.Sizeof(ast.TableSample{}))
	}
	if t.FullSchema != nil {
		sum += t.FullSchema.MemoryUsage()
	}
	return
}

// NewTableSampleInfo creates a new TableSampleInfo.
func NewTableSampleInfo(node *ast.TableSample, fullSchema *expression.Schema, pt []table.PartitionedTable) *TableSampleInfo {
	if node == nil {
		return nil
	}
	return &TableSampleInfo{
		AstNode:    node,
		FullSchema: fullSchema.Clone(),
		Partitions: pt,
	}
}

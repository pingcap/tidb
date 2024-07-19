// Copyright 2024 PingCAP, Inc.
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

package tablesampler

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
func NewTableSampleInfo(node *ast.TableSample, fullSchema *expression.Schema,
	pt []table.PartitionedTable) *TableSampleInfo {
	if node == nil {
		return nil
	}
	return &TableSampleInfo{
		AstNode:    node,
		FullSchema: fullSchema.Clone(),
		Partitions: pt,
	}
}

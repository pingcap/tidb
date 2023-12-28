// Copyright 2023 PingCAP, Inc.
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

package bindinfo

import (
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// CollectTableNames gets all table names from ast.Node.
func CollectTableNames(in ast.Node) []*ast.Node {
	collector := tableNameCollectorPool.Get().(*tableNameCollector)
	defer collector.DestroyAndPutToPool()
	in.Accept(collector)
	return collector.GetResult()
}

var tableNameCollectorPool = sync.Pool{
	New: func() any {
		return newCollectTableName()
	},
}

type tableNameCollector struct {
	tableNames []*ast.Node
}

func newCollectTableName() *tableNameCollector {
	return &tableNameCollector{
		tableNames: make([]*ast.Node, 0, 4),
	}
}

// Enter implements Visitor interface.
func (c *tableNameCollector) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.ColumnName:
		if node.Table.String() != "" {
			c.tableNames = append(c.tableNames, &in)
		}
	case *ast.TableName:
		c.tableNames = append(c.tableNames, &in)
		return in, true
	}
	return in, false
}

// Leave implements Visitor interface.
func (*tableNameCollector) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (c *tableNameCollector) GetResult() []*ast.Node {
	return slices.Clone(c.tableNames)
}

func (c *tableNameCollector) DestroyAndPutToPool() {
	c.tableNames = c.tableNames[:0]
	tableNameCollectorPool.Put(c)
}

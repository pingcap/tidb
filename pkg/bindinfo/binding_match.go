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
	"sync"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// CollectTableNames gets all table names from ast.Node.
// This function is mainly for binding fuzzy matching.
// ** the return is read-only.
// For example:
//
//	`select * from t1 where a < 1` --> [t1]
//	`select * from db1.t1, t2 where a < 1` --> [db1.t1, t2]
//
// You can see more example at the TestExtractTableName.
func CollectTableNames(in ast.Node) []*ast.TableName {
	collector := tableNameCollectorPool.Get().(*tableNameCollector)
	collector.reset()
	in.Accept(collector)
	return collector.GetResult()
}

var tableNameCollectorPool = sync.Pool{
	New: func() any {
		return newCollectTableName()
	},
}

type tableNameCollector struct {
	tableNames []*ast.TableName
}

func newCollectTableName() *tableNameCollector {
	return &tableNameCollector{
		tableNames: make([]*ast.TableName, 0, 4),
	}
}

// Enter implements Visitor interface.
func (c *tableNameCollector) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if node, ok := in.(*ast.TableName); ok {
		c.tableNames = append(c.tableNames, node)
		return in, true
	}
	return in, false
}

// Leave implements Visitor interface.
func (*tableNameCollector) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (c *tableNameCollector) GetResult() []*ast.TableName {
	return c.tableNames
}

func (c *tableNameCollector) reset() {
	c.tableNames = nil
	tableNameCollectorPool.Put(c)
}

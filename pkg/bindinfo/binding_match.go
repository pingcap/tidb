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

// ExtractTableName gets all table names from ast.Node.
func ExtractTableName(in ast.Node) []*ast.Node {
	collector := collectTableNamePool.Get().(*collectTableName)
	defer collector.DestroyAndPutToPool()
	in.Accept(collector)
	return collector.GetResult()
}

var collectTableNamePool = sync.Pool{
	New: func() any {
		return newCollectTableName()
	},
}

type collectTableName struct {
	tableNames []*ast.Node
}

func newCollectTableName() *collectTableName {
	return &collectTableName{
		tableNames: make([]*ast.Node, 0, 4),
	}
}

// Enter implements Visitor interface.
func (c *collectTableName) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.TableName, *ast.ColumnName:
		c.tableNames = append(c.tableNames, &node)
	}
	return in, false
}

// Leave implements Visitor interface.
func (*collectTableName) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (c *collectTableName) GetResult() []*ast.Node {
	return slices.Clone(c.tableNames)
}

func (c *collectTableName) DestroyAndPutToPool() {
	clear(c.tableNames)
	collectTableNamePool.Put(c)
}

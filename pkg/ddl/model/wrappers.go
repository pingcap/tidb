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

package model

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
)

// TableNameW is a wrapper of ast.TableName.
type TableNameW struct {
	*ast.TableName
	DBInfo    *model.DBInfo
	TableInfo *model.TableInfo
}

// NodeW is a wrapper of ast.Node.
type NodeW struct {
	Node       ast.Node
	resolveCtx *ResolveContext
}

// NewNodeW creates a NodeW.
func NewNodeW(node ast.Node) *NodeW {
	return &NodeW{
		Node:       node,
		resolveCtx: NewResolveContext(),
	}
}

// NewNodeWWithCtx creates a NodeW with the given ResolveContext.
func NewNodeWWithCtx(node ast.Node, resolveCtx *ResolveContext) *NodeW {
	return &NodeW{
		Node:       node,
		resolveCtx: resolveCtx,
	}
}

// CloneWithNewNode creates a new NodeW with the given ast.Node.
func (n *NodeW) CloneWithNewNode(newNode ast.Node) *NodeW {
	return &NodeW{
		Node:       newNode,
		resolveCtx: n.resolveCtx,
	}
}

// GetResolveContext returns the ResolveContext of the NodeW.
func (n *NodeW) GetResolveContext() *ResolveContext {
	return n.resolveCtx
}

// ResolveContext is used to store the context and result of resolving AST tree.
type ResolveContext struct {
	tableNames map[*ast.TableName]*TableNameW
}

// NewResolveContext creates a ResolveContext.
func NewResolveContext() *ResolveContext {
	return &ResolveContext{
		tableNames: make(map[*ast.TableName]*TableNameW),
	}
}

// Add adds the table name and its corresponding TableNameW to the ResolveContext.
func (c *ResolveContext) Add(tableName *ast.TableName, tableNameW *TableNameW) {
	c.tableNames[tableName] = tableNameW
}

// Get returns the TableNameW of the table name.
func (c *ResolveContext) Get(tableName *ast.TableName) *TableNameW {
	return c.tableNames[tableName]
}

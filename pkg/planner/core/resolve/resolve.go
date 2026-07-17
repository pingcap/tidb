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

// Package resolve is used for semantic resolve of the AST tree.
// semantic resolve is mostly done by 'core.preprocessor', in tableListExtractor
// and updatableTableListResolver we also do some resolve for aliases.
package resolve

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// AutoEmbedConsumerPresence classifies whether an AST contains a function that
// consumes auto-embedding provenance. The zero value is deliberately Unknown
// so callers that bypass or do not complete preprocessing preserve lineage.
type AutoEmbedConsumerPresence uint8

const (
	// AutoEmbedConsumerUnknown means the AST could not be classified reliably.
	AutoEmbedConsumerUnknown AutoEmbedConsumerPresence = iota
	// AutoEmbedConsumerAbsent means a complete traversal found no consumer.
	AutoEmbedConsumerAbsent
	// AutoEmbedConsumerPresent means at least one consumer was found.
	AutoEmbedConsumerPresent
)

// MergeAutoEmbedConsumerPresence conservatively combines classifications from
// static and dynamically discovered ASTs. Present dominates; Absent is kept
// only when both inputs are reliably Absent.
func MergeAutoEmbedConsumerPresence(a, b AutoEmbedConsumerPresence) AutoEmbedConsumerPresence {
	if a == AutoEmbedConsumerPresent || b == AutoEmbedConsumerPresent {
		return AutoEmbedConsumerPresent
	}
	if a == AutoEmbedConsumerAbsent && b == AutoEmbedConsumerAbsent {
		return AutoEmbedConsumerAbsent
	}
	return AutoEmbedConsumerUnknown
}

// NeedsLineage reports whether build-time provenance must be preserved.
func (p AutoEmbedConsumerPresence) NeedsLineage() bool {
	return p != AutoEmbedConsumerAbsent
}

// TableNameW is a wrapper around ast.TableName to store more information.
type TableNameW struct {
	*ast.TableName
	DBInfo    *model.DBInfo
	TableInfo *model.TableInfo
}

// NodeW is a wrapper around ast.Node to store resolve context.
type NodeW struct {
	Node                      ast.Node
	resolveCtx                *Context
	autoEmbedConsumerPresence *AutoEmbedConsumerPresence
}

// NewNodeW creates a NodeW.
func NewNodeW(node ast.Node) *NodeW {
	return &NodeW{
		Node:       node,
		resolveCtx: NewContext(),
	}
}

// NewNodeWWithCtx creates a NodeW with the given Context.
func NewNodeWWithCtx(node ast.Node, resolveCtx *Context) *NodeW {
	return &NodeW{
		Node:       node,
		resolveCtx: resolveCtx,
	}
}

// CloneWithNewNode creates a new NodeW with the given ast.Node.
func (n *NodeW) CloneWithNewNode(newNode ast.Node) *NodeW {
	return &NodeW{
		Node:                      newNode,
		resolveCtx:                n.resolveCtx,
		autoEmbedConsumerPresence: n.autoEmbedConsumerPresence,
	}
}

// WithAutoEmbedConsumerPresence returns a shallow NodeW copy whose effective
// classification is overridden for this build only. It does not mutate the
// AST's resolve context and therefore cannot persist dynamic AST state in a
// prepared-plan cache entry.
func (n *NodeW) WithAutoEmbedConsumerPresence(p AutoEmbedConsumerPresence) *NodeW {
	if n == nil {
		return nil
	}
	clone := *n
	clone.autoEmbedConsumerPresence = &p
	return &clone
}

// AutoEmbedConsumerPresence returns the effective classification for this
// build. A missing NodeW or resolve context is conservatively Unknown.
func (n *NodeW) AutoEmbedConsumerPresence() AutoEmbedConsumerPresence {
	if n == nil {
		return AutoEmbedConsumerUnknown
	}
	if n.autoEmbedConsumerPresence != nil {
		return *n.autoEmbedConsumerPresence
	}
	if n.resolveCtx == nil {
		return AutoEmbedConsumerUnknown
	}
	return n.resolveCtx.AutoEmbedConsumerPresence()
}

// GetResolveContext returns the Context of the NodeW.
func (n *NodeW) GetResolveContext() *Context {
	return n.resolveCtx
}

// Context is used to store the context and result of resolving AST tree.
type Context struct {
	tableNames                map[*ast.TableName]*TableNameW
	autoEmbedConsumerPresence AutoEmbedConsumerPresence
}

// SetAutoEmbedConsumerPresence records the static classification for the AST
// associated with this resolve context.
func (c *Context) SetAutoEmbedConsumerPresence(p AutoEmbedConsumerPresence) {
	if c != nil {
		c.autoEmbedConsumerPresence = p
	}
}

// AutoEmbedConsumerPresence returns the AST's static classification. A nil
// context is conservatively Unknown.
func (c *Context) AutoEmbedConsumerPresence() AutoEmbedConsumerPresence {
	if c == nil {
		return AutoEmbedConsumerUnknown
	}
	return c.autoEmbedConsumerPresence
}

// NewContext creates a Context.
func NewContext() *Context {
	return &Context{
		tableNames: make(map[*ast.TableName]*TableNameW),
	}
}

// AddTableName adds the AST table name and its corresponding TableNameW to the Context.
func (c *Context) AddTableName(tableNameW *TableNameW) {
	c.tableNames[tableNameW.TableName] = tableNameW
}

// GetTableName returns the TableNameW of the AST table name.
// the TableNameW should have been added to the Context in pre-process phase before
// calling this function, if it doesn't exist, pre-process should have returned error,
// so we don't check nil-ness in most cases.
func (c *Context) GetTableName(tableName *ast.TableName) *TableNameW {
	return c.tableNames[tableName]
}

// GetTableNames returns the inner map contain all TableNameW in the Context in pre-process phase.
func (c *Context) GetTableNames() map[*ast.TableName]*TableNameW {
	return c.tableNames
}

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ast is the abstract syntax tree parsed from a SQL statement by parser.
// It can be analysed and transformed by optimizer.
package ast

import (
	"github.com/pingcap/tidb/util/types"
)

// Node is the basic element of the AST.
type Node interface {
	// Accept accepts Visitor to visit itself.
	// The returned node should replace original node.
	// ok returns false to stop visiting.
	Accept(v Visitor) (node Node, ok bool)
	// Text returns the original text of the element.
	Text() string
	// SetText sets original text to the Node.
	SetText(text string)
}

// Expression is a node that can be evaluated.
type Expression interface {
	// Node is embeded in Expression.
	Node
	// IsStatic means it can be evaluated independently.
	IsStatic() bool
	// SetType sets type to the expression.
	SetType(tp *types.FieldType)
	// GetType gets type of the expression.
	GetType() *types.FieldType
}

// Visitor visits a Node.
type Visitor interface {
	// VisitEnter is called before children nodes is visited.
	// ok returns false to stop visiting.
	Enter(n Node) (ok bool)
	// VisitLeave is called after children nodes has been visited.
	// ok returns false to stop visiting.
	Leave(n Node) (node Node, ok bool)
}

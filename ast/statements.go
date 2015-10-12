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

package ast

import (
	"github.com/pingcap/tidb/table"
)

// JoinType is join type, including cross/left/right/full.
type JoinType int

const (
	// CrossJoin is cross join type.
	CrossJoin JoinType = iota + 1
	// LeftJoin is left Join type.
	LeftJoin
	// RightJoin is right Join type.
	RightJoin
)

// JoinNode represents table join.
type JoinNode struct {
	txtNode

	Left  Node
	Right Node
	Tp    JoinType
}

// TableRef represents a reference to actual table.
type TableRef struct {
	// Ident is the table identifier.
	Ident table.Ident
}

// TableSource represents table source with a name.
type TableSource struct {
	txtNode

	// Source is the source of the data, can be a TableRef,
	// a SubQuery, or a JoinNode.
	Source Node

	// Name is the alias name of the table source.
	Name string
}

// SelectNode represents the select query node.
type SelectNode struct {
	txtNode

	// Distinct represents if the select has distinct option.
	Distinct bool
	// Fields is the select expression list.
	Fields []Expression
	// From is the from clause of the query.
	From JoinNode
	// Where is the where clause in select statement.
	Where Expression
	// GroupBy is the group by expression list.
	GroupBy []Expression
	// Having is the having condition.
	Having Expression
	// OrderBy is the odering expression list.
	OrderBy []Expression
	// Offset is the offset value.
	Offset Value
	// Limit is the limit value.
	Limit Value
}

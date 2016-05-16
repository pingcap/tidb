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

package plan

import (
	"github.com/pingcap/tidb/ast"
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin
type JoinType int

const (
	// CrossJoin means Cartesian Product, but not used now
	CrossJoin JoinType = iota
	// InnerJoin means inner join
	InnerJoin
	// LeftOuterJoin means left join
	LeftOuterJoin
	// RightOuterJoin means right join
	RightOuterJoin
	// todo: support semi join
)

// Join is the logical join plan
type Join struct {
	basePlan

	JoinType JoinType

	EqualConditions []ast.ExprNode
	LeftConditions  []ast.ExprNode
	RightConditions []ast.ExprNode
	OtherConditions []ast.ExprNode
}

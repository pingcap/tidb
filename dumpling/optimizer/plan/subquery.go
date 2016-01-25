// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infoschema"
)

// SubQuery is an exprNode with a plan.
type SubQuery struct {
	ast.ExprNode
	Plan Plan
	IS   infoschema.InfoSchema
}

// Accept implements Node Accept interface.
func (sq *SubQuery) Accept(v ast.Visitor) (ast.Node, bool) {
	// SubQuery is not a normal ExprNode
	// Do nothing
	return sq, true
}

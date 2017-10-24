// Copyright 2017 PingCAP, Inc.
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

package cache

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/plan"
)

// SQLCacheValue stores the cached Statement and StmtNode.
type SQLCacheValue struct {
	StmtNode  ast.StmtNode
	Plan      plan.Plan
	Expensive bool
}

// NewSQLCacheValue creates a SQLCacheValue.
func NewSQLCacheValue(ast ast.StmtNode, plan plan.Plan, expensive bool) *SQLCacheValue {
	return &SQLCacheValue{
		StmtNode:  ast,
		Plan:      plan,
		Expensive: expensive,
	}
}

// PSTMTPlanCacheValue stores the cached Statement and StmtNode.
type PSTMTPlanCacheValue struct {
	Plan plan.Plan
}

// NewPSTMTPlanCacheValue creates a SQLCacheValue.
func NewPSTMTPlanCacheValue(plan plan.Plan) *PSTMTPlanCacheValue {
	return &PSTMTPlanCacheValue{
		Plan: plan,
	}
}

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

package util

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// Error instances.
var (
	ErrUnsupportedType      = terror.ClassOptimizerPlan.New(CodeUnsupportedType, "Unsupported type")
	SystemInternalErrorType = terror.ClassOptimizerPlan.New(SystemInternalError, "System internal error")
	ErrUnknownColumn        = terror.ClassOptimizerPlan.New(CodeUnknownColumn, "Unknown column '%s' in '%s'")
	ErrWrongArguments       = terror.ClassOptimizerPlan.New(CodeWrongArguments, "Incorrect arguments to EXECUTE")
	ErrAmbiguous            = terror.ClassOptimizerPlan.New(CodeAmbiguous, "Column '%s' in field list is ambiguous")
	ErrAnalyzeMissIndex     = terror.ClassOptimizerPlan.New(CodeAnalyzeMissIndex, "Index '%s' in field list does not exist in table '%s'")
	ErrAlterAutoID          = terror.ClassAutoid.New(CodeAlterAutoID, "No support for setting auto_increment using alter_table")
)

// Error codes.
const (
	CodeUnsupportedType  terror.ErrCode = 1
	SystemInternalError  terror.ErrCode = 2
	CodeAlterAutoID      terror.ErrCode = 3
	CodeAnalyzeMissIndex terror.ErrCode = 4
	CodeAmbiguous        terror.ErrCode = 1052
	CodeUnknownColumn    terror.ErrCode = 1054
	CodeWrongArguments   terror.ErrCode = 1210
)

func init() {
	tableMySQLErrCodes := map[terror.ErrCode]uint16{
		CodeUnknownColumn:  mysql.ErrBadField,
		CodeAmbiguous:      mysql.ErrNonUniq,
		CodeWrongArguments: mysql.ErrWrongArguments,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizerPlan] = tableMySQLErrCodes
}

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.UnionStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	return n, true
}

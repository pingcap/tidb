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

package optimizer

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/terror"
)

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, node ast.Node, sb plan.SubQueryBuilder) (plan.Plan, error) {
	// We have to infer type again because after parameter is set, the expression type may change.
	if err := InferType(node); err != nil {
		return nil, errors.Trace(err)
	}
	if err := logicOptimize(ctx, node); err != nil {
		return nil, errors.Trace(err)
	}
	p, err := plan.BuildPlan(node, sb)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = plan.Refine(p)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return p, nil
}

// Prepare prepares a raw statement parsed from parser.
// The statement must be prepared before it can be passed to optimize function.
// We pass InfoSchema instead of getting from Context in case it is changed after resolving name.
func Prepare(is infoschema.InfoSchema, ctx context.Context, node ast.Node) error {
	ast.SetFlag(node)
	if err := Preprocess(node, is, ctx); err != nil {
		return errors.Trace(err)
	}
	if err := Validate(node, true); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type supportChecker struct {
	unsupported bool
}

func (c *supportChecker) Enter(in ast.Node) (ast.Node, bool) {
	switch x := in.(type) {
	case *ast.TableName:
		if strings.EqualFold(x.Schema.O, infoschema.Name) {
			c.unsupported = true
		} else if strings.EqualFold(x.Schema.O, perfschema.Name) {
			c.unsupported = true
		}
	}
	return in, c.unsupported
}

func (c *supportChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, !c.unsupported
}

// IsSupported checks if the node is supported to use new plan.
// We first support single table select statement without group by clause or aggregate functions.
// TODO: 1. insert/update/delete. 2. join tables. 3. subquery. 4. group by and aggregate function.
func IsSupported(node ast.Node) bool {
	switch node.(type) {
	case *ast.SelectStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt,
		*ast.AdminStmt, *ast.UpdateStmt, *ast.DeleteStmt, *ast.UnionStmt, *ast.InsertStmt:
	case *ast.UseStmt, *ast.SetStmt, *ast.SetCharsetStmt:
	case *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt:
	case *ast.DoStmt:
	case *ast.CreateUserStmt, *ast.SetPwdStmt, *ast.GrantStmt:
	case *ast.TruncateTableStmt, *ast.AlterTableStmt:
	case *ast.CreateDatabaseStmt, *ast.CreateTableStmt, *ast.CreateIndexStmt:
	case *ast.DropDatabaseStmt, *ast.DropIndexStmt, *ast.DropTableStmt:
	case *ast.ShowStmt:
	case *ast.ExplainStmt:
	default:
		return false
	}

	var checker supportChecker
	node.Accept(&checker)
	return !checker.unsupported
}

// Optimizer error codes.
const (
	CodeOneColumn           terror.ErrCode = 1
	CodeSameColumns         terror.ErrCode = 2
	CodeMultiWildCard       terror.ErrCode = 3
	CodeUnsupported         terror.ErrCode = 4
	CodeInvalidGroupFuncUse terror.ErrCode = 5
	CodeIllegalReference    terror.ErrCode = 6
)

// Optimizer base errors.
var (
	ErrOneColumn           = terror.ClassOptimizer.New(CodeOneColumn, "Operand should contain 1 column(s)")
	ErrSameColumns         = terror.ClassOptimizer.New(CodeSameColumns, "Operands should contain same columns")
	ErrMultiWildCard       = terror.ClassOptimizer.New(CodeMultiWildCard, "wildcard field exist more than once")
	ErrUnSupported         = terror.ClassOptimizer.New(CodeUnsupported, "unsupported")
	ErrInvalidGroupFuncUse = terror.ClassOptimizer.New(CodeInvalidGroupFuncUse, "Invalid use of group function")
	ErrIllegalReference    = terror.ClassOptimizer.New(CodeIllegalReference, "Illegal reference")
)

func init() {
	mySQLErrCodes := map[terror.ErrCode]uint16{
		CodeOneColumn:           mysql.ErrOperandColumns,
		CodeSameColumns:         mysql.ErrOperandColumns,
		CodeMultiWildCard:       mysql.ErrParse,
		CodeInvalidGroupFuncUse: mysql.ErrInvalidGroupFuncUse,
		CodeIllegalReference:    mysql.ErrIllegalReference,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mySQLErrCodes
}

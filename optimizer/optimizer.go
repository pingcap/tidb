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
	"github.com/pingcap/tidb/terror"
)

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, node ast.Node) (plan.Plan, error) {
	// We have to inter type again because after parameter is set, the expression type may change.
	if err := InferType(node); err != nil {
		return nil, errors.Trace(err)
	}
	if err := preEvaluate(ctx, node); err != nil {
		return nil, errors.Trace(err)
	}
	p, err := plan.BuildPlan(node)
	if err != nil {
		return nil, errors.Trace(err)
	}
	alts, err := plan.Alternatives(p)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = plan.Refine(p)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bestCost := plan.EstimateCost(p)
	bestPlan := p
	for _, alt := range alts {
		err = plan.Refine(alt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cost := plan.EstimateCost(alt)
		if cost < bestCost {
			bestCost = cost
			bestPlan = alt
		}
	}
	return bestPlan, nil
}

// Prepare prepares a raw statement parsed from parser.
// The statement must be prepared before it can be passed to optimize function.
// We pass InfoSchema instead of getting from Context in case it is changed after resolving name.
func Prepare(is infoschema.InfoSchema, ctx context.Context, node ast.Node) error {
	if err := Validate(node, true); err != nil {
		return errors.Trace(err)
	}
	ast.SetFlag(node)
	if err := ResolveName(node, is, ctx); err != nil {
		return errors.Trace(err)
	}
	if err := InferType(node); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type supportChecker struct {
	unsupported bool
}

func (c *supportChecker) Enter(in ast.Node) (ast.Node, bool) {
	switch ti := in.(type) {
	case *ast.SubqueryExpr, *ast.GroupByClause, *ast.HavingClause:
		c.unsupported = true
	case *ast.AggregateFuncExpr:
		fn := strings.ToLower(ti.F)
		switch fn {
		case "count":
			c.unsupported = false
		default:
			c.unsupported = true
		}
	case *ast.Join:
		x := in.(*ast.Join)
		if x.Right != nil {
			c.unsupported = true
		} else {
			ts, tsok := x.Left.(*ast.TableSource)
			if !tsok {
				c.unsupported = true
			} else {
				tn, tnok := ts.Source.(*ast.TableName)
				if !tnok {
					c.unsupported = true
				} else if strings.EqualFold(tn.Schema.O, infoschema.Name) {
					c.unsupported = true
				}
			}
		}
	case *ast.SelectStmt:
		x := in.(*ast.SelectStmt)
		if x.Distinct {
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
	case *ast.SelectStmt, *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt:
	default:
		return false
	}

	var checker supportChecker
	node.Accept(&checker)
	return !checker.unsupported
}

// Optimizer error codes.
const (
	CodeOneColumn     terror.ErrCode = 1
	CodeSameColumns                  = 2
	CodeMultiWildCard                = 3
	CodeUnsupported                  = 4
)

// Optimizer base errors.
var (
	ErrOneColumn     = terror.ClassOptimizer.New(CodeOneColumn, "Operand should contain 1 column(s)")
	ErrSameColumns   = terror.ClassOptimizer.New(CodeSameColumns, "Operands should contain same columns")
	ErrMultiWildCard = terror.ClassOptimizer.New(CodeMultiWildCard, "wildcard field exist more than once")
	ErrUnSupported   = terror.ClassOptimizer.New(CodeUnsupported, "unsupported")
)

func init() {
	mySQLErrCodes := map[terror.ErrCode]uint16{
		CodeOneColumn:     mysql.ErrOperandColumns,
		CodeSameColumns:   mysql.ErrOperandColumns,
		CodeMultiWildCard: mysql.ErrParse,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mySQLErrCodes
}

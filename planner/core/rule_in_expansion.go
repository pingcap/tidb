// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"context"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
)

// inExpansionSolver tries to convert `IN` to `UNION` + `EQ`(s)
// For condition like `a=0 AND b IN(1, 2)`, we could convert it to `(a=0 AND b=1) UNION ALL (a=0 AND b=2)`.
type inExpansionSolver struct {
}

func (a *inExpansionSolver) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	return p.expandInExpr(opt), nil
}

func (s *baseLogicalPlan) expandInExpr(opt *logicalOptimizeOp) LogicalPlan {
	p := s.self
	for i, child := range p.Children() {
		p.Children()[i] = child.expandInExpr(opt)
	}
	return p
}

func copyDataSource(ds *DataSource) LogicalPlan {
	newDS := *ds
	newDS.baseLogicalPlan = newBaseLogicalPlan(ds.ctx, ds.tp, &newDS, 0)
	newDS.pushedDownConds = make([]expression.Expression, 0, len(ds.pushedDownConds))
	newDS.pushedDownConds = append(newDS.pushedDownConds, ds.pushedDownConds...)
	newDS.allConds = make([]expression.Expression, 0, len(ds.allConds))
	newDS.allConds = append(newDS.allConds, ds.allConds...)
	newDS.possibleAccessPaths = make([]*util.AccessPath, len(ds.possibleAccessPaths))
	for i := range ds.possibleAccessPaths {
		newPath := *ds.possibleAccessPaths[i]
		newDS.possibleAccessPaths[i] = &newPath
	}
	newDS.id = ds.id
	return &newDS
}

// if is `IN` expr, try to deduplicate its arguments and return a new `IN` expr
func deduplicateInExpression(expr expression.Expression) (bool, *expression.ScalarFunction) {
	function, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false, nil
	}
	if function.FuncName.L != ast.In {
		return false, nil
	}
	inExpr := function
	// usually no duplicate args, so allocate a slice with same size
	newArgs := make([]expression.Expression, 0, len(inExpr.GetArgs()))
	newArgs = append(newArgs, inExpr.GetArgs()[0])
	for _, arg := range inExpr.GetArgs()[1:] {
		// only support `Constant` instead of `ConstItem`,
		// because we don't want to compute the equality of `Constant` and some const `ScalarFunction`
		if value, ok := arg.(*expression.Constant); !ok {
			return true, nil
		} else if value.GetType().GetType() == mysql.TypeNull {
			return true, nil
		}
		isDuplicated := false
		for _, newArg := range newArgs {
			if arg.Equal(inExpr.GetCtx(), newArg) {
				isDuplicated = true
				break
			}
		}
		if !isDuplicated {
			newArgs = append(newArgs, arg)
		}
	}
	newInExpr, err := expression.NewFunction(inExpr.GetCtx(), ast.In, types.NewFieldType(mysql.TypeTiny), newArgs...)
	if err != nil {
		return true, nil
	}
	return true, newInExpr.(*expression.ScalarFunction)
}

func (s *LogicalSelection) expandInExpr(opt *logicalOptimizeOp) LogicalPlan {
	p := s.self
	// only match the bottom `DataSource->Selection` pattern
	if ds, ok := p.Children()[0].(*DataSource); ok {
		if ds.Children() != nil && len(ds.Children()) > 0 {
			return p
		}
	} else {
		return p
	}
	var inConditions []*expression.ScalarFunction
	var nonInConditions []expression.Expression
	for _, condition := range s.Conditions {
		isInExpr, newExpr := deduplicateInExpression(condition)
		if !isInExpr {
			nonInConditions = append(nonInConditions, condition)
		} else {
			if newExpr == nil {
				return p
			}
			inConditions = append(inConditions, newExpr)
		}
	}
	if len(inConditions) == 0 {
		return p
	}
	newSelectionNum := 1
	for _, condition := range inConditions {
		newSelectionNum = newSelectionNum * (len(condition.GetArgs()) - 1)
	}
	if newSelectionNum > p.SCtx().GetSessionVars().InExpansionLimit {
		return p
	}
	selections := make([]LogicalPlan, 0, newSelectionNum)
	// each `i` represents an element of cartesian product
	for i := 0; i < newSelectionNum; i++ {
		j := i
		newConditions := make([]expression.Expression, 0, len(s.Conditions))
		for _, condition := range inConditions {
			argsNum := len(condition.GetArgs()) - 1
			k := j % argsNum
			equal, err := expression.NewFunction(s.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), condition.GetArgs()[0], condition.GetArgs()[k+1])
			j = j / argsNum
			if err != nil {
				return p
			}
			newConditions = append(newConditions, equal)
		}
		newConditions = append(newConditions, nonInConditions...)
		selection := LogicalSelection{Conditions: newConditions}.Init(p.SCtx(), 0)
		newChild := copyDataSource(p.Children()[0].(*DataSource))
		selection.SetChildren(newChild)
		selections = append(selections, selection)
	}
	union := LogicalUnionAll{}.Init(p.SCtx(), 0)
	union.SetSchema(p.Schema())
	union.SetChildren(selections...)
	return union
}

func (*inExpansionSolver) name() string {
	return "in_expansion"
}

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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = true

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	// We have to infer type again because after parameter is set, the expression type may change.
	if err := InferType(node); err != nil {
		return nil, errors.Trace(err)
	}
	allocator := new(idAllocator)
	builder := &planBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
		allocator: allocator}
	p := builder.build(node)
	if builder.err != nil {
		return nil, errors.Trace(builder.err)
	}
	if logic, ok := p.(LogicalPlan); ok {
		var err error
		_, logic, err = logic.PredicatePushDown(nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		solver := &aggPushDownSolver{
			ctx:   ctx,
			alloc: allocator,
		}
		solver.aggPushDown(logic)
		_, err = logic.PruneColumnsAndResolveIndices(p.GetSchema())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !AllowCartesianProduct && existsCartesianProduct(logic) {
			return nil, ErrCartesianProductUnsupported
		}
		info, err := logic.convert2PhysicalPlan(&requiredProperty{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		pp := info.p
		pp = EliminateProjection(pp)
		log.Debugf("[PLAN] %s", ToString(pp))
		return pp, nil
	}
	return p, nil
}
func existsCartesianProduct(p LogicalPlan) bool {
	if join, ok := p.(*Join); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	for _, child := range p.GetChildren() {
		if existsCartesianProduct(child.(LogicalPlan)) {
			return true
		}
	}
	return false
}

// PrepareStmt prepares a raw statement parsed from parser.
// The statement must be prepared before it can be passed to optimize function.
// We pass InfoSchema instead of getting from Context in case it is changed after resolving name.
func PrepareStmt(is infoschema.InfoSchema, ctx context.Context, node ast.Node) error {
	if err := Preprocess(node, is, ctx); err != nil {
		return errors.Trace(err)
	}
	if err := Validate(node, true); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Optimizer error codes.
const (
	CodeOneColumn           terror.ErrCode = 1
	CodeSameColumns         terror.ErrCode = 2
	CodeInvalidWildCard     terror.ErrCode = 3
	CodeUnsupported         terror.ErrCode = 4
	CodeInvalidGroupFuncUse terror.ErrCode = 5
	CodeIllegalReference    terror.ErrCode = 6
)

// Optimizer base errors.
var (
	ErrOneColumn                   = terror.ClassOptimizer.New(CodeOneColumn, "Operand should contain 1 column(s)")
	ErrSameColumns                 = terror.ClassOptimizer.New(CodeSameColumns, "Operands should contain same columns")
	ErrInvalidWildCard             = terror.ClassOptimizer.New(CodeInvalidWildCard, "Wildcard fields without any table name appears in wrong place")
	ErrCartesianProductUnsupported = terror.ClassOptimizer.New(CodeUnsupported, "Cartesian product is unsupported")
	ErrInvalidGroupFuncUse         = terror.ClassOptimizer.New(CodeInvalidGroupFuncUse, "Invalid use of group function")
	ErrIllegalReference            = terror.ClassOptimizer.New(CodeIllegalReference, "Illegal reference")
)

func init() {
	mySQLErrCodes := map[terror.ErrCode]uint16{
		CodeOneColumn:           mysql.ErrOperandColumns,
		CodeSameColumns:         mysql.ErrOperandColumns,
		CodeInvalidWildCard:     mysql.ErrParse,
		CodeInvalidGroupFuncUse: mysql.ErrInvalidGroupFuncUse,
		CodeIllegalReference:    mysql.ErrIllegalReference,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mySQLErrCodes
}

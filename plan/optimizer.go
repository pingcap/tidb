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
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/terror"
)

// AllowCartesianProduct means whether tidb allows cartesian join without equal conditions.
var AllowCartesianProduct = true

const (
	flagPrunColumns uint64 = 1 << iota
	flagEliminateProjection
	flagBuildKeyInfo
	flagDecorrelate
	flagPredicatePushDown
	flagAggregationOptimize
	flagPushDownTopN
)

var optRuleList = []logicalOptRule{
	&columnPruner{},
	&projectionEliminater{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&ppdSolver{},
	&aggregationOptimizer{},
	&pushDownTopNOptimizer{},
}

// logicalOptRule means a logical optimizing rule, which contains decorrelate, ppd, column pruning, etc.
type logicalOptRule interface {
	optimize(LogicalPlan, context.Context, *idAllocator) (LogicalPlan, error)
}

// Optimize does optimization and creates a Plan.
// The node must be prepared first.
func Optimize(ctx context.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	allocator := new(idAllocator)
	builder := &planBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
		allocator: allocator,
	}
	p := builder.build(node)
	if builder.err != nil {
		return nil, errors.Trace(builder.err)
	}

	// Maybe it's better to move this to Preprocess, but check privilege need table
	// information, which is collected into visitInfo during logical plan builder.
	if pm := privilege.GetPrivilegeManager(ctx); pm != nil {
		if !checkPrivilege(pm, builder.visitInfo) {
			return nil, errors.New("privilege check fail")
		}
	}

	if logic, ok := p.(LogicalPlan); ok {
		return doOptimize(builder.optFlag, logic, ctx, allocator)
	}
	return p, nil
}

// BuildLogicalPlan is exported and only used for test.
func BuildLogicalPlan(ctx context.Context, node ast.Node, is infoschema.InfoSchema) (Plan, error) {
	builder := &planBuilder{
		ctx:       ctx,
		is:        is,
		colMapper: make(map[*ast.ColumnNameExpr]int),
		allocator: new(idAllocator),
	}
	p := builder.build(node)
	if builder.err != nil {
		return nil, errors.Trace(builder.err)
	}
	return p, nil
}

func checkPrivilege(pm privilege.Manager, vs []visitInfo) bool {
	for _, v := range vs {
		if !pm.RequestVerification(v.db, v.table, v.column, v.privilege) {
			return false
		}
	}
	return true
}

func doOptimize(flag uint64, logic LogicalPlan, ctx context.Context, allocator *idAllocator) (PhysicalPlan, error) {
	logic, err := logicalOptimize(flag, logic, ctx, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !AllowCartesianProduct && existsCartesianProduct(logic) {
		return nil, errors.Trace(ErrCartesianProductUnsupported)
	}
	var physical PhysicalPlan
	if UseDAGPlanBuilder(ctx) {
		physical, err = dagPhysicalOptimize(logic)
	} else {
		physical, err = physicalOptimize(flag, logic, allocator)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	finalPlan := eliminatePhysicalProjection(physical)
	return finalPlan, nil
}

func logicalOptimize(flag uint64, logic LogicalPlan, ctx context.Context, alloc *idAllocator) (LogicalPlan, error) {
	var err error
	for i, rule := range optRuleList {
		// The order of flags is same as the order of optRule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 {
			continue
		}
		logic, err = rule.optimize(logic, ctx, alloc)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return logic, errors.Trace(err)
}

func dagPhysicalOptimize(logic LogicalPlan) (PhysicalPlan, error) {
	logic.preparePossibleProperties()
	logic.prepareStatsProfile()
	t, err := logic.convert2NewPhysicalPlan(&requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64})
	if err != nil {
		return nil, errors.Trace(err)
	}
	p := t.plan()
	rebuildSchema(p)
	p.ResolveIndices()
	return p, nil
}

func physicalOptimize(flag uint64, logic LogicalPlan, allocator *idAllocator) (PhysicalPlan, error) {
	logic.ResolveIndices()
	info, err := logic.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	p := info.p
	if flag&(flagDecorrelate) > 0 {
		addCachePlan(p, allocator)
	}
	return p, nil
}

func existsCartesianProduct(p LogicalPlan) bool {
	if join, ok := p.(*LogicalJoin); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	for _, child := range p.Children() {
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
	if err := ResolveName(node, is, ctx); err != nil {
		return errors.Trace(err)
	}
	if err := Preprocess(ctx, node, is, true); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Optimizer error codes.
const (
	CodeOperandColumns      terror.ErrCode = 1
	CodeInvalidWildCard     terror.ErrCode = 3
	CodeUnsupported         terror.ErrCode = 4
	CodeInvalidGroupFuncUse terror.ErrCode = 5
	CodeIllegalReference    terror.ErrCode = 6

	// MySQL error code.
	CodeNoDB                 terror.ErrCode = mysql.ErrNoDB
	CodeUnknownExplainFormat terror.ErrCode = mysql.ErrUnknownExplainFormat
)

// Optimizer base errors.
var (
	ErrOperandColumns              = terror.ClassOptimizer.New(CodeOperandColumns, "Operand should contain %d column(s)")
	ErrInvalidWildCard             = terror.ClassOptimizer.New(CodeInvalidWildCard, "Wildcard fields without any table name appears in wrong place")
	ErrCartesianProductUnsupported = terror.ClassOptimizer.New(CodeUnsupported, "Cartesian product is unsupported")
	ErrInvalidGroupFuncUse         = terror.ClassOptimizer.New(CodeInvalidGroupFuncUse, "Invalid use of group function")
	ErrIllegalReference            = terror.ClassOptimizer.New(CodeIllegalReference, "Illegal reference")
	ErrNoDB                        = terror.ClassOptimizer.New(CodeNoDB, "No database selected")
	ErrUnknownExplainFormat        = terror.ClassOptimizer.New(CodeUnknownExplainFormat, mysql.MySQLErrName[mysql.ErrUnknownExplainFormat])
)

func init() {
	mySQLErrCodes := map[terror.ErrCode]uint16{
		CodeOperandColumns:       mysql.ErrOperandColumns,
		CodeInvalidWildCard:      mysql.ErrParse,
		CodeInvalidGroupFuncUse:  mysql.ErrInvalidGroupFuncUse,
		CodeIllegalReference:     mysql.ErrIllegalReference,
		CodeNoDB:                 mysql.ErrNoDB,
		CodeUnknownExplainFormat: mysql.ErrUnknownExplainFormat,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mySQLErrCodes
	expression.EvalAstExpr = evalAstExpr
}

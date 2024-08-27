// Copyright 2023-2023 PingCAP, Inc.
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

package table

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"go.uber.org/zap"
)

// Constraint provides meta and map dependency describing a table constraint.
type Constraint struct {
	*model.ConstraintInfo
	ConstraintExpr expression.Expression
}

// LoadCheckConstraint load check constraint
func LoadCheckConstraint(tblInfo *model.TableInfo) ([]*Constraint, error) {
	removeInvalidCheckConstraintsInfo(tblInfo)

	constraints := make([]*Constraint, 0, len(tblInfo.Constraints))
	for _, conInfo := range tblInfo.Constraints {
		con, err := ToConstraint(conInfo, tblInfo)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, con)
	}
	return constraints, nil
}

// Delete invalid check constraint lazily
func removeInvalidCheckConstraintsInfo(tblInfo *model.TableInfo) {
	// check if all the columns referenced by check constraint exist
	var conInfos []*model.ConstraintInfo
	for _, cons := range tblInfo.Constraints {
		valid := true
		for _, col := range cons.ConstraintCols {
			if tblInfo.FindPublicColumnByName(col.L) == nil {
				valid = false
				break
			}
		}
		if valid {
			conInfos = append(conInfos, cons)
		}
	}
	tblInfo.Constraints = conInfos
}

// ToConstraint converts model.ConstraintInfo to Constraint
func ToConstraint(constraintInfo *model.ConstraintInfo, tblInfo *model.TableInfo) (*Constraint, error) {
	ctx := mock.NewContext()
	dbName := ctx.GetSessionVars().CurrentDB
	expr, err := buildConstraintExpression(ctx, constraintInfo.ExprString, dbName, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Constraint{
		constraintInfo,
		expr,
	}, nil
}

func buildConstraintExpression(ctx expression.BuildContext, exprString string, db string, tblInfo *model.TableInfo) (expression.Expression, error) {
	expr, err := expression.ParseSimpleExpr(ctx, exprString, expression.WithTableInfo(db, tblInfo))
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong check constraint expression", zap.String("expression", exprString), zap.Error(err))
		return nil, errors.Trace(err)
	}
	return expr, nil
}

// IsSupportedExpr checks whether the check constraint expression is allowed
func IsSupportedExpr(constr *ast.Constraint) (bool, error) {
	checker := &checkConstraintChecker{
		allowed: true,
		name:    constr.Name,
	}
	constr.Expr.Accept(checker)
	return checker.allowed, checker.reason
}

var unsupportedNodeForCheckConstraint = map[string]struct{}{
	ast.Now:              {},
	ast.CurrentTimestamp: {},
	ast.Curdate:          {},
	ast.CurrentDate:      {},
	ast.Curtime:          {},
	ast.CurrentTime:      {},
	ast.LocalTime:        {},
	ast.LocalTimestamp:   {},
	ast.UnixTimestamp:    {},
	ast.UTCDate:          {},
	ast.UTCTimestamp:     {},
	ast.UTCTime:          {},
	ast.ConnectionID:     {},
	ast.CurrentUser:      {},
	ast.SessionUser:      {},
	ast.Version:          {},
	ast.FoundRows:        {},
	ast.LastInsertId:     {},
	ast.SystemUser:       {},
	ast.User:             {},
	ast.Rand:             {},
	ast.RowCount:         {},
	ast.GetLock:          {},
	ast.IsFreeLock:       {},
	ast.IsUsedLock:       {},
	ast.ReleaseLock:      {},
	ast.ReleaseAllLocks:  {},
	ast.LoadFile:         {},
	ast.UUID:             {},
	ast.UUIDShort:        {},
	ast.Sleep:            {},
}

type checkConstraintChecker struct {
	allowed bool
	reason  error
	name    string
}

// Enter implements Visitor interface.
func (checker *checkConstraintChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch x := in.(type) {
	case *ast.FuncCallExpr:
		if _, ok := unsupportedNodeForCheckConstraint[x.FnName.L]; ok {
			checker.allowed = false
			checker.reason = dbterror.ErrCheckConstraintNamedFuncIsNotAllowed.GenWithStackByArgs(checker.name, x.FnName.L)
			return in, true
		}
	case *ast.VariableExpr:
		// user defined or system variable is not allowed
		checker.allowed = false
		checker.reason = dbterror.ErrCheckConstraintVariables.GenWithStackByArgs(checker.name)
		return in, true
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// subquery is not allowed
		checker.allowed = false
		checker.reason = dbterror.ErrCheckConstraintFuncIsNotAllowed.GenWithStackByArgs(checker.name)
		return in, true
	case *ast.DefaultExpr:
		// default expr is not allowed
		checker.allowed = false
		checker.reason = dbterror.ErrCheckConstraintNamedFuncIsNotAllowed.GenWithStackByArgs(checker.name, "default")
		return in, true
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *checkConstraintChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.allowed
}

// ContainsAutoIncrementCol checks if there is auto-increment col in given cols
func ContainsAutoIncrementCol(cols []model.CIStr, tblInfo *model.TableInfo) bool {
	if autoIncCol := tblInfo.GetAutoIncrementColInfo(); autoIncCol != nil {
		for _, col := range cols {
			if col.L == autoIncCol.Name.L {
				return true
			}
		}
	}
	return false
}

// HasForeignKeyRefAction checks if there is foreign key with referential action in check constraints
func HasForeignKeyRefAction(fkInfos []*model.FKInfo, constraints []*ast.Constraint, checkConstr *ast.Constraint, dependedCols []model.CIStr) error {
	if fkInfos != nil {
		return checkForeignKeyRefActionByFKInfo(fkInfos, checkConstr, dependedCols)
	}
	for _, cons := range constraints {
		if cons.Tp != ast.ConstraintForeignKey {
			continue
		}
		refCol := cons.Refer
		if refCol.OnDelete.ReferOpt != model.ReferOptionNoOption || refCol.OnUpdate.ReferOpt != model.ReferOptionNoOption {
			var fkCols []model.CIStr
			for _, key := range cons.Keys {
				fkCols = append(fkCols, key.Column.Name)
			}
			for _, col := range dependedCols {
				if hasSpecifiedCol(fkCols, col) {
					return dbterror.ErrCheckConstraintUsingFKReferActionColumn.GenWithStackByArgs(col.L, checkConstr.Name)
				}
			}
		}
	}
	return nil
}

func checkForeignKeyRefActionByFKInfo(fkInfos []*model.FKInfo, checkConstr *ast.Constraint, dependedCols []model.CIStr) error {
	for _, fkInfo := range fkInfos {
		if fkInfo.OnDelete != 0 || fkInfo.OnUpdate != 0 {
			for _, col := range dependedCols {
				if hasSpecifiedCol(fkInfo.Cols, col) {
					return dbterror.ErrCheckConstraintUsingFKReferActionColumn.GenWithStackByArgs(col.L, checkConstr.Name)
				}
			}
		}
	}
	return nil
}

func hasSpecifiedCol(cols []model.CIStr, col model.CIStr) bool {
	for _, c := range cols {
		if c.L == col.L {
			return true
		}
	}
	return false
}

// IfCheckConstraintExprBoolType checks whether the check expression is bool type
func IfCheckConstraintExprBoolType(ctx expression.EvalContext, info *model.ConstraintInfo, tableInfo *model.TableInfo) error {
	cons, err := ToConstraint(info, tableInfo)
	if err != nil {
		return err
	}
	if !mysql.HasIsBooleanFlag(cons.ConstraintExpr.GetType(ctx).GetFlag()) {
		return dbterror.ErrNonBooleanExprForCheckConstraint.GenWithStackByArgs(cons.Name)
	}
	return nil
}

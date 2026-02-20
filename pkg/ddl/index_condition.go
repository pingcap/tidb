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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// CheckAndBuildIndexConditionString validates whether the given expression is compatible with
// the table schema and returns a string representation of the expression.
func CheckAndBuildIndexConditionString(tblInfo *model.TableInfo, indexConditionExpr ast.ExprNode) (string, error) {
	if indexConditionExpr == nil {
		return "", nil
	}

	// Be careful, in `CREATE TABLE` statement, the `tblInfo.Partition` is always nil here. We have to
	// check it in `buildTablePartitionInfo` again.
	if tblInfo.Partition != nil {
		return "", dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"partial index on partitioned table is not supported")
	}

	// check partial index condition expression
	err := checkIndexCondition(tblInfo, indexConditionExpr)
	if err != nil {
		return "", errors.Trace(err)
	}

	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	sb.Reset()
	err = indexConditionExpr.Restore(restoreCtx)
	if err != nil {
		return "", errors.Trace(err)
	}

	return sb.String(), nil
}

func checkIndexCondition(tblInfo *model.TableInfo, indexCondition ast.ExprNode) error {
	// Only the following expressions are supported:
	// 1. column IS NULL
	// 2. column IS NOT NULL
	// 3. column = / != / > / < / >= / <= const
	// The column must be a visible column in the table, and the const must be a literal value with
	// the same type as the column.
	// The column must **NOT** be a generated column. We can loosen this restriction in the future.
	//
	// TODO: support more expressions in the future.
	if indexCondition == nil {
		return nil
	}

	switch cond := indexCondition.(type) {
	case *ast.IsNullExpr:
		// `IS NULL` and `IS NOT NULL` are both in this branch.
		columnName, ok := cond.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a column name in the IS NULL expression")
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name %s referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		return nil
	case *ast.BinaryOperationExpr:
		if cond.Op != opcode.EQ && cond.Op != opcode.NE && cond.Op != opcode.GT &&
			cond.Op != opcode.LT && cond.Op != opcode.GE && cond.Op != opcode.LE {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("binary operation %s is not supported", cond.Op.String()))
		}

		var columnName *ast.ColumnNameExpr
		var anotherSide ast.ExprNode
		columnName, ok := cond.L.(*ast.ColumnNameExpr)
		if !ok {
			// maybe the right side is a column name
			columnName, ok = cond.R.(*ast.ColumnNameExpr)
			if !ok {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					"partial index condition must include a column name in the binary operation")
			}

			anotherSide = cond.L
		} else {
			anotherSide = cond.R
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name `%s` referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		// The another side must be a literal value, and it must have the same type as the column.
		constantExpr, ok := anotherSide.(ast.ValueExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a literal value on the other side of the binary operation")
		}
		// Reference `types.DefaultTypeForValue`, they are all possible types for literal values.
		// However, this switch-case still includes more types than the ones we have in that function
		// to avoid breaking in the future.
		//
		// Accept tiny type conversion as the type of the literal value is too limited. We shouldn't
		// force the user to use such a limited range of types.
		//
		// It'll allow precision / length difference in most of the cases.
		switch constantExpr.GetType().GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeInt24, mysql.TypeBit, mysql.TypeYear:
			// the target column must be an integer type or enum or set
			if columnInfo.GetType() != mysql.TypeTiny &&
				columnInfo.GetType() != mysql.TypeShort &&
				columnInfo.GetType() != mysql.TypeLong &&
				columnInfo.GetType() != mysql.TypeLonglong &&
				columnInfo.GetType() != mysql.TypeInt24 &&
				columnInfo.GetType() != mysql.TypeBit &&
				columnInfo.GetType() != mysql.TypeYear &&
				columnInfo.GetType() != mysql.TypeEnum &&
				columnInfo.GetType() != mysql.TypeSet {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			// the target column must be either a float or double type
			// TODO: consider whether need to support decimal type in this branch
			if columnInfo.GetType() != mysql.TypeFloat &&
				columnInfo.GetType() != mysql.TypeDouble &&
				columnInfo.GetType() != mysql.TypeNewDecimal {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			if types.IsString(columnInfo.GetType()) {
				// check the collation of the column and the literal value
				if columnInfo.FieldType.GetCharset() != constantExpr.GetType().GetCharset() {
					return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
						fmt.Sprintf("the charset %s of the column `%s` in partial index condition is not compatible with the literal value charset %s",
							columnInfo.FieldType.GetCharset(), columnName.Name.Name.L, constantExpr.GetType().GetCharset()))
				}

				return nil
			}

			// Allow to compare a datetime type column with a string literal, because we don't have a datetime literal.
			// This branch will allow users to use datetime columns in index condition.
			if columnInfo.GetType() == mysql.TypeTimestamp ||
				columnInfo.GetType() == mysql.TypeDate ||
				columnInfo.GetType() == mysql.TypeDuration ||
				columnInfo.GetType() == mysql.TypeNewDate ||
				columnInfo.GetType() == mysql.TypeDatetime {
				return nil
			}

			// ENUM and SET are also allowed for string literal.
			if columnInfo.GetType() == mysql.TypeEnum || columnInfo.GetType() == mysql.TypeSet {
				return nil
			}

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
					columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
		case mysql.TypeNull:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"= NULL is not supported in partial index condition because it is always false")
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration, mysql.TypeNewDate,
			mysql.TypeDatetime, mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet:
			// The `DATE '2025-07-28'` is actually a `cast` function, so they are also not supported yet.
			intest.Assert(false, "should never generate literal values of these types")

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		default:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		}
	default:
		return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"the kind of partial index condition is not supported")
	}
}

func buildAffectColumn(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]*model.IndexColumn, error) {
	ectx := exprstatic.NewExprContext()

	// Build affect column for partial index.
	if idxInfo.HasCondition() {
		cols, err := tables.ExtractColumnsFromCondition(ectx, idxInfo, tblInfo, true)
		if err != nil {
			return nil, err
		}
		return tables.DedupIndexColumns(cols), nil
	}

	return nil, nil
}

// buildIndexConditionChecker builds an expression for evaluating the index condition based on
// the given columns.
func buildIndexConditionChecker(copCtx copr.CopContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) (func(row chunk.Row) (bool, error), error) {
	schema, names := copCtx.GetBase().GetSchemaAndNames()

	exprCtx := copCtx.GetBase().ExprCtx
	expr, err := expression.ParseSimpleExpr(exprCtx, idxInfo.ConditionExprString, expression.WithInputSchemaAndNames(schema, names, tblInfo))
	if err != nil {
		return nil, err
	}

	return func(row chunk.Row) (bool, error) {
		datum, isNull, err := expr.EvalInt(exprCtx.GetEvalCtx(), row)
		if err != nil {
			return false, err
		}
		// If the result is NULL, it usually means the original column itself is NULL.
		// In this case, we should refuse to consider the index for partial index condition.
		return datum > 0 && !isNull, nil
	}, nil
}

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
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func precheckBuildHiddenColumnInfo(
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexName ast.CIStr,
) error {
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		name := fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i)
		if utf8.RuneCountInString(name) > mysql.MaxColumnNameLength {
			// TODO: Refine the error message.
			return dbterror.ErrTooLongIdent.GenWithStackByArgs("hidden column")
		}
		// TODO: Refine the error message.
		if err := checkIllegalFn4Generated(indexName.L, typeIndex, idxPart.Expr); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func buildHiddenColumnInfoWithCheck(ctx *metabuild.Context, indexPartSpecifications []*ast.IndexPartSpecification, indexName ast.CIStr, tblInfo *model.TableInfo, existCols []*table.Column) ([]*model.ColumnInfo, error) {
	if err := precheckBuildHiddenColumnInfo(indexPartSpecifications, indexName); err != nil {
		return nil, err
	}
	return BuildHiddenColumnInfo(ctx, indexPartSpecifications, indexName, tblInfo, existCols)
}

// BuildHiddenColumnInfo builds hidden column info.
func BuildHiddenColumnInfo(ctx *metabuild.Context, indexPartSpecifications []*ast.IndexPartSpecification, indexName ast.CIStr, tblInfo *model.TableInfo, existCols []*table.Column) ([]*model.ColumnInfo, error) {
	hiddenCols := make([]*model.ColumnInfo, 0, len(indexPartSpecifications))
	for i, idxPart := range indexPartSpecifications {
		if idxPart.Expr == nil {
			continue
		}
		idxPart.Column = &ast.ColumnName{Name: ast.NewCIStr(fmt.Sprintf("%s_%s_%d", expressionIndexPrefix, indexName, i))}
		// Check whether the hidden columns have existed.
		col := table.FindCol(existCols, idxPart.Column.Name.L)
		if col != nil {
			// TODO: Use expression index related error.
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name.String())
		}
		idxPart.Length = types.UnspecifiedLength
		// The index part is an expression, prepare a hidden column for it.

		var sb strings.Builder
		restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
			format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
		restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
		sb.Reset()
		err := idxPart.Expr.Restore(restoreCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}

		exprCtx := ctx.GetExprCtx()
		expr, err := expression.BuildSimpleExpr(exprCtx, idxPart.Expr,
			expression.WithTableInfo(exprCtx.GetEvalCtx().CurrentDB(), tblInfo),
			expression.WithAllowCastArray(true),
		)
		if err != nil {
			// TODO: refine the error message.
			return nil, err
		}
		if _, ok := expr.(*expression.Column); ok {
			return nil, dbterror.ErrFunctionalIndexOnField
		}

		colInfo := &model.ColumnInfo{
			Name:                idxPart.Column.Name,
			GeneratedExprString: sb.String(),
			GeneratedStored:     false,
			Version:             model.CurrLatestColumnInfoVersion,
			Dependences:         make(map[string]struct{}),
			Hidden:              true,
			FieldType:           *expr.GetType(ctx.GetExprCtx().GetEvalCtx()),
		}
		// Reset some flag, it may be caused by wrong type infer. But it's not easy to fix them all, so reset them here for safety.
		colInfo.DelFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.AutoIncrementFlag)

		if colInfo.GetType() == mysql.TypeDatetime || colInfo.GetType() == mysql.TypeDate || colInfo.GetType() == mysql.TypeTimestamp || colInfo.GetType() == mysql.TypeDuration {
			if colInfo.FieldType.GetDecimal() == types.UnspecifiedLength {
				colInfo.FieldType.SetDecimal(types.MaxFsp)
			}
		}
		// For an array, the collation is set to "binary". The collation has no effect on the array itself (as it's usually
		// regarded as a JSON), but will influence how TiKV handles the index value.
		if colInfo.FieldType.IsArray() {
			colInfo.SetCharset("binary")
			colInfo.SetCollate("binary")
		}
		checkDependencies := make(map[string]struct{})
		for _, colName := range FindColumnNamesInExpr(idxPart.Expr) {
			colInfo.Dependences[colName.Name.L] = struct{}{}
			checkDependencies[colName.Name.L] = struct{}{}
		}
		if err = checkDependedColExist(checkDependencies, existCols); err != nil {
			return nil, errors.Trace(err)
		}
		if !ctx.EnableAutoIncrementInGenerated() {
			if err = checkExpressionIndexAutoIncrement(indexName.O, colInfo.Dependences, tblInfo); err != nil {
				return nil, errors.Trace(err)
			}
		}
		idxPart.Expr = nil
		hiddenCols = append(hiddenCols, colInfo)
	}
	return hiddenCols, nil
}

// addIndexForForeignKey uses to auto create an index for the foreign key if the table doesn't have any index cover the
// foreign key columns.
func addIndexForForeignKey(ctx *metabuild.Context, tbInfo *model.TableInfo) error {
	if len(tbInfo.ForeignKeys) == 0 {
		return nil
	}
	var handleCol *model.ColumnInfo
	if tbInfo.PKIsHandle {
		handleCol = tbInfo.GetPkColInfo()
	}
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		if handleCol != nil && len(fk.Cols) == 1 && handleCol.Name.L == fk.Cols[0].L {
			continue
		}
		if model.FindIndexByColumns(tbInfo, tbInfo.Indices, fk.Cols...) != nil {
			continue
		}
		idxName := fk.Name
		if tbInfo.FindIndexByName(idxName.L) != nil {
			return dbterror.ErrDupKeyName.GenWithStack("duplicate key name %s", fk.Name.O)
		}
		keys := make([]*ast.IndexPartSpecification, 0, len(fk.Cols))
		for _, col := range fk.Cols {
			keys = append(keys, &ast.IndexPartSpecification{
				Column: &ast.ColumnName{Name: col},
				Length: types.UnspecifiedLength,
			})
		}
		idxInfo, err := BuildIndexInfo(ctx, tbInfo, idxName, false, false, model.ColumnarIndexTypeNA, keys, nil, model.StatePublic)
		if err != nil {
			return errors.Trace(err)
		}
		idxInfo.ID = AllocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return nil
}

func isIntCol(col *model.ColumnInfo) bool {
	switch col.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
		return true
	}
	return false
}

// isSingleIntPKFromTableInfo determines if a constraint represents a single integer primary key
// by looking up the column information from the table info. This is used during the pre-scan
// phase before CheckPKOnGeneratedColumn is called.
func isSingleIntPKFromTableInfo(constr *ast.Constraint, tbInfo *model.TableInfo) bool {
	// Multi-column PKs are not single integer PKs
	if len(constr.Keys) != 1 {
		return false
	}

	// Expression-based PKs (e.g., PRIMARY KEY ((col+1))) are not single integer PKs
	if constr.Keys[0].Expr != nil {
		return false
	}

	// Find the column in the table
	colName := constr.Keys[0].Column.Name.L
	for _, col := range tbInfo.Columns {
		if col.Name.L == colName {
			return isIntCol(col)
		}
	}
	return false
}

func isSingleIntPKFromCol(constr *ast.Constraint, lastCol *model.ColumnInfo) bool {
	if len(constr.Keys) != 1 {
		return false
	}
	return isIntCol(lastCol)
}

// ShouldBuildClusteredIndex is used to determine whether the CREATE TABLE statement should build a clustered index table.
func ShouldBuildClusteredIndex(mode vardef.ClusteredIndexDefMode, opt *ast.IndexOption, isSingleIntPK bool) bool {
	if opt == nil || opt.PrimaryKeyTp == ast.PrimaryKeyTypeDefault {
		switch mode {
		case vardef.ClusteredIndexDefModeOn:
			return true
		case vardef.ClusteredIndexDefModeIntOnly:
			return !config.GetGlobalConfig().AlterPrimaryKey && isSingleIntPK
		default:
			return false
		}
	}
	return opt.PrimaryKeyTp == ast.PrimaryKeyTypeClustered
}

// BuildViewInfo builds a ViewInfo structure from an ast.CreateViewStmt.
func BuildViewInfo(s *ast.CreateViewStmt) (*model.ViewInfo, error) {
	// Always Use `format.RestoreNameBackQuotes` to restore `SELECT` statement despite the `ANSI_QUOTES` SQL Mode is enabled or not.
	restoreFlag := format.RestoreStringSingleQuotes | format.RestoreKeyWordUppercase | format.RestoreNameBackQuotes
	var sb strings.Builder
	if err := s.Select.Restore(format.NewRestoreCtx(restoreFlag, &sb)); err != nil {
		return nil, err
	}

	return &model.ViewInfo{Definer: s.Definer, Algorithm: s.Algorithm,
		Security: s.Security, SelectStmt: sb.String(), CheckOption: s.CheckOption, Cols: nil}, nil
}

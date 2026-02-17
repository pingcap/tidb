// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/filter"
)

// AddColumn will add a new column to the table.
func (e *executor) AddColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterGetSchemaAndTableByIdent", ctx)
	tbInfo := t.Meta()
	if err = checkAddColumnTooManyColumns(len(t.Cols()) + 1); err != nil {
		return errors.Trace(err)
	}
	col, err := checkAndCreateNewColumn(ctx, ti, schema, spec, t, specNewColumn)
	if err != nil {
		return errors.Trace(err)
	}
	// Added column has existed and if_not_exists flag is true.
	if col == nil {
		return nil
	}
	err = CheckAfterPositionExists(tbInfo, spec.Position)
	if err != nil {
		return errors.Trace(err)
	}

	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	bdrRole, err := meta.NewMutator(txn).GetBDRRole()
	if err != nil {
		return errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) && deniedByBDRWhenAddColumn(specNewColumn.Options) && !filter.IsSystemSchema(schema.Name.L) {
		return dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tbInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tbInfo.Name.L,
		Type:           model.ActionAddColumn,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.TableColumnArgs{
		Col:                col.ColumnInfo,
		Pos:                spec.Position,
		IgnoreExistenceErr: spec.IfNotExists,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}


// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (e *executor) DropColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}
	failpoint.InjectCall("afterGetSchemaAndTableByIdent", ctx)

	isDropable, err := checkIsDroppableColumn(ctx, e.infoCache.GetLatest(), schema, t, spec)
	if err != nil {
		return err
	}
	if !isDropable {
		return nil
	}
	colName := spec.OldColumnName.Name
	err = checkVisibleColumnCnt(t, 0, 1)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		SchemaState:    model.StatePublic,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionDropColumn,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.TableColumnArgs{
		Col:                &model.ColumnInfo{Name: colName},
		IgnoreExistenceErr: spec.IfExists,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func checkIsDroppableColumn(ctx sessionctx.Context, is infoschema.InfoSchema, schema *model.DBInfo, t table.Table, spec *ast.AlterTableSpec) (isDrapable bool, err error) {
	tblInfo := t.Meta()
	// Check whether dropped column has existed.
	colName := spec.OldColumnName.Name
	col := table.FindCol(t.VisibleCols(), colName.L)
	if col == nil {
		err = dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs(colName)
		if spec.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return false, nil
		}
		return false, err
	}

	if err = isDroppableColumn(tblInfo, colName); err != nil {
		return false, errors.Trace(err)
	}
	if err = checkDropColumnWithPartitionConstraint(t, colName); err != nil {
		return false, errors.Trace(err)
	}
	// Check the column with foreign key.
	err = checkDropColumnWithForeignKeyConstraint(is, schema.Name.L, tblInfo, colName.L)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Check the column with TTL config
	err = checkDropColumnWithTTLConfig(tblInfo, colName.L)
	if err != nil {
		return false, errors.Trace(err)
	}
	// We don't support dropping column with PK handle covered now.
	if col.IsPKHandleColumn(tblInfo) {
		return false, dbterror.ErrUnsupportedPKHandle
	}
	if mysql.HasAutoIncrementFlag(col.GetFlag()) && !ctx.GetSessionVars().AllowRemoveAutoInc {
		return false, dbterror.ErrCantDropColWithAutoInc
	}
	// Check the partial index condition
	err = checkColumnReferencedByPartialCondition(t.Meta(), col.ColumnInfo.Name)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

// checkDropColumnWithPartitionConstraint is used to check the partition constraint of the drop column.
func checkDropColumnWithPartitionConstraint(t table.Table, colName ast.CIStr) error {
	if t.Meta().Partition == nil {
		return nil
	}
	pt, ok := t.(table.PartitionedTable)
	if !ok {
		// Should never happen!
		return errors.Trace(dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(colName.L))
	}
	for _, name := range pt.GetPartitionColumnNames() {
		if strings.EqualFold(name.L, colName.L) {
			return errors.Trace(dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(colName.L))
		}
	}
	return nil
}

func checkVisibleColumnCnt(t table.Table, addCnt, dropCnt int) error {
	tblInfo := t.Meta()
	visibleColumCnt := 0
	for _, column := range tblInfo.Columns {
		if !column.Hidden {
			visibleColumCnt++
		}
	}
	if visibleColumCnt+addCnt > dropCnt {
		return nil
	}
	if len(tblInfo.Columns)-visibleColumCnt > 0 {
		// There are only invisible columns.
		return dbterror.ErrTableMustHaveColumns
	}
	return dbterror.ErrCantRemoveAllFields
}

// checkModifyCharsetAndCollation returns error when the charset or collation is not modifiable.
// needRewriteCollationData is used when trying to modify the collation of a column, it is true when the column is with
// index because index of a string column is collation-aware.
func checkModifyCharsetAndCollation(toCharset, toCollate, origCharset, origCollate string, needRewriteCollationData bool) error {
	if !charset.ValidCharsetAndCollation(toCharset, toCollate) {
		return dbterror.ErrUnknownCharacterSet.GenWithStack("Unknown character set: '%s', collation: '%s'", toCharset, toCollate)
	}

	if needRewriteCollationData && collate.NewCollationEnabled() && !collate.CompatibleCollate(origCollate, toCollate) {
		return dbterror.ErrUnsupportedModifyCollation.GenWithStackByArgs(origCollate, toCollate)
	}

	if (origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8MB4) ||
		(origCharset == charset.CharsetUTF8 && toCharset == charset.CharsetUTF8) ||
		(origCharset == charset.CharsetUTF8MB4 && toCharset == charset.CharsetUTF8MB4) ||
		(origCharset == charset.CharsetLatin1 && toCharset == charset.CharsetUTF8MB4) {
		// TiDB only allow utf8/latin1 to be changed to utf8mb4, or changing the collation when the charset is utf8/utf8mb4/latin1.
		return nil
	}

	if toCharset != origCharset {
		msg := fmt.Sprintf("charset from %s to %s", origCharset, toCharset)
		return dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	if toCollate != origCollate {
		msg := fmt.Sprintf("change collate from %s to %s", origCollate, toCollate)
		return dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs(msg)
	}
	return nil
}

func (e *executor) getModifiableColumnJob(
	ctx context.Context, sctx sessionctx.Context,
	ident ast.Ident, originalColName ast.CIStr, spec *ast.AlterTableSpec,
) (*JobWrapper, error) {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ctx, ident.Schema, ident.Name)
	if err != nil {
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}

	return GetModifiableColumnJob(ctx, sctx, is, ident, originalColName, schema, t, spec)
}

// ChangeColumn renames an existing column and modifies the column's definition,
// currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (e *executor) ChangeColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(spec.OldColumnName.Schema.O) != 0 && ident.Schema.L != spec.OldColumnName.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(spec.OldColumnName.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}
	if len(spec.OldColumnName.Table.O) != 0 && ident.Name.L != spec.OldColumnName.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(spec.OldColumnName.Table.O)
	}

	jobW, err := e.getModifiableColumnJob(ctx, sctx, ident, spec.OldColumnName.Name, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.FastGenByArgs(spec.OldColumnName.Name, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}
	jobW.AddSystemVars(vardef.TiDBEnableDDLAnalyze, getEnableDDLAnalyze(sctx))
	jobW.AddSystemVars(vardef.TiDBAnalyzeVersion, getAnalyzeVersion(sctx))

	err = e.DoDDLJobWrapper(sctx, jobW)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		sctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

// RenameColumn renames an existing column.
func (e *executor) RenameColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	oldColName := spec.OldColumnName.Name
	newColName := spec.NewColumnName.Name

	schema, tbl, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}

	oldCol := table.FindCol(tbl.VisibleCols(), oldColName.L)
	if oldCol == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldColName, ident.Name)
	}
	// check if column can rename with check constraint
	err = IsColumnRenameableWithCheckConstraint(oldCol.Name, tbl.Meta())
	if err != nil {
		return err
	}

	if oldColName.L == newColName.L {
		return nil
	}
	if newColName.L == model.ExtraHandleName.L {
		return dbterror.ErrWrongColumnName.GenWithStackByArgs(newColName.L)
	}

	allCols := tbl.Cols()
	colWithNewNameAlreadyExist := table.FindCol(allCols, newColName.L) != nil
	if colWithNewNameAlreadyExist {
		return infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
	}

	// Check generated expression.
	err = checkModifyColumnWithGeneratedColumnsConstraint(allCols, oldColName)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkDropColumnWithPartitionConstraint(tbl, oldColName)
	if err != nil {
		return errors.Trace(err)
	}

	newCol := oldCol.Clone()
	newCol.Name = newColName
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tbl.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tbl.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	err = initJobReorgMetaFromVariables(e.ctx, job, tbl, ctx)
	if err != nil {
		return err
	}

	args := &model.ModifyColumnArgs{
		Column:        newCol,
		OldColumnName: oldColName,
		Position:      spec.Position,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// ModifyColumn does modification on an existing column, currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (e *executor) ModifyColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}

	originalColName := specNewColumn.Name.Name
	jobW, err := e.getModifiableColumnJob(ctx, sctx, ident, originalColName, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.FastGenByArgs(originalColName, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}
	jobW.AddSystemVars(vardef.TiDBEnableDDLAnalyze, getEnableDDLAnalyze(sctx))
	jobW.AddSystemVars(vardef.TiDBAnalyzeVersion, getAnalyzeVersion(sctx))

	err = e.DoDDLJobWrapper(sctx, jobW)
	// column not exists, but if_exists flags is true, so we ignore this error.
	if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
		sctx.GetSessionVars().StmtCtx.AppendNote(err)
		return nil
	}
	return errors.Trace(err)
}

func (e *executor) AlterColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}
	t, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name)
	}

	colName := specNewColumn.Name.Name
	// Check whether alter column has existed.
	oldCol := table.FindCol(t.Cols(), colName.L)
	if oldCol == nil {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, ident.Name)
	}
	col := table.ToColumn(oldCol.Clone())

	// Clean the NoDefaultValueFlag value.
	col.DelFlag(mysql.NoDefaultValueFlag)
	col.DefaultIsExpr = false
	if len(specNewColumn.Options) == 0 {
		err = col.SetDefaultValue(nil)
		if err != nil {
			return errors.Trace(err)
		}
		col.AddFlag(mysql.NoDefaultValueFlag)
	} else {
		if IsAutoRandomColumnID(t.Meta(), col.ID) {
			return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
		hasDefaultValue, err := SetDefaultValue(ctx.GetExprCtx(), col, specNewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
		if err = checkDefaultValue(ctx.GetExprCtx(), col, hasDefaultValue); err != nil {
			return errors.Trace(err)
		}
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionSetDefaultValue,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.SetDefaultValueArgs{
		Col: col.ColumnInfo,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package schematracker

import (
	"context"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/syncer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	pumpcli "github.com/pingcap/tidb/pkg/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var _ ddl.DDL = SchemaTracker{}

// SchemaTracker is used to track schema changes by DM. It implements DDL interface and by applying DDL, it updates the
// table structure to keep tracked with upstream changes.
// It embeds an InfoStore which stores DBInfo and TableInfo. The DBInfo and TableInfo can be treated as immutable, so
// after reading them by SchemaByName or TableByName, later modifications made by SchemaTracker will not change them.
// SchemaTracker is not thread-safe.
type SchemaTracker struct {
	*InfoStore
}

// NewSchemaTracker creates a SchemaTracker. lowerCaseTableNames has the same meaning as MySQL variable lower_case_table_names.
func NewSchemaTracker(lowerCaseTableNames int) SchemaTracker {
	return SchemaTracker{
		InfoStore: NewInfoStore(lowerCaseTableNames),
	}
}

// CreateSchema implements the DDL interface.
func (d SchemaTracker) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error {
	// we only consider explicit charset/collate, if not found, fallback to default charset/collate.
	charsetOpt := ast.CharsetOpt{}
	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			charsetOpt.Chs = val.Value
		case ast.DatabaseOptionCollate:
			charsetOpt.Col = val.Value
		}
	}

	var sessVars *variable.SessionVars
	if ctx != nil {
		sessVars = ctx.GetSessionVars()
	}
	chs, coll, err := ddl.ResolveCharsetCollation(sessVars, charsetOpt)
	if err != nil {
		return errors.Trace(err)
	}

	dbInfo := &model.DBInfo{Name: stmt.Name, Charset: chs, Collate: coll}
	onExist := ddl.OnExistError
	if stmt.IfNotExists {
		onExist = ddl.OnExistIgnore
	}
	return d.CreateSchemaWithInfo(ctx, dbInfo, onExist)
}

// CreateTestDB creates the `test` database, which is the default behavior of TiDB.
func (d SchemaTracker) CreateTestDB(ctx sessionctx.Context) {
	_ = d.CreateSchema(ctx, &ast.CreateDatabaseStmt{
		Name: model.NewCIStr("test"),
	})
}

// CreateSchemaWithInfo implements the DDL interface.
func (d SchemaTracker) CreateSchemaWithInfo(_ sessionctx.Context, dbInfo *model.DBInfo, onExist ddl.OnExist) error {
	oldInfo := d.SchemaByName(dbInfo.Name)
	if oldInfo != nil {
		if onExist == ddl.OnExistIgnore {
			return nil
		}
		// not support MariaDB's CREATE OR REPLACE SCHEMA
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	d.PutSchema(dbInfo)
	return nil
}

// AlterSchema implements the DDL interface.
func (d SchemaTracker) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) (err error) {
	dbInfo := d.SchemaByName(stmt.Name)
	if dbInfo == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(stmt.Name.O)
	}

	dbInfo = dbInfo.Clone()
	defer func() {
		if err == nil {
			d.PutSchema(dbInfo)
		}
	}()

	// Resolve target charset and collation from options.
	var (
		toCharset, toCollate string
	)

	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			if toCharset == "" {
				toCharset = val.Value
			} else if toCharset != val.Value {
				return dbterror.ErrConflictingDeclarations.GenWithStackByArgs(toCharset, val.Value)
			}
		case ast.DatabaseOptionCollate:
			info, errGetCollate := collate.GetCollationByName(val.Value)
			if errGetCollate != nil {
				return errors.Trace(errGetCollate)
			}
			if toCharset == "" {
				toCharset = info.CharsetName
			} else if toCharset != info.CharsetName {
				return dbterror.ErrConflictingDeclarations.GenWithStackByArgs(toCharset, info.CharsetName)
			}
			toCollate = info.Name
		}
	}
	if toCollate == "" {
		if toCollate, err = ddl.GetDefaultCollation(ctx.GetSessionVars(), toCharset); err != nil {
			return errors.Trace(err)
		}
	}

	dbInfo.Charset = toCharset
	dbInfo.Collate = toCollate

	return nil
}

// DropSchema implements the DDL interface.
func (d SchemaTracker) DropSchema(_ sessionctx.Context, stmt *ast.DropDatabaseStmt) error {
	ok := d.DeleteSchema(stmt.Name)
	if !ok {
		if stmt.IfExists {
			return nil
		}
		return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
	}
	return nil
}

// CreateTable implements the DDL interface.
func (d SchemaTracker) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	schema := d.SchemaByName(ident.Schema)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	// suppress ErrTooLongKey
	ctx.SetValue(ddl.SuppressErrorTooLongKeyKey, true)
	// support drop PK
	enableClusteredIndexBackup := ctx.GetSessionVars().EnableClusteredIndex
	ctx.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOff
	defer func() {
		ctx.ClearValue(ddl.SuppressErrorTooLongKeyKey)
		ctx.GetSessionVars().EnableClusteredIndex = enableClusteredIndexBackup
	}()

	var (
		referTbl *model.TableInfo
		err      error
	)
	if s.ReferTable != nil {
		referTbl, err = d.TableByName(s.ReferTable.Schema, s.ReferTable.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(s.ReferTable.Schema, s.ReferTable.Name)
		}
	}

	// build tableInfo
	var (
		tbInfo *model.TableInfo
	)
	if s.ReferTable != nil {
		tbInfo, err = ddl.BuildTableInfoWithLike(ctx, ident, referTbl, s)
	} else {
		tbInfo, err = ddl.BuildTableInfoWithStmt(ctx, s, schema.Charset, schema.Collate, nil)
	}
	if err != nil {
		return errors.Trace(err)
	}

	// TODO: to reuse the constant fold of expression in partition range definition we use CheckTableInfoValidWithStmt,
	// but it may also introduce unwanted limit check in DM's use case. Should check it later.
	if err = ddl.CheckTableInfoValidWithStmt(ctx, tbInfo, s); err != nil {
		return err
	}

	onExist := ddl.OnExistError
	if s.IfNotExists {
		onExist = ddl.OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, schema.Name, tbInfo, onExist)
}

// CreateTableWithInfo implements the DDL interface.
func (d SchemaTracker) CreateTableWithInfo(
	_ sessionctx.Context,
	dbName model.CIStr,
	info *model.TableInfo,
	cs ...ddl.CreateTableWithInfoConfigurier,
) error {
	c := ddl.GetCreateTableWithInfoConfig(cs)

	schema := d.SchemaByName(dbName)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	oldTable, _ := d.TableByName(dbName, info.Name)
	if oldTable != nil {
		switch c.OnExist {
		case ddl.OnExistIgnore:
			return nil
		case ddl.OnExistReplace:
			return d.PutTable(dbName, info)
		default:
			return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: dbName, Name: info.Name})
		}
	}

	return d.PutTable(dbName, info)
}

// CreateView implements the DDL interface.
func (d SchemaTracker) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) error {
	viewInfo, err := ddl.BuildViewInfo(ctx, s)
	if err != nil {
		return err
	}

	cols := make([]*table.Column, len(s.Cols))
	for i, v := range s.Cols {
		cols[i] = table.ToColumn(&model.ColumnInfo{
			Name:   v,
			ID:     int64(i),
			Offset: i,
			State:  model.StatePublic,
		})
	}

	tbInfo, err := ddl.BuildTableInfo(ctx, s.ViewName.Name, cols, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := ddl.OnExistError
	if s.OrReplace {
		onExist = ddl.OnExistReplace
	}

	return d.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, onExist)
}

// DropTable implements the DDL interface.
func (d SchemaTracker) DropTable(_ sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	notExistTables := make([]string, 0, len(stmt.Tables))
	for _, name := range stmt.Tables {
		tb, err := d.TableByName(name.Schema, name.Name)
		if err != nil || !tb.IsBaseTable() {
			if stmt.IfExists {
				continue
			}

			id := ast.Ident{Schema: name.Schema, Name: name.Name}
			notExistTables = append(notExistTables, id.String())
			// For statement dropping multiple tables, we should return error after try to drop all tables.
			continue
		}

		// Without IF EXISTS, the statement drops all named tables that do exist, and returns an error indicating which
		// nonexisting tables it was unable to drop.
		// https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
		_ = d.DeleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

// RecoverTable implements the DDL interface, which is no-op in DM's case.
func (SchemaTracker) RecoverTable(_ sessionctx.Context, _ *ddl.RecoverInfo) (err error) {
	return nil
}

// FlashbackCluster implements the DDL interface, which is no-op in DM's case.
func (SchemaTracker) FlashbackCluster(_ sessionctx.Context, _ uint64) (err error) {
	return nil
}

// RecoverSchema implements the DDL interface, which is no-op in DM's case.
func (SchemaTracker) RecoverSchema(_ sessionctx.Context, _ *ddl.RecoverSchemaInfo) (err error) {
	return nil
}

// DropView implements the DDL interface.
func (d SchemaTracker) DropView(_ sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	notExistTables := make([]string, 0, len(stmt.Tables))
	for _, name := range stmt.Tables {
		tb, err := d.TableByName(name.Schema, name.Name)
		if err != nil {
			if stmt.IfExists {
				continue
			}

			id := ast.Ident{Schema: name.Schema, Name: name.Name}
			notExistTables = append(notExistTables, id.String())
			continue
		}

		// the behaviour is fast fail when type is wrong.
		if !tb.IsView() {
			return dbterror.ErrWrongObject.GenWithStackByArgs(name.Schema, name.Name, "VIEW")
		}

		_ = d.DeleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

// CreateIndex implements the DDL interface.
func (d SchemaTracker) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	return d.createIndex(ctx, ident, stmt.KeyType, model.NewCIStr(stmt.IndexName),
		stmt.IndexPartSpecifications, stmt.IndexOption, stmt.IfNotExists)
}

func (d SchemaTracker) putTableIfNoError(err error, dbName model.CIStr, tbInfo *model.TableInfo) {
	if err != nil {
		return
	}
	_ = d.PutTable(dbName, tbInfo)
}

// createIndex is shared by CreateIndex and AlterTable.
func (d SchemaTracker) createIndex(
	ctx sessionctx.Context,
	ti ast.Ident,
	keyType ast.IndexKeyType,
	indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
	ifNotExists bool,
) (err error) {
	unique := keyType == ast.IndexKeyTypeUnique
	tblInfo, err := d.TableClonedByName(ti.Schema, ti.Name)
	if err != nil {
		return err
	}

	defer d.putTableIfNoError(err, ti.Schema, tblInfo)

	t := tables.MockTableFromMeta(tblInfo)

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		colName := model.NewCIStr("expression_index")
		if indexPartSpecifications[0].Column != nil {
			colName = indexPartSpecifications[0].Column.Name
		}
		indexName = ddl.GetName4AnonymousIndex(t, colName, model.NewCIStr(""))
	}

	if indexInfo := tblInfo.FindIndexByName(indexName.L); indexInfo != nil {
		if ifNotExists {
			return nil
		}
		return dbterror.ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	hiddenCols, err := ddl.BuildHiddenColumnInfo(ctx, indexPartSpecifications, indexName, t.Meta(), t.Cols())
	if err != nil {
		return err
	}
	finalColumns := make([]*model.ColumnInfo, len(tblInfo.Columns), len(tblInfo.Columns)+len(hiddenCols))
	copy(finalColumns, tblInfo.Columns)
	finalColumns = append(finalColumns, hiddenCols...)

	for _, hiddenCol := range hiddenCols {
		ddl.InitAndAddColumnToTable(tblInfo, hiddenCol)
	}

	indexInfo, err := ddl.BuildIndexInfo(
		ctx,
		finalColumns,
		indexName,
		false,
		unique,
		false,
		indexPartSpecifications,
		indexOption,
		model.StatePublic,
	)
	if err != nil {
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(hiddenCols)]
		return err
	}
	indexInfo.ID = ddl.AllocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)

	ddl.AddIndexColumnFlag(tblInfo, indexInfo)
	return nil
}

// DropIndex implements the DDL interface.
func (d SchemaTracker) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	err := d.dropIndex(ctx, ti, model.NewCIStr(stmt.IndexName), stmt.IfExists)
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && stmt.IfExists {
		err = nil
	}
	return err
}

// dropIndex is shared by DropIndex and AlterTable.
func (d SchemaTracker) dropIndex(_ sessionctx.Context, ti ast.Ident, indexName model.CIStr, ifExists bool) (err error) {
	tblInfo, err := d.TableClonedByName(ti.Schema, ti.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}

	defer d.putTableIfNoError(err, ti.Schema, tblInfo)

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		if ifExists {
			return nil
		}
		return dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.Name.L != indexInfo.Name.L {
			newIndices = append(newIndices, idx)
		}
	}
	tblInfo.Indices = newIndices
	ddl.DropIndexColumnFlag(tblInfo, indexInfo)
	ddl.RemoveDependentHiddenColumns(tblInfo, indexInfo)
	return nil
}

// addColumn is used by AlterTable.
func (d SchemaTracker) addColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) (err error) {
	specNewColumn := spec.NewColumns[0]
	schema := d.SchemaByName(ti.Schema)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}
	tblInfo, err := d.TableClonedByName(ti.Schema, ti.Name)
	if err != nil {
		return err
	}

	defer d.putTableIfNoError(err, ti.Schema, tblInfo)

	t := tables.MockTableFromMeta(tblInfo)
	colName := specNewColumn.Name.Name.O

	if col := table.FindCol(t.Cols(), colName); col != nil {
		if spec.IfNotExists {
			return nil
		}
		return infoschema.ErrColumnExists.GenWithStackByArgs(colName)
	}

	col, err := ddl.CreateNewColumn(ctx, schema, spec, t, specNewColumn)
	if err != nil {
		return errors.Trace(err)
	}
	err = ddl.CheckAfterPositionExists(tblInfo, spec.Position)
	if err != nil {
		return errors.Trace(err)
	}

	columnInfo := ddl.InitAndAddColumnToTable(tblInfo, col.ColumnInfo)
	offset, err := ddl.LocateOffsetToMove(columnInfo.Offset, spec.Position, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo.MoveColumnInfo(columnInfo.Offset, offset)
	columnInfo.State = model.StatePublic
	return nil
}

// dropColumn is used by AlterTable.
func (d *SchemaTracker) dropColumn(_ sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) (err error) {
	tblInfo, err := d.TableClonedByName(ti.Schema, ti.Name)
	if err != nil {
		return err
	}

	defer d.putTableIfNoError(err, ti.Schema, tblInfo)

	colName := spec.OldColumnName.Name
	colInfo := tblInfo.FindPublicColumnByName(colName.L)
	if colInfo == nil {
		if spec.IfExists {
			return nil
		}
		return dbterror.ErrCantDropFieldOrKey.GenWithStackByArgs(colName)
	}
	if len(tblInfo.Columns) == 1 {
		return dbterror.ErrCantRemoveAllFields.GenWithStack("can't drop only column %s in table %s",
			colName, tblInfo.Name)
	}

	// do drop column
	tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
	tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]

	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		i, _ := model.FindIndexColumnByName(idx.Columns, colName.L)
		if i == -1 {
			newIndices = append(newIndices, idx)
			continue
		}

		idx.Columns = append(idx.Columns[:i], idx.Columns[i+1:]...)
		if len(idx.Columns) == 0 {
			continue
		}
		newIndices = append(newIndices, idx)
	}
	tblInfo.Indices = newIndices
	return nil
}

// renameColumn is used by AlterTable.
func (d SchemaTracker) renameColumn(_ sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	oldColName := spec.OldColumnName.Name
	newColName := spec.NewColumnName.Name

	tblInfo, err := d.TableClonedByName(ident.Schema, ident.Name)
	if err != nil {
		return err
	}

	defer d.putTableIfNoError(err, ident.Schema, tblInfo)

	tbl := tables.MockTableFromMeta(tblInfo)

	oldCol := table.FindCol(tbl.VisibleCols(), oldColName.L)
	if oldCol == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldColName, ident.Name)
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
	for _, col := range allCols {
		if col.GeneratedExpr == nil {
			continue
		}
		dependedColNames := ddl.FindColumnNamesInExpr(col.GeneratedExpr.Internal())
		for _, name := range dependedColNames {
			if name.Name.L == oldColName.L {
				if col.Hidden {
					return dbterror.ErrDependentByFunctionalIndex.GenWithStackByArgs(oldColName.O)
				}
				return dbterror.ErrDependentByGeneratedColumn.GenWithStackByArgs(oldColName.O)
			}
		}
	}

	oldCol.Name = newColName
	return nil
}

// alterColumn is used by AlterTable.
func (d SchemaTracker) alterColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	specNewColumn := spec.NewColumns[0]
	tblInfo, err := d.TableClonedByName(ident.Schema, ident.Name)
	if err != nil {
		return err
	}

	defer d.putTableIfNoError(err, ident.Schema, tblInfo)

	t := tables.MockTableFromMeta(tblInfo)

	colName := specNewColumn.Name.Name
	// Check whether alter column has existed.
	oldCol := table.FindCol(t.Cols(), colName.L)
	if oldCol == nil {
		return dbterror.ErrBadField.GenWithStackByArgs(colName, ident.Name)
	}

	// Clean the NoDefaultValueFlag value.
	oldCol.DelFlag(mysql.NoDefaultValueFlag)
	if len(specNewColumn.Options) == 0 {
		oldCol.DefaultIsExpr = false
		err = oldCol.SetDefaultValue(nil)
		if err != nil {
			return errors.Trace(err)
		}
		oldCol.AddFlag(mysql.NoDefaultValueFlag)
	} else {
		_, err := ddl.SetDefaultValue(ctx, oldCol, specNewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// modifyColumn is used by AlterTable.
func (d SchemaTracker) modifyColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	if len(specNewColumn.Name.Schema.O) != 0 && ident.Schema.L != specNewColumn.Name.Schema.L {
		return dbterror.ErrWrongDBName.GenWithStackByArgs(specNewColumn.Name.Schema.O)
	}
	if len(specNewColumn.Name.Table.O) != 0 && ident.Name.L != specNewColumn.Name.Table.L {
		return dbterror.ErrWrongTableName.GenWithStackByArgs(specNewColumn.Name.Table.O)
	}
	return d.handleModifyColumn(ctx, sctx, ident, specNewColumn.Name.Name, spec)
}

// changeColumn is used by AlterTable.
func (d SchemaTracker) changeColumn(ctx context.Context, sctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
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
	return d.handleModifyColumn(ctx, sctx, ident, spec.OldColumnName.Name, spec)
}

func (d SchemaTracker) handleModifyColumn(
	ctx context.Context,
	sctx sessionctx.Context,
	ident ast.Ident,
	originalColName model.CIStr,
	spec *ast.AlterTableSpec,
) (err error) {
	tblInfo, err := d.TableClonedByName(ident.Schema, ident.Name)
	if err != nil {
		return err
	}

	defer d.putTableIfNoError(err, ident.Schema, tblInfo)

	schema := d.SchemaByName(ident.Schema)
	t := tables.MockTableFromMeta(tblInfo)
	job, err := ddl.GetModifiableColumnJob(ctx, sctx, nil, ident, originalColName, schema, t, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.FastGenByArgs(originalColName, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	newColInfo := *job.Args[0].(**model.ColumnInfo)
	updatedAutoRandomBits := job.Args[4].(uint64)

	tblInfo.AutoRandomBits = updatedAutoRandomBits
	oldCol := table.FindCol(t.Cols(), originalColName.L).ColumnInfo

	originDefVal, err := ddl.GetOriginDefaultValueForModifyColumn(sctx.GetExprCtx(), newColInfo, oldCol)
	if err != nil {
		return errors.Trace(err)
	}
	if err = newColInfo.SetOriginDefaultValue(originDefVal); err != nil {
		return errors.Trace(err)
	}

	// replace old column and its related index column in-place.
	newColInfo.ID = ddl.AllocateColumnID(tblInfo)
	newColInfo.Offset = oldCol.Offset
	tblInfo.Columns[oldCol.Offset] = newColInfo
	indexesToChange := ddl.FindRelatedIndexesToChange(tblInfo, oldCol.Name)
	for _, info := range indexesToChange {
		ddl.SetIdxColNameOffset(info.IndexInfo.Columns[info.Offset], newColInfo)
	}

	destOffset, err := ddl.LocateOffsetToMove(newColInfo.Offset, spec.Position, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo.MoveColumnInfo(newColInfo.Offset, destOffset)

	newColInfo.State = model.StatePublic
	return nil
}

// renameIndex is used by AlterTable.
func (d SchemaTracker) renameIndex(_ sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	tblInfo, err := d.TableClonedByName(ident.Schema, ident.Name)
	if err != nil {
		return err
	}

	defer d.putTableIfNoError(err, ident.Schema, tblInfo)

	duplicate, err := ddl.ValidateRenameIndex(spec.FromKey, spec.ToKey, tblInfo)
	if duplicate {
		return nil
	}
	if err != nil {
		return err
	}
	idx := tblInfo.FindIndexByName(spec.FromKey.L)
	idx.Name = spec.ToKey
	return nil
}

// addTablePartitions is used by AlterTable.
func (d SchemaTracker) addTablePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	tblInfo, err := d.TableClonedByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}

	defer d.putTableIfNoError(err, ident.Schema, tblInfo)

	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partInfo, err := ddl.BuildAddedPartitionInfo(ctx.GetExprCtx(), tblInfo, spec)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo.Partition.Definitions = append(tblInfo.Partition.Definitions, partInfo.Definitions...)
	return nil
}

// dropTablePartitions is used by AlterTable.
func (d SchemaTracker) dropTablePartitions(_ sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	tblInfo, err := d.TableClonedByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}

	defer d.putTableIfNoError(err, ident.Schema, tblInfo)

	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partNames := make([]string, len(spec.PartitionNames))
	for i, partCIName := range spec.PartitionNames {
		partNames[i] = partCIName.L
	}
	err = ddl.CheckDropTablePartition(tblInfo, partNames)
	if err != nil {
		if dbterror.ErrDropPartitionNonExistent.Equal(err) && spec.IfExists {
			return nil
		}
		return errors.Trace(err)
	}

	newDefs := make([]model.PartitionDefinition, 0, len(tblInfo.Partition.Definitions)-len(partNames))
	for _, def := range tblInfo.Partition.Definitions {
		found := false
		for _, partName := range partNames {
			if def.Name.L == partName {
				found = true
				break
			}
		}
		if !found {
			newDefs = append(newDefs, def)
		}
	}
	tblInfo.Partition.Definitions = newDefs
	return nil
}

// createPrimaryKey is used by AlterTable.
func (d SchemaTracker) createPrimaryKey(
	ctx sessionctx.Context,
	ti ast.Ident,
	indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
) (err error) {
	tblInfo, err := d.TableClonedByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(err)
	}

	defer d.putTableIfNoError(err, ti.Schema, tblInfo)

	indexName = model.NewCIStr(mysql.PrimaryKeyName)
	if indexInfo := tblInfo.FindIndexByName(indexName.L); indexInfo != nil ||
		// If the table's PKIsHandle is true, it also means that this table has a primary key.
		tblInfo.PKIsHandle {
		return infoschema.ErrMultiplePriKey
	}

	// Primary keys cannot include expression index parts. A primary key requires the generated column to be stored,
	// but expression index parts are implemented as virtual generated columns, not stored generated columns.
	for _, idxPart := range indexPartSpecifications {
		if idxPart.Expr != nil {
			return dbterror.ErrFunctionalIndexPrimaryKey
		}
	}

	if _, err = ddl.CheckPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
		return err
	}

	indexInfo, err := ddl.BuildIndexInfo(
		ctx,
		tblInfo.Columns,
		indexName,
		true,
		true,
		false,
		indexPartSpecifications,
		indexOption,
		model.StatePublic,
	)
	if err != nil {
		return errors.Trace(err)
	}
	indexInfo.ID = ddl.AllocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	// Set column index flag.
	ddl.AddIndexColumnFlag(tblInfo, indexInfo)
	if err = ddl.UpdateColsNull2NotNull(tblInfo, indexInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// AlterTable implements the DDL interface.
func (d SchemaTracker) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) (err error) {
	validSpecs, err := ddl.ResolveAlterTableSpec(sctx, stmt.Specs)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO: reorder specs to follow MySQL's order, drop index -> drop column -> rename column -> add column -> add index
	// https://github.com/mysql/mysql-server/blob/8d8c986e5716e38cb776b627a8eee9e92241b4ce/sql/sql_table.cc#L16698-L16714

	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	tblInfo, err := d.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}

	// atomicity for multi-schema change
	oldTblInfo := tblInfo.Clone()
	defer func() {
		if err != nil {
			_ = d.PutTable(ident.Schema, oldTblInfo)
		}
	}()

	for _, spec := range validSpecs {
		var handledCharsetOrCollate bool
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			err = d.addColumn(sctx, ident, spec)
		case ast.AlterTableAddPartitions:
			err = d.addTablePartitions(sctx, ident, spec)
		case ast.AlterTableDropColumn:
			err = d.dropColumn(sctx, ident, spec)
		case ast.AlterTableDropIndex:
			err = d.dropIndex(sctx, ident, model.NewCIStr(spec.Name), spec.IfExists)
		case ast.AlterTableDropPrimaryKey:
			err = d.dropIndex(sctx, ident, model.NewCIStr(mysql.PrimaryKeyName), spec.IfExists)
		case ast.AlterTableRenameIndex:
			err = d.renameIndex(sctx, ident, spec)
		case ast.AlterTableDropPartition:
			err = d.dropTablePartitions(sctx, ident, spec)
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = d.createIndex(sctx, ident, ast.IndexKeyTypeNone, model.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, constr.IfNotExists)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = d.createIndex(sctx, ident, ast.IndexKeyTypeUnique, model.NewCIStr(constr.Name),
					spec.Constraint.Keys, constr.Option, false) // IfNotExists should be not applied
			case ast.ConstraintPrimaryKey:
				err = d.createPrimaryKey(sctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, constr.Option)
			case ast.ConstraintForeignKey,
				ast.ConstraintFulltext,
				ast.ConstraintCheck:
			default:
				// Nothing to do now.
			}
		case ast.AlterTableModifyColumn:
			err = d.modifyColumn(ctx, sctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = d.changeColumn(ctx, sctx, ident, spec)
		case ast.AlterTableRenameColumn:
			err = d.renameColumn(sctx, ident, spec)
		case ast.AlterTableAlterColumn:
			err = d.alterColumn(sctx, ident, spec)
		case ast.AlterTableRenameTable:
			newIdent := ast.Ident{Schema: spec.NewTable.Schema, Name: spec.NewTable.Name}
			err = d.renameTable(sctx, []ast.Ident{ident}, []ast.Ident{newIdent}, true)
		case ast.AlterTableOption:
			for i, opt := range spec.Options {
				switch opt.Tp {
				case ast.TableOptionShardRowID:
				case ast.TableOptionAutoIncrement:
				case ast.TableOptionAutoIdCache:
				case ast.TableOptionAutoRandomBase:
				case ast.TableOptionComment:
					tblInfo = tblInfo.Clone()
					tblInfo.Comment = opt.StrValue
					_ = d.PutTable(ident.Schema, tblInfo)
				case ast.TableOptionCharset, ast.TableOptionCollate:
					// getCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						continue
					}
					var toCharset, toCollate string
					toCharset, toCollate, err = ddl.GetCharsetAndCollateInTableOption(sctx.GetSessionVars(), i, spec.Options)
					if err != nil {
						return err
					}
					needsOverwriteCols := ddl.NeedToOverwriteColCharset(spec.Options)

					tblInfo = tblInfo.Clone()
					if toCharset != "" {
						tblInfo.Charset = toCharset
					}
					if toCollate != "" {
						tblInfo.Collate = toCollate
					}
					if needsOverwriteCols {
						// update column charset.
						for _, col := range tblInfo.Columns {
							if field_types.HasCharset(&col.FieldType) {
								col.SetCharset(toCharset)
								col.SetCollate(toCollate)
							} else {
								col.SetCharset(charset.CharsetBin)
								col.SetCollate(charset.CharsetBin)
							}
						}
					}
					_ = d.PutTable(ident.Schema, tblInfo)

					handledCharsetOrCollate = true
				case ast.TableOptionPlacementPolicy:
				case ast.TableOptionEngine:
				default:
					err = dbterror.ErrUnsupportedAlterTableOption
				}

				if err != nil {
					return errors.Trace(err)
				}
			}
		case ast.AlterTableIndexInvisible:
			tblInfo = tblInfo.Clone()
			idx := tblInfo.FindIndexByName(spec.IndexName.L)
			if idx == nil {
				return errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(spec.IndexName.O, ident.Name))
			}
			idx.Invisible = spec.Visibility == ast.IndexVisibilityInvisible
			_ = d.PutTable(ident.Schema, tblInfo)
		case ast.AlterTablePartitionOptions,
			ast.AlterTableDropForeignKey,
			ast.AlterTableCoalescePartitions,
			ast.AlterTableReorganizePartition,
			ast.AlterTableCheckPartitions,
			ast.AlterTableRebuildPartition,
			ast.AlterTableOptimizePartition,
			ast.AlterTableRemovePartitioning,
			ast.AlterTableRepairPartition,
			ast.AlterTableTruncatePartition,
			ast.AlterTableWriteable,
			ast.AlterTableExchangePartition,
			ast.AlterTablePartition,
			ast.AlterTableSetTiFlashReplica,
			ast.AlterTableOrderByColumns,
			ast.AlterTableAlterCheck,
			ast.AlterTableDropCheck,
			ast.AlterTableWithValidation,
			ast.AlterTableWithoutValidation,
			ast.AlterTableAddStatistics,
			ast.AlterTableDropStatistics,
			ast.AlterTableAttributes,
			ast.AlterTablePartitionAttributes,
			ast.AlterTableCache,
			ast.AlterTableNoCache:
		default:
			// Nothing to do now.
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	return err
}

// TruncateTable implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) TruncateTable(_ sessionctx.Context, _ ast.Ident) error {
	return nil
}

// RenameTable implements the DDL interface.
func (d SchemaTracker) RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error {
	oldIdents := make([]ast.Ident, 0, len(stmt.TableToTables))
	newIdents := make([]ast.Ident, 0, len(stmt.TableToTables))
	for _, tablePair := range stmt.TableToTables {
		oldIdent := ast.Ident{Schema: tablePair.OldTable.Schema, Name: tablePair.OldTable.Name}
		newIdent := ast.Ident{Schema: tablePair.NewTable.Schema, Name: tablePair.NewTable.Name}
		oldIdents = append(oldIdents, oldIdent)
		newIdents = append(newIdents, newIdent)
	}
	return d.renameTable(ctx, oldIdents, newIdents, false)
}

// renameTable is used by RenameTable and AlterTable.
func (d SchemaTracker) renameTable(_ sessionctx.Context, oldIdents, newIdents []ast.Ident, isAlterTable bool) error {
	tablesCache := make(map[string]int64)
	is := InfoStoreAdaptor{inner: d.InfoStore}
	for i := range oldIdents {
		schema, _, err := ddl.ExtractTblInfos(is, oldIdents[i], newIdents[i], isAlterTable, tablesCache)
		if err != nil {
			return err
		}

		// no-op for ALTER TABLE RENAME t1 TO T1
		if schema == nil && isAlterTable {
			return nil
		}
	}

	for i := range oldIdents {
		tableInfo, err := d.TableByName(oldIdents[i].Schema, oldIdents[i].Name)
		if err != nil {
			return err
		}
		if err = d.DeleteTable(oldIdents[i].Schema, oldIdents[i].Name); err != nil {
			return err
		}
		tableInfo.Name = newIdents[i].Name
		if err = d.PutTable(newIdents[i].Schema, tableInfo); err != nil {
			return err
		}
	}
	return nil
}

// LockTables implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) LockTables(_ sessionctx.Context, _ *ast.LockTablesStmt) error {
	return nil
}

// UnlockTables implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) UnlockTables(_ sessionctx.Context, _ []model.TableLockTpInfo) error {
	return nil
}

// CleanupTableLock implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) CleanupTableLock(_ sessionctx.Context, _ []*ast.TableName) error {
	return nil
}

// UpdateTableReplicaInfo implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) UpdateTableReplicaInfo(_ sessionctx.Context, _ int64, _ bool) error {
	return nil
}

// RepairTable implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) RepairTable(_ sessionctx.Context, _ *ast.CreateTableStmt) error {
	return nil
}

// CreateSequence implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) CreateSequence(_ sessionctx.Context, _ *ast.CreateSequenceStmt) error {
	return nil
}

// DropSequence implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) DropSequence(_ sessionctx.Context, _ *ast.DropSequenceStmt) (err error) {
	return nil
}

// AlterSequence implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) AlterSequence(_ sessionctx.Context, _ *ast.AlterSequenceStmt) error {
	return nil
}

// CreatePlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) CreatePlacementPolicy(_ sessionctx.Context, _ *ast.CreatePlacementPolicyStmt) error {
	return nil
}

// DropPlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) DropPlacementPolicy(_ sessionctx.Context, _ *ast.DropPlacementPolicyStmt) error {
	return nil
}

// AlterPlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) AlterPlacementPolicy(_ sessionctx.Context, _ *ast.AlterPlacementPolicyStmt) error {
	return nil
}

// AddResourceGroup implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) AddResourceGroup(_ sessionctx.Context, _ *ast.CreateResourceGroupStmt) error {
	return nil
}

// DropResourceGroup implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) DropResourceGroup(_ sessionctx.Context, _ *ast.DropResourceGroupStmt) error {
	return nil
}

// AlterResourceGroup implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) AlterResourceGroup(_ sessionctx.Context, _ *ast.AlterResourceGroupStmt) error {
	return nil
}

// BatchCreateTableWithInfo implements the DDL interface, it will call CreateTableWithInfo for each table.
func (d SchemaTracker) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	for _, tableInfo := range info {
		if err := d.CreateTableWithInfo(ctx, schema, tableInfo, cs...); err != nil {
			return err
		}
	}
	return nil
}

// CreatePlacementPolicyWithInfo implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) CreatePlacementPolicyWithInfo(_ sessionctx.Context, _ *model.PolicyInfo, _ ddl.OnExist) error {
	return nil
}

// Start implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) Start(_ *pools.ResourcePool) error {
	return nil
}

// GetLease implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) GetLease() time.Duration {
	return 0
}

// Stats implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) Stats(_ *variable.SessionVars) (map[string]any, error) {
	return nil, nil
}

// GetScope implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) GetScope(_ string) variable.ScopeFlag {
	return 0
}

// Stop implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) Stop() error {
	return nil
}

// RegisterStatsHandle implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) RegisterStatsHandle(_ *handle.Handle) {}

// SchemaSyncer implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) SchemaSyncer() syncer.SchemaSyncer {
	return nil
}

// StateSyncer implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) StateSyncer() syncer.StateSyncer {
	return nil
}

// OwnerManager implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) OwnerManager() owner.Manager {
	return nil
}

// GetID implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) GetID() string {
	return "schema-tracker"
}

// GetTableMaxHandle implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) GetTableMaxHandle(_ *ddl.JobContext, _ uint64, _ table.PhysicalTable) (kv.Handle, bool, error) {
	return nil, false, nil
}

// SetBinlogClient implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) SetBinlogClient(_ *pumpcli.PumpsClient) {}

// GetHook implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) GetHook() ddl.Callback {
	return nil
}

// SetHook implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) SetHook(_ ddl.Callback) {}

// GetInfoSchemaWithInterceptor implements the DDL interface.
func (SchemaTracker) GetInfoSchemaWithInterceptor(_ sessionctx.Context) infoschema.InfoSchema {
	panic("not implemented")
}

// DoDDLJob implements the DDL interface, it's no-op in DM's case.
func (SchemaTracker) DoDDLJob(_ sessionctx.Context, _ *model.Job) error {
	return nil
}

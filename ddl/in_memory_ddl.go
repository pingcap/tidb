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

package ddl

import (
	"context"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	field_types "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/types"

	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/util/dbterror"
)

// infoStore is a simple structure that stores DBInfo and TableInfo. It's not thread-safe.
type infoStore struct {
	lowerCaseTableNames int // same as variable lower_case_table_names

	dbs    map[string]*model.DBInfo
	tables map[string]map[string]*model.TableInfo
}

func newInfoStore(lowerCaseTableNames int) *infoStore {
	return &infoStore{
		lowerCaseTableNames: lowerCaseTableNames,
		dbs:                 map[string]*model.DBInfo{},
		tables:              map[string]map[string]*model.TableInfo{},
	}
}

func (i *infoStore) ciStr2Key(name model.CIStr) string {
	if i.lowerCaseTableNames == 0 {
		return name.O
	}
	return name.L
}

func (i *infoStore) SchemaByName(name model.CIStr) *model.DBInfo {
	key := i.ciStr2Key(name)
	return i.dbs[key]
}

func (i *infoStore) putSchema(dbInfo *model.DBInfo) {
	key := i.ciStr2Key(dbInfo.Name)
	i.dbs[key] = dbInfo
	if i.tables[key] == nil {
		i.tables[key] = map[string]*model.TableInfo{}
	}
}

func (i *infoStore) deleteSchema(name model.CIStr) bool {
	key := i.ciStr2Key(name)
	_, ok := i.dbs[key]
	if !ok {
		return false
	}
	delete(i.dbs, key)
	delete(i.tables, key)
	return true
}

func (i *infoStore) TableByName(schema, table model.CIStr) (*model.TableInfo, error) {
	schemaKey := i.ciStr2Key(schema)
	tables, ok := i.tables[schemaKey]
	if !ok {
		return nil, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema)
	}

	tableKey := i.ciStr2Key(table)
	tbl, ok := tables[tableKey]
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(schema, table)
	}
	return tbl, nil
}

func (i *infoStore) putTable(schemaName model.CIStr, tblInfo *model.TableInfo) error {
	schemaKey := i.ciStr2Key(schemaName)
	tables, ok := i.tables[schemaKey]
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schemaName)
	}
	tableKey := i.ciStr2Key(tblInfo.Name)
	tables[tableKey] = tblInfo
	return nil
}

func (i *infoStore) deleteTable(schema, table model.CIStr) error {
	schemaKey := i.ciStr2Key(schema)
	tables, ok := i.tables[schemaKey]
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema)
	}

	tableKey := i.ciStr2Key(table)
	_, ok = tables[tableKey]
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(schema, table)
	}
	delete(tables, tableKey)
	return nil
}

type infoSchemaAdaptor struct {
	infoschema.InfoSchema
	inner *infoStore
}

func (i infoSchemaAdaptor) SchemaByName(schema model.CIStr) (*model.DBInfo, bool) {
	dbInfo := i.inner.SchemaByName(schema)
	return dbInfo, dbInfo != nil
}

func (i infoSchemaAdaptor) TableExists(schema, table model.CIStr) bool {
	tableInfo, _ := i.inner.TableByName(schema, table)
	return tableInfo != nil
}

func (i infoSchemaAdaptor) TableIsView(schema, table model.CIStr) bool {
	tableInfo, _ := i.inner.TableByName(schema, table)
	if tableInfo == nil {
		return false
	}
	return tableInfo.IsView()
}

func (i infoSchemaAdaptor) TableByName(schema, table model.CIStr) (t table.Table, err error) {
	tableInfo, err := i.inner.TableByName(schema, table)
	if err != nil {
		return nil, err
	}
	return tables.MockTableFromMeta(tableInfo), nil
}

var _ DDL = SchemaTracker{}

type SchemaTracker struct {
	*infoStore
}

func NewInMemoryDDL(lowerCaseTableNames int) SchemaTracker {
	return SchemaTracker{
		infoStore: newInfoStore(lowerCaseTableNames),
	}
}

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

	chs, coll, err := ResolveCharsetCollation(charsetOpt)
	if err != nil {
		return errors.Trace(err)
	}

	dbInfo := &model.DBInfo{Name: stmt.Name, Charset: chs, Collate: coll}
	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}
	return d.CreateSchemaWithInfo(ctx, dbInfo, onExist)
}

func (d SchemaTracker) CreateSchemaWithInfo(ctx sessionctx.Context, dbInfo *model.DBInfo, onExist OnExist) error {
	oldInfo := d.SchemaByName(dbInfo.Name)
	if oldInfo != nil {
		if onExist == OnExistIgnore {
			return nil
		}
		// not support MariaDB's CREATE OR REPLACE SCHEMA
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	d.putSchema(dbInfo)
	return nil
}

func (d SchemaTracker) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error {
	dbInfo := d.SchemaByName(stmt.Name)
	if dbInfo == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(stmt.Name.O)
	}

	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			dbInfo.Charset = val.Value
		case ast.DatabaseOptionCollate:
			dbInfo.Collate = val.Value
		}
	}

	return nil
}

func (d SchemaTracker) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error {
	ok := d.deleteSchema(stmt.Name)
	if !ok {
		if stmt.IfExists {
			return nil
		}
		return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
	}
	return nil
}

func (d SchemaTracker) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	schema := d.SchemaByName(ident.Schema)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	var referTbl *model.TableInfo
	if s.ReferTable != nil {
		_, err := d.TableByName(s.ReferTable.Schema, s.ReferTable.Name)
		if err != nil {
			return infoschema.ErrTableNotExists.GenWithStackByArgs(s.ReferTable.Schema, s.ReferTable.Name)
		}
	}

	// build tableInfo
	var (
		tbInfo *model.TableInfo
		err    error
	)
	if s.ReferTable != nil {
		tbInfo, err = buildTableInfoWithLike(ctx, ident, referTbl, s)
	} else {
		tbInfo, err = buildTableInfoWithStmt(ctx, s, schema.Charset, schema.Collate, schema.PlacementPolicyRef)
	}
	if err != nil {
		return errors.Trace(err)
	}

	onExist := OnExistError
	if s.IfNotExists {
		onExist = OnExistIgnore
	}

	return d.CreateTableWithInfo(ctx, schema.Name, tbInfo, onExist)
}

func (d SchemaTracker) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName model.CIStr,
	info *model.TableInfo,
	onExist OnExist,
) error {
	schema := d.SchemaByName(dbName)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName)
	}

	oldTable, _ := d.TableByName(dbName, info.Name)
	if oldTable != nil {
		switch onExist {
		case OnExistIgnore:
			return nil
		case OnExistReplace:
			return d.putTable(dbName, info)
		default:
			return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: dbName, Name: info.Name})
		}
	}

	return d.putTable(dbName, info)
}

func (d SchemaTracker) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) error {
	viewInfo, err := buildViewInfo(ctx, s)
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

	tbInfo, err := buildTableInfo(ctx, s.ViewName.Name, cols, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.View = viewInfo

	onExist := OnExistError
	if s.OrReplace {
		onExist = OnExistReplace
	}

	return d.CreateTableWithInfo(ctx, s.ViewName.Schema, tbInfo, onExist)
}

func (d SchemaTracker) DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	notExistTables := make([]string, 0, len(stmt.Tables))
	for _, name := range stmt.Tables {
		tb, err := d.TableByName(name.Schema, name.Name)
		if err != nil || !tb.IsBaseTable() {
			if stmt.IfExists {
				continue
			}

			id := ast.Ident{Schema: name.Schema, Name: name.Name}
			notExistTables = append(notExistTables, id.String())
			continue
		}

		_ = d.deleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

func (d SchemaTracker) RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error) {
	return nil
}

func (d SchemaTracker) DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
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

		if !tb.IsView() {
			return dbterror.ErrWrongObject.GenWithStackByArgs(name.Schema, name.Name, "VIEW")
		}

		_ = d.deleteTable(name.Schema, name.Name)
	}

	if len(notExistTables) > 0 {
		return infoschema.ErrTableDropExists.GenWithStackByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

func (d SchemaTracker) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	return d.createIndex(ctx, ident, stmt.KeyType, model.NewCIStr(stmt.IndexName),
		stmt.IndexPartSpecifications, stmt.IndexOption, stmt.IfNotExists)
}

func (d SchemaTracker) createIndex(ctx sessionctx.Context, ti ast.Ident, keyType ast.IndexKeyType, indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error {

	unique := keyType == ast.IndexKeyTypeUnique
	tblInfo, err := d.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return err
	}
	t := tables.MockTableFromMeta(tblInfo)

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		colName := model.NewCIStr("expression_index")
		if indexPartSpecifications[0].Column != nil {
			colName = indexPartSpecifications[0].Column.Name
		}
		indexName = getAnonymousIndex(t, colName, model.NewCIStr(""))
	}

	if indexInfo := tblInfo.FindIndexByName(indexName.L); indexInfo != nil {
		if ifNotExists {
			return nil
		}
		return dbterror.ErrDupKeyName.GenWithStack("index already exist %s", indexName)
	}

	// Skip build hidden column for expression index

	indexInfo, err := buildIndexInfo(nil, tblInfo, indexName, indexPartSpecifications, model.StatePublic)
	if err != nil {
		return err
	}
	if indexOption != nil {
		indexInfo.Comment = indexOption.Comment
		if indexOption.Visibility == ast.IndexVisibilityInvisible {
			indexInfo.Invisible = true
		}
		if indexOption.Tp == model.IndexTypeInvalid {
			// Use btree as default index type.
			indexInfo.Tp = model.IndexTypeBtree
		} else {
			indexInfo.Tp = indexOption.Tp
		}
	} else {
		// Use btree as default index type.
		indexInfo.Tp = model.IndexTypeBtree
	}
	indexInfo.Primary = false
	indexInfo.Unique = unique
	indexInfo.ID = allocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)

	addIndexColumnFlag(tblInfo, indexInfo)
	return nil
}

func (d SchemaTracker) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	err := d.dropIndex(ctx, ti, model.NewCIStr(stmt.IndexName), stmt.IfExists)
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && stmt.IfExists {
		err = nil
	}
	return err
}

func (d SchemaTracker) dropIndex(ctx sessionctx.Context, ti ast.Ident, indexName model.CIStr, ifExists bool) error {
	tblInfo, err := d.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name)
	}
	t := tables.MockTableFromMeta(tblInfo)

	indexInfo := tblInfo.FindIndexByName(indexName.L)

	_, err = checkIsDropPrimaryKey(indexName, indexInfo, t)
	if err != nil {
		return err
	}

	if indexInfo == nil {
		if ifExists {
			return nil
		}
		return dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Check for drop index on auto_increment column.
	err = checkDropIndexOnAutoIncrementColumn(t.Meta(), indexInfo)
	if err != nil {
		return err
	}

	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.Name.L != indexInfo.Name.L {
			newIndices = append(newIndices, idx)
		}
	}
	tblInfo.Indices = newIndices
	// Set column index flag.
	dropIndexColumnFlag(tblInfo, indexInfo)
	return nil
}

func (d SchemaTracker) addColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	schema := d.SchemaByName(ti.Schema)
	if schema == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ti.Schema)
	}
	tblInfo, err := d.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return err
	}
	t := tables.MockTableFromMeta(tblInfo)
	colName := specNewColumn.Name.Name.O

	if col := table.FindCol(t.Cols(), colName); col != nil {
		if spec.IfNotExists {
			return nil
		}
		return infoschema.ErrColumnExists.GenWithStackByArgs(colName)
	}

	col, err := createNewColumn(ctx, ti, schema, spec, t, specNewColumn)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkAfterPositionExists(tblInfo, spec.Position)
	if err != nil {
		return errors.Trace(err)
	}

	columnInfo := initAndAddColumnToTable(tblInfo, col.ColumnInfo)
	offset, err := locateOffsetToMove(columnInfo.Offset, spec.Position, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo.MoveColumnInfo(columnInfo.Offset, offset)
	columnInfo.State = model.StatePublic
	return nil
}

func (d *SchemaTracker) dropColumn(ctx sessionctx.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	tblInfo, err := d.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return err
	}
	colName := spec.OldColumnName.Name

	i := -1
	for i = range tblInfo.Columns {
		if tblInfo.Columns[i].Name.L == colName.L {
			break
		}
	}
	if i == -1 {
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
	tblInfo.Columns = append(tblInfo.Columns[:i], tblInfo.Columns[i+1:]...)
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		j := -1
		for j = range idx.Columns {
			if idx.Columns[j].Name.L == colName.L {
				break
			}
		}
		if j == -1 {
			newIndices = append(newIndices, idx)
			continue
		}

		idx.Columns = append(idx.Columns[:j], idx.Columns[j+1:]...)
		if len(idx.Columns) == 0 {
			continue
		}
		newIndices = append(newIndices, idx)
	}
	tblInfo.Indices = newIndices
	return nil
}

func (d SchemaTracker) renameColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	oldColName := spec.OldColumnName.Name
	newColName := spec.NewColumnName.Name

	tblInfo, err := d.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return err
	}
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

	if fkInfo := getColumnForeignKeyInfo(oldColName.L, tbl.Meta().ForeignKeys); fkInfo != nil {
		return dbterror.ErrFKIncompatibleColumns.GenWithStackByArgs(oldColName, fkInfo.Name)
	}

	// Check generated expression.
	for _, col := range allCols {
		if col.GeneratedExpr == nil {
			continue
		}
		dependedColNames := findColumnNamesInExpr(col.GeneratedExpr)
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

func (d SchemaTracker) alterColumn(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	specNewColumn := spec.NewColumns[0]
	tblInfo, err := d.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return err
	}
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
		if IsAutoRandomColumnID(t.Meta(), oldCol.ID) {
			return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
		hasDefaultValue, err := setDefaultValue(ctx, oldCol, specNewColumn.Options[0])
		if err != nil {
			return errors.Trace(err)
		}
		if err = checkDefaultValue(ctx, oldCol, hasDefaultValue); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

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
) error {
	tblInfo, err := d.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return err
	}
	schema := d.SchemaByName(ident.Schema)
	t := tables.MockTableFromMeta(tblInfo)
	job, err := getModifiableColumnJob(ctx, sctx, ident, originalColName, schema, t, spec)
	if err != nil {
		if infoschema.ErrColumnNotExists.Equal(err) && spec.IfExists {
			sctx.GetSessionVars().StmtCtx.AppendNote(infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name))
			return nil
		}
		return errors.Trace(err)
	}

	modifyInfo := &modifyingColInfo{
		newCol:                job.Args[0].(*model.ColumnInfo),
		oldColName:            job.Args[1].(*model.CIStr),
		pos:                   job.Args[2].(*ast.ColumnPosition),
		modifyColumnTp:        job.Args[3].(byte),
		updatedAutoRandomBits: job.Args[4].(uint64),
	}

	tblInfo.AutoRandomBits = modifyInfo.updatedAutoRandomBits
	oldCol := table.FindCol(t.Cols(), modifyInfo.oldColName.L).ColumnInfo

	changingColPos := &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
	newColName := model.NewCIStr(genChangingColumnUniqueName(tblInfo, oldCol))
	if mysql.HasPriKeyFlag(oldCol.GetFlag()) {
		job.State = model.JobStateCancelled
		msg := "this column has primary key flag"
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}

	modifyInfo.changingCol = modifyInfo.newCol.Clone()
	modifyInfo.changingCol.Name = newColName
	modifyInfo.changingCol.ChangeStateInfo = &model.ChangeStateInfo{DependencyColumnOffset: oldCol.Offset}
	originDefVal, err := getOriginDefaultValueForModifyColumn(sctx, modifyInfo.changingCol, oldCol)
	if err != nil {
		return errors.Trace(err)
	}
	if err = modifyInfo.changingCol.SetOriginDefaultValue(originDefVal); err != nil {
		return errors.Trace(err)
	}

	_, _, _, err = createColumnInfoWithPosCheck(tblInfo, modifyInfo.changingCol, changingColPos)
	if err != nil {
		return errors.Trace(err)
	}

	idxInfos, offsets := findIndexesByColName(tblInfo.Indices, oldCol.Name.L)
	modifyInfo.changingIdxs = make([]*model.IndexInfo, 0, len(idxInfos))
	for i, idxInfo := range idxInfos {
		newIdxInfo := idxInfo.Clone()
		newIdxInfo.Name = model.NewCIStr(genChangingIndexUniqueName(tblInfo, idxInfo))
		newIdxInfo.ID = allocateIndexID(tblInfo)
		newIdxChangingCol := newIdxInfo.Columns[offsets[i]]
		newIdxChangingCol.Name = newColName
		newIdxChangingCol.Offset = modifyInfo.changingCol.Offset
		canPrefix := types.IsTypePrefixable(modifyInfo.changingCol.GetType())
		if !canPrefix || (canPrefix && modifyInfo.changingCol.GetFlen() < newIdxChangingCol.Length) {
			newIdxChangingCol.Length = types.UnspecifiedLength
		}
		modifyInfo.changingIdxs = append(modifyInfo.changingIdxs, newIdxInfo)
	}
	tblInfo.Indices = append(tblInfo.Indices, modifyInfo.changingIdxs...)

	err = adjustTableInfoAfterModifyColumnWithData(
		tblInfo,
		modifyInfo.pos,
		oldCol,
		modifyInfo.changingCol,
		modifyInfo.newCol.Name,
		modifyInfo.changingIdxs,
	)
	if err != nil {
		return err
	}

	updateChangingObjState(modifyInfo.changingCol, modifyInfo.changingIdxs, model.StatePublic)
	return nil
}

func (d SchemaTracker) renameIndex(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	tblInfo, err := d.TableByName(ident.Schema, ident.Name)
	duplicate, err := validateRenameIndex(spec.FromKey, spec.ToKey, tblInfo)
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

func (d SchemaTracker) addTablePartitions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	tblInfo, err := d.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}

	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partInfo, err := buildAddedPartitionInfo(ctx, tblInfo, spec)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo.Partition.Definitions = append(tblInfo.Partition.Definitions, partInfo.Definitions...)
	return nil
}

func (d SchemaTracker) dropTablePartition(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	tblInfo, err := d.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}

	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partNames := make([]string, len(spec.PartitionNames))
	for i, partCIName := range spec.PartitionNames {
		partNames[i] = partCIName.L
	}
	err = checkDropTablePartition(tblInfo, partNames)
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

func (d SchemaTracker) createPrimaryKey(
	ctx sessionctx.Context,
	ti ast.Ident,
	indexName model.CIStr,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
) error {
	tblInfo, err := d.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(err)
	}

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

	if _, err = checkPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
		return err
	}

	// May be truncate comment here, when index comment too long and sql_mode is't strict.
	if indexOption != nil {
		if _, err = validateCommentLength(ctx.GetSessionVars(), indexName.String(), &indexOption.Comment, dbterror.ErrTooLongIndexComment); err != nil {
			return errors.Trace(err)
		}
	}

	// TODO: provide a common function, to serve SchemaTracker.createIndex and ddl.
	indexInfo, err := buildIndexInfo(nil, tblInfo, indexName, indexPartSpecifications, model.StatePublic)
	if err != nil {
		return errors.Trace(err)
	}
	if indexOption != nil {
		indexInfo.Comment = indexOption.Comment
		if indexOption.Visibility == ast.IndexVisibilityInvisible {
			indexInfo.Invisible = true
		}
		if indexOption.Tp == model.IndexTypeInvalid {
			// Use btree as default index type.
			indexInfo.Tp = model.IndexTypeBtree
		} else {
			indexInfo.Tp = indexOption.Tp
		}
	} else {
		// Use btree as default index type.
		indexInfo.Tp = model.IndexTypeBtree
	}
	indexInfo.Primary = true
	indexInfo.Unique = true
	indexInfo.ID = allocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	// Set column index flag.
	addIndexColumnFlag(tblInfo, indexInfo)
	if err = updateColsNull2NotNull(tblInfo, indexInfo); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d SchemaTracker) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error {
	validSpecs, err := resolveAlterTableSpec(sctx, stmt.Specs)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO: reorder specs to follow MySQL's order, drop index -> drop column -> rename column -> add column -> add index
	// https://github.com/mysql/mysql-server/blob/8d8c986e5716e38cb776b627a8eee9e92241b4ce/sql/sql_table.cc#L16698-L16714

	ident := ast.Ident{Schema: stmt.Table.Schema, Name: stmt.Table.Name}
	// TODO: precheck about table existence?

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
			err = d.dropTablePartition(sctx, ident, spec)
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
			tblInfo, err := d.TableByName(ident.Schema, ident.Name)
			if err != nil {
				return errors.Trace(err)
			}
			for i, opt := range spec.Options {
				switch opt.Tp {
				case ast.TableOptionShardRowID:
				case ast.TableOptionAutoIncrement:
				case ast.TableOptionAutoIdCache:
				case ast.TableOptionAutoRandomBase:
				case ast.TableOptionComment:
					tblInfo.Comment = opt.StrValue
				case ast.TableOptionCharset, ast.TableOptionCollate:
					// getCharsetAndCollateInTableOption will get the last charset and collate in the options,
					// so it should be handled only once.
					if handledCharsetOrCollate {
						continue
					}
					var toCharset, toCollate string
					toCharset, toCollate, err = getCharsetAndCollateInTableOption(i, spec.Options)
					if err != nil {
						return err
					}
					needsOverwriteCols := needToOverwriteColCharset(spec.Options)

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
			ast.AlterTableIndexInvisible,
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

	//TODO implement me
	panic("implement me")
}

func (d SchemaTracker) TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error {
	return nil
}

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

func (d SchemaTracker) renameTable(ctx sessionctx.Context, oldIdents, newIdents []ast.Ident, isAlterTable bool) error {
	tablesCache := make(map[string]int64)
	is := infoSchemaAdaptor{inner: d.infoStore}
	for i := range oldIdents {
		_, _, err := extractTblInfos(is, oldIdents[i], newIdents[i], isAlterTable, tablesCache)
		if err != nil {
			return err
		}
	}

	for i := range oldIdents {
		tableInfo, err := d.TableByName(oldIdents[i].Schema, oldIdents[i].Name)
		if err != nil {
			return err
		}
		if err = d.deleteTable(oldIdents[i].Schema, oldIdents[i].Name); err != nil {
			return err
		}
		tableInfo.Name = newIdents[i].Name
		if err = d.putTable(newIdents[i].Schema, tableInfo); err != nil {
			return err
		}
	}
	return nil
}

func (d SchemaTracker) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	return nil
}

func (d SchemaTracker) UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error {
	return nil

}

func (d SchemaTracker) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	return nil

}

func (d SchemaTracker) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	return nil

}

func (d SchemaTracker) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	return nil

}

func (d SchemaTracker) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	return nil
}

func (d SchemaTracker) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return nil

}

func (d SchemaTracker) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	return nil
}

func (d SchemaTracker) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error {
	return nil

}

func (d SchemaTracker) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error {
	return nil

}

func (d SchemaTracker) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error {
	return nil

}

func (d SchemaTracker) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, onExist OnExist) error {
	for _, tableInfo := range info {
		if err := d.CreateTableWithInfo(ctx, schema, tableInfo, onExist); err != nil {
			return err
		}
	}
	return nil
}

func (d SchemaTracker) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	return nil

}

func (d SchemaTracker) Start(ctxPool *pools.ResourcePool) error {
	return nil

}

func (d SchemaTracker) GetLease() time.Duration {
	return 0
}

func (d SchemaTracker) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	return nil, nil
}

func (d SchemaTracker) GetScope(status string) variable.ScopeFlag {
	return 0
}

func (d SchemaTracker) Stop() error {
	return nil
}

func (d SchemaTracker) RegisterStatsHandle(handle *handle.Handle) {}

func (d SchemaTracker) SchemaSyncer() util.SchemaSyncer {
	return nil
}

func (d SchemaTracker) OwnerManager() owner.Manager {
	return nil
}

func (d SchemaTracker) GetID() string {
	return "schema-tracker"
}

func (d SchemaTracker) GetTableMaxHandle(ctx *JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error) {
	return nil, false, nil
}

func (d SchemaTracker) SetBinlogClient(client *pumpcli.PumpsClient) {}

func (d SchemaTracker) GetHook() Callback {
	return nil
}

func (d SchemaTracker) SetHook(h Callback) {}

func (d SchemaTracker) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return nil
}

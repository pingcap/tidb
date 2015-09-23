// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package ddl

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/charset"
	qerror "github.com/pingcap/tidb/util/errors"
	"github.com/pingcap/tidb/util/errors2"
)

// Pre-defined errors
var (
	// ErrExists returned for creating an exist schema or table.
	ErrExists = errors.Errorf("DDL:exists")
	// ErrNotExists returned for dropping a not exist schema or table.
	ErrNotExists = errors.Errorf("DDL:not exists")
)

// DDL is responsible for updating schema in data store and maintain in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx context.Context, name model.CIStr) error
	DropSchema(ctx context.Context, schema model.CIStr) error
	CreateTable(ctx context.Context, ident table.Ident, cols []*coldef.ColumnDef, constrs []*coldef.TableConstraint) error
	DropTable(ctx context.Context, tableIdent table.Ident) (err error)
	CreateIndex(ctx context.Context, tableIdent table.Ident, unique bool, indexName model.CIStr, columnNames []*coldef.IndexColName) error
	DropIndex(ctx context.Context, schema, tableName, indexName model.CIStr) error
	GetInformationSchema() infoschema.InfoSchema
	AlterTable(ctx context.Context, tableIdent table.Ident, spec []*AlterSpecification) error
}

type ddl struct {
	store       kv.Storage
	infoHandle  *infoschema.Handle
	onDDLChange OnDDLChange
}

type OnDDLChange func(err error) error

// NewDDL create new DDL
func NewDDL(store kv.Storage, infoHandle *infoschema.Handle, hook OnDDLChange) DDL {
	d := &ddl{
		store:       store,
		infoHandle:  infoHandle,
		onDDLChange: hook,
	}
	return d
}

func (d *ddl) GetInformationSchema() infoschema.InfoSchema {
	return d.infoHandle.Get()
}

func (d *ddl) CreateSchema(ctx context.Context, schema model.CIStr) (err error) {
	is := d.GetInformationSchema()
	_, ok := is.SchemaByName(schema)
	if ok {
		return ErrExists
	}
	info := &model.DBInfo{Name: schema}
	info.ID, err = meta.GenGlobalID(d.store)
	if err != nil {
		return errors.Trace(err)
	}
	err = d.writeSchemaInfo(info)
	if err != nil {
		return errors.Trace(err)
	}
	newInfo := append(is.Clone(), info)
	d.infoHandle.Set(newInfo)
	return nil
}

func (d *ddl) verifySchemaMetaVersion(txn kv.Transaction) error {
	curVer, err := txn.GetInt64(meta.SchemaMetaVersion)
	if err != nil {
		return errors.Trace(err)
	}
	ourVer := d.infoHandle.SchemaMetaVersion()
	if curVer != ourVer {
		return errors.Errorf("Schema changed, our version %d, got %d", ourVer, curVer)
	}

	// Increment version
	_, err = txn.Inc(meta.SchemaMetaVersion, 1)
	return errors.Trace(err)
}

func (d *ddl) DropSchema(ctx context.Context, schema model.CIStr) (err error) {
	is := d.GetInformationSchema()
	old, ok := is.SchemaByName(schema)
	if !ok {
		return ErrNotExists
	}

	// Update InfoSchema
	oldInfo := is.Clone()
	var newInfo []*model.DBInfo
	for _, v := range oldInfo {
		if v.Name.L != schema.L {
			newInfo = append(newInfo, v)
		}
	}
	d.infoHandle.Set(newInfo)

	// Remove data
	txn, err := ctx.GetTxn(true)
	if err != nil {
		return errors.Trace(err)
	}
	tables := is.SchemaTables(schema)
	for _, t := range tables {
		err = t.Truncate(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// remove indices
		for _, v := range t.Indices() {
			if v != nil && v.X != nil {
				if err = v.X.Drop(txn); err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	// Delete meta key
	err = kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		err := d.verifySchemaMetaVersion(txn)
		if err != nil {
			return errors.Trace(err)
		}
		key := []byte(meta.DBMetaKey(old.ID))
		if err := txn.LockKeys(meta.SchemaMetaVersion, key); err != nil {
			return errors.Trace(err)
		}
		return txn.Delete(key)
	})
	if d.onDDLChange != nil {
		err = d.onDDLChange(err)
	}
	return errors.Trace(err)
}

func getDefaultCharsetAndCollate() (string, string) {
	// TODO: TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset
	// TODO: change TableOption parser to parse collate
	// This is a tmp solution
	//return "latin1", "latin1_swedish_ci"
	return "utf8", "utf8_unicode_ci"
}

func setColumnFlagWithConstraint(colMap map[string]*column.Col, v *coldef.TableConstraint) {
	switch v.Tp {
	case coldef.ConstrPrimaryKey:
		for _, key := range v.Keys {
			c, ok := colMap[strings.ToLower(key.ColumnName)]
			if !ok {
				// TODO: table constraint on unknown column
				continue
			}
			c.Flag |= mysql.PriKeyFlag
			// primary key can not be NULL
			c.Flag |= mysql.NotNullFlag
		}
	case coldef.ConstrUniq, coldef.ConstrUniqIndex, coldef.ConstrUniqKey:
		for i, key := range v.Keys {
			c, ok := colMap[strings.ToLower(key.ColumnName)]
			if !ok {
				// TODO: table constraint on unknown column
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// see https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					c.Flag |= mysql.MultipleKeyFlag
				} else {
					c.Flag |= mysql.UniqueKeyFlag
				}
			}
		}
	case coldef.ConstrKey, coldef.ConstrIndex:
		for i, key := range v.Keys {
			c, ok := colMap[strings.ToLower(key.ColumnName)]
			if !ok {
				// TODO: table constraint on unknown column
				continue
			}

			if i == 0 {
				// Only the first column can be set
				c.Flag |= mysql.MultipleKeyFlag
			}
		}
	}
}

func (d *ddl) buildColumnsAndConstraints(colDefs []*coldef.ColumnDef, constraints []*coldef.TableConstraint) ([]*column.Col, []*coldef.TableConstraint, error) {
	var cols []*column.Col
	colMap := map[string]*column.Col{}
	for i, colDef := range colDefs {
		col, cts, err := d.buildColumnAndConstraint(i, colDef)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[strings.ToLower(colDef.Name)] = col
	}
	// traverse table Constraints and set col.flag
	for _, v := range constraints {
		setColumnFlagWithConstraint(colMap, v)
	}
	return cols, constraints, nil
}

func (d *ddl) buildColumnAndConstraint(offset int, colDef *coldef.ColumnDef) (*column.Col, []*coldef.TableConstraint, error) {
	// set charset
	if len(colDef.Tp.Charset) == 0 {
		switch colDef.Tp.Tp {
		case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			colDef.Tp.Charset, colDef.Tp.Collate = getDefaultCharsetAndCollate()
		default:
			colDef.Tp.Charset = charset.CharsetBin
			colDef.Tp.Collate = charset.CharsetBin
		}
	}
	// convert colDef into col
	col, cts, err := coldef.ColumnDefToCol(offset, colDef)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	col.ID, err = meta.GenGlobalID(d.store)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, cts, nil
}

func checkDuplicateColumn(colDefs []*coldef.ColumnDef) error {
	colNames := map[string]bool{}
	for _, colDef := range colDefs {
		nameLower := strings.ToLower(colDef.Name)
		if colNames[nameLower] {
			return errors.Errorf("CREATE TABLE: duplicate column %s", colDef.Name)
		}
		colNames[nameLower] = true
	}
	return nil
}

func checkConstraintNames(constraints []*coldef.TableConstraint) error {
	constrNames := map[string]bool{}

	// Check not empty constraint name do not have duplication.
	for _, constr := range constraints {
		if constr.ConstrName != "" {
			nameLower := strings.ToLower(constr.ConstrName)
			if constrNames[nameLower] {
				return errors.Errorf("CREATE TABLE: duplicate key %s", constr.ConstrName)
			}
			constrNames[nameLower] = true
		}
	}

	// Set empty constraint names.
	for _, constr := range constraints {
		if constr.ConstrName == "" && len(constr.Keys) > 0 {
			colName := constr.Keys[0].ColumnName
			constrName := colName
			i := 2
			for constrNames[strings.ToLower(constrName)] {
				// We loop forever until we find constrName that haven't been used.
				constrName = fmt.Sprintf("%s_%d", colName, i)
				i++
			}
			constr.ConstrName = constrName
			constrNames[constrName] = true
		}
	}
	return nil
}

func (d *ddl) buildTableInfo(tableName model.CIStr, cols []*column.Col, constraints []*coldef.TableConstraint) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name: tableName,
	}
	tbInfo.ID, err = meta.GenGlobalID(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, v := range cols {
		tbInfo.Columns = append(tbInfo.Columns, &v.ColumnInfo)
	}
	for _, constr := range constraints {
		// 1. check if the column is exists
		// 2. add index
		indexColumns := make([]*model.IndexColumn, 0, len(constr.Keys))
		for _, key := range constr.Keys {
			col := column.FindCol(cols, key.ColumnName)
			if col == nil {
				return nil, errors.Errorf("No such column: %v", key)
			}
			indexColumns = append(indexColumns, &model.IndexColumn{
				Name:   model.NewCIStr(key.ColumnName),
				Offset: col.Offset,
				Length: key.Length,
			})
		}
		idxInfo := &model.IndexInfo{
			Name:    model.NewCIStr(constr.ConstrName),
			Columns: indexColumns,
		}
		switch constr.Tp {
		case coldef.ConstrPrimaryKey:
			idxInfo.Unique = true
			idxInfo.Primary = true
			idxInfo.Name = model.NewCIStr(column.PrimaryKeyName)
		case coldef.ConstrUniq, coldef.ConstrUniqKey, coldef.ConstrUniqIndex:
			idxInfo.Unique = true
		}
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return
}

func (d *ddl) CreateTable(ctx context.Context, ident table.Ident, colDefs []*coldef.ColumnDef, constraints []*coldef.TableConstraint) (err error) {
	is := d.GetInformationSchema()
	if !is.SchemaExists(ident.Schema) {
		return errors.Trace(qerror.ErrDatabaseNotExist)
	}
	if is.TableExists(ident.Schema, ident.Name) {
		return errors.Trace(ErrExists)
	}
	if err = checkDuplicateColumn(colDefs); err != nil {
		return errors.Trace(err)
	}

	cols, newConstraints, err := d.buildColumnsAndConstraints(colDefs, constraints)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkConstraintNames(newConstraints)
	if err != nil {
		return errors.Trace(err)
	}

	tbInfo, err := d.buildTableInfo(ident.Name, cols, newConstraints)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("New table: %+v", tbInfo)
	err = d.updateInfoSchema(ctx, ident.Schema, tbInfo)
	return errors.Trace(err)
}

func (d *ddl) AlterTable(ctx context.Context, ident table.Ident, specs []*AlterSpecification) (err error) {
	//Get database and table
	is := d.GetInformationSchema()
	if !is.SchemaExists(ident.Schema) {
		return errors.Trace(qerror.ErrDatabaseNotExist)
	}
	tbl, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(err)
	}
	for _, spec := range specs {
		switch spec.Action {
		case AlterAddColumn:
			if err := d.addColumn(ctx, ident.Schema, tbl, spec); err != nil {
				return errors.Trace(err)
			}
		default:
			// TODO: process more actions
			continue
		}
	}
	return nil
}

// Add a column into table
func (d *ddl) addColumn(ctx context.Context, schema model.CIStr, tbl table.Table, spec *AlterSpecification) error {
	// Find position
	cols := tbl.Cols()
	position := len(cols)
	name := spec.Column.Name
	// Check column name duplicate
	dc := column.FindCol(cols, name)
	if dc != nil {
		return errors.Errorf("Try to add a column with the same name of an already exists column.")
	}
	if spec.Position.Type == ColumnPositionFirst {
		position = 0
	} else if spec.Position.Type == ColumnPositionAfter {
		// Find the mentioned column
		c := column.FindCol(cols, spec.Position.RelativeColumn)
		if c == nil {
			return errors.Errorf("No such column: %v", name)
		}
		// insert position is after the mentioned column
		position = c.Offset + 1
	}
	// TODO: Set constraint
	col, _, err := d.buildColumnAndConstraint(position, spec.Column)
	if err != nil {
		return errors.Trace(err)
	}
	// insert col into the right place of the column list
	newCols := make([]*column.Col, 0, len(cols)+1)
	newCols = append(newCols, cols[:position]...)
	newCols = append(newCols, col)
	newCols = append(newCols, cols[position:]...)
	// adjust position
	if position != len(cols) {
		offsetChange := make(map[int]int)
		for i := position + 1; i < len(newCols); i++ {
			offsetChange[newCols[i].Offset] = i
			newCols[i].Offset = i
		}
		// Update index offset info
		for _, idx := range tbl.Indices() {
			for _, c := range idx.Columns {
				newOffset, ok := offsetChange[c.Offset]
				if ok {
					c.Offset = newOffset
				}
			}
		}
	}
	tb := tbl.(*tables.Table)
	tb.Columns = newCols
	// TODO: update index
	// TODO: update default value
	// update infomation schema
	err = d.updateInfoSchema(ctx, schema, tb.Meta())
	return errors.Trace(err)
}

// drop table will proceed even if some table in the list does not exists
func (d *ddl) DropTable(ctx context.Context, ti table.Ident) (err error) {
	is := d.GetInformationSchema()
	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(err)
	}
	// update InfoSchema before delete all the table data.
	clonedInfo := is.Clone()
	for _, info := range clonedInfo {
		if info.Name == ti.Schema {
			var newTableInfos []*model.TableInfo
			// append other tables.
			for _, tbInfo := range info.Tables {
				if tbInfo.Name.L != ti.Name.L {
					newTableInfos = append(newTableInfos, tbInfo)
				}
			}
			info.Tables = newTableInfos
			err = d.writeSchemaInfo(info)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	d.infoHandle.Set(clonedInfo)
	err = d.deleteTableData(ctx, tb)
	return errors.Trace(err)
}

func (d *ddl) deleteTableData(ctx context.Context, t table.Table) error {
	// Remove data
	err := t.Truncate(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	// Remove indices
	for _, v := range t.Indices() {
		if v != nil && v.X != nil {
			if err = v.X.Drop(txn); err != nil {
				return errors.Trace(err)
			}
		}
	}
	// Remove auto ID key
	err = txn.Delete([]byte(meta.AutoIDKey(t.TableID())))

	// Auto ID meta is created when the first time used, so it may not exist.
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		return nil
	}
	return errors.Trace(err)
}

func (d *ddl) CreateIndex(ctx context.Context, ti table.Ident, unique bool, indexName model.CIStr, idxColNames []*coldef.IndexColName) error {
	is := d.infoHandle.Get()
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := is.IndexByName(ti.Schema, ti.Name, indexName); ok {
		return errors.Errorf("CREATE INDEX: index already exist %s", indexName)
	}
	if is.ColumnExists(ti.Schema, ti.Name, indexName) {
		return errors.Errorf("CREATE INDEX: index name collision with existing column: %s", indexName)
	}

	tbInfo := t.Meta()

	// build offsets
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))
	for i, ic := range idxColNames {
		col := column.FindCol(t.Cols(), ic.ColumnName)
		if col == nil {
			return errors.Errorf("CREATE INDEX: column does not exist: %s", ic.ColumnName)
		}
		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
		// Set ColumnInfo flag
		if i == 0 {
			if unique && len(idxColNames) == 1 {
				tbInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
			} else {
				tbInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
			}
		}
	}
	// create index info
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		Unique:  unique,
	}
	tbInfo.Indices = append(tbInfo.Indices, idxInfo)

	// build index
	err = d.buildIndex(ctx, t, idxInfo, unique)
	if err != nil {
		return errors.Trace(err)
	}

	// update InfoSchema
	return d.updateInfoSchema(ctx, ti.Schema, tbInfo)
}

func (d *ddl) buildIndex(ctx context.Context, t table.Table, idxInfo *model.IndexInfo, unique bool) error {
	firstKey := t.FirstKey()
	prefix := t.KeyPrefix()

	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	it, err := txn.Seek([]byte(firstKey), nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()
	for it.Valid() && strings.HasPrefix(it.Key(), prefix) {
		var err error
		handle, err := util.DecodeHandleFromRowKey(it.Key())
		log.Info("building index...", handle)
		if err != nil {
			return errors.Trace(err)
		}
		// TODO: v is timestamp ?
		// fetch datas
		cols := t.Cols()
		var vals []interface{}
		for _, v := range idxInfo.Columns {
			var (
				data []byte
				val  interface{}
			)
			col := cols[v.Offset]
			k := t.RecordKey(handle, col)
			data, err = txn.Get([]byte(k))
			if err != nil {
				return errors.Trace(err)
			}
			val, err = t.DecodeValue(data, col)
			if err != nil {
				return errors.Trace(err)
			}
			vals = append(vals, val)
		}
		// build index
		kvX := kv.NewKVIndex(t.IndexPrefix(), idxInfo.Name.L, unique)
		err = kvX.Create(txn, vals, handle)
		if err != nil {
			return errors.Trace(err)
		}

		rk := []byte(t.RecordKey(handle, nil))
		it, err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (d *ddl) DropIndex(ctx context.Context, schema, tableName, indexNmae model.CIStr) error {
	// TODO: implement
	return nil
}

func (d *ddl) writeSchemaInfo(info *model.DBInfo) error {
	var b []byte
	b, err := json.Marshal(info)
	if err != nil {
		return errors.Trace(err)
	}
	err = kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		err := d.verifySchemaMetaVersion(txn)
		if err != nil {
			return errors.Trace(err)
		}
		key := []byte(meta.DBMetaKey(info.ID))
		if err := txn.LockKeys(key); err != nil {
			return errors.Trace(err)
		}
		return txn.Set(key, b)
	})
	log.Warn("save schema", string(b))
	if d.onDDLChange != nil {
		err = d.onDDLChange(err)
	}
	return errors.Trace(err)
}

func (d *ddl) updateInfoSchema(ctx context.Context, schema model.CIStr, tbInfo *model.TableInfo) error {
	clonedInfo := d.GetInformationSchema().Clone()
	for _, info := range clonedInfo {
		if info.Name == schema {
			var match bool
			for i := range info.Tables {
				if info.Tables[i].Name == tbInfo.Name {
					info.Tables[i] = tbInfo
					match = true
				}
			}
			if !match {
				info.Tables = append(info.Tables, tbInfo)
			}
			err := d.writeSchemaInfo(info)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	d.infoHandle.Set(clonedInfo)
	return nil
}

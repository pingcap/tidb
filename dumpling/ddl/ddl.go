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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/charset"
	qerror "github.com/pingcap/tidb/util/errors"
	"github.com/twinj/uuid"
)

// Pre-defined errors.
var (
	// ErrExists returns for creating an exist schema or table.
	ErrExists = errors.Errorf("DDL:exists")
	// ErrNotExists returns for dropping a not exist schema or table.
	ErrNotExists = errors.Errorf("DDL:not exists")
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx context.Context, name model.CIStr) error
	DropSchema(ctx context.Context, schema model.CIStr) error
	CreateTable(ctx context.Context, ident table.Ident, cols []*coldef.ColumnDef, constrs []*coldef.TableConstraint) error
	DropTable(ctx context.Context, tableIdent table.Ident) (err error)
	CreateIndex(ctx context.Context, tableIdent table.Ident, unique bool, indexName model.CIStr, columnNames []*coldef.IndexColName) error
	DropIndex(ctx context.Context, tableIdent table.Ident, indexName model.CIStr) error
	GetInformationSchema() infoschema.InfoSchema
	AlterTable(ctx context.Context, tableIdent table.Ident, spec []*AlterSpecification) error
}

type ddl struct {
	infoHandle  *infoschema.Handle
	onDDLChange OnDDLChange
	store       kv.Storage
	// schema lease seconds.
	lease     time.Duration
	uuid      string
	jobCh     chan struct{}
	jobDoneCh chan struct{}
	// reOrgDoneCh is for reorgnization, if the reorgnization job is done,
	// we will use this channel to notify outer.
	// TODO: now we use goroutine to simulate reorgnization jobs, later we may
	// use a persistent job list.
	reOrgDoneCh chan error

	quitCh chan struct{}
	wait   sync.WaitGroup
}

// OnDDLChange is used as hook function when schema changed.
type OnDDLChange func(err error) error

func fakeOnDDLChange(err error) error {
	return err
}

// NewDDL creates a new DDL.
func NewDDL(store kv.Storage, infoHandle *infoschema.Handle, hook OnDDLChange, lease time.Duration) DDL {
	return newDDL(store, infoHandle, hook, lease)
}

func newDDL(store kv.Storage, infoHandle *infoschema.Handle, hook OnDDLChange, lease time.Duration) *ddl {
	if hook == nil {
		hook = fakeOnDDLChange
	}

	d := &ddl{
		infoHandle:  infoHandle,
		onDDLChange: hook,
		store:       store,
		lease:       lease,
		uuid:        uuid.NewV4().String(),
		jobCh:       make(chan struct{}, 1),
		jobDoneCh:   make(chan struct{}, 1),
	}

	d.start()

	return d
}

func (d *ddl) start() {
	d.quitCh = make(chan struct{})
	d.wait.Add(1)
	go d.onWorker()
	// for every start, we will send a fake job to let worker
	// check owner first and try to find whether a job exists and run.
	asyncNotify(d.jobCh)
}

func (d *ddl) close() {
	if d.isClosed() {
		return
	}

	close(d.quitCh)

	d.wait.Wait()
}

func (d *ddl) isClosed() bool {
	select {
	case <-d.quitCh:
		return true
	default:
		return false
	}
}

func (d *ddl) GetInformationSchema() infoschema.InfoSchema {
	return d.infoHandle.Get()
}

func (d *ddl) genGlobalID() (int64, error) {
	var globalID int64
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		var err error
		globalID, err = meta.NewMeta(txn).GenGlobalID()
		return errors.Trace(err)
	})

	return globalID, errors.Trace(err)
}

func (d *ddl) CreateSchema(ctx context.Context, schema model.CIStr) (err error) {
	is := d.GetInformationSchema()
	_, ok := is.SchemaByName(schema)
	if ok {
		return errors.Trace(ErrExists)
	}

	schemaID, err := d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{schema},
	}

	err = d.startJob(ctx, job)
	err = d.onDDLChange(err)
	return errors.Trace(err)
}

func (d *ddl) verifySchemaMetaVersion(t *meta.Meta, schemaMetaVersion int64) error {
	curVer, err := t.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}
	if curVer != schemaMetaVersion {
		return errors.Errorf("Schema changed, our version %d, but got %d", schemaMetaVersion, curVer)
	}

	// Increment version.
	_, err = t.GenSchemaVersion()
	return errors.Trace(err)
}

func (d *ddl) DropSchema(ctx context.Context, schema model.CIStr) (err error) {
	is := d.GetInformationSchema()
	old, ok := is.SchemaByName(schema)
	if !ok {
		return errors.Trace(ErrNotExists)
	}

	job := &model.Job{
		SchemaID: old.ID,
		Type:     model.ActionDropSchema,
	}

	err = d.startJob(ctx, job)
	err = d.onDDLChange(err)
	return errors.Trace(err)
}

func getDefaultCharsetAndCollate() (string, string) {
	// TODO: TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
	// TODO: change TableOption parser to parse collate.
	// This is a tmp solution.
	return "utf8", "utf8_unicode_ci"
}

func setColumnFlagWithConstraint(colMap map[string]*column.Col, v *coldef.TableConstraint) {
	switch v.Tp {
	case coldef.ConstrPrimaryKey:
		for _, key := range v.Keys {
			c, ok := colMap[strings.ToLower(key.ColumnName)]
			if !ok {
				// TODO: table constraint on unknown column.
				continue
			}
			c.Flag |= mysql.PriKeyFlag
			// Primary key can not be NULL.
			c.Flag |= mysql.NotNullFlag
		}
	case coldef.ConstrUniq, coldef.ConstrUniqIndex, coldef.ConstrUniqKey:
		for i, key := range v.Keys {
			c, ok := colMap[strings.ToLower(key.ColumnName)]
			if !ok {
				// TODO: table constraint on unknown column.
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See: https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
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
				// TODO: table constraint on unknown column.
				continue
			}
			if i == 0 {
				// Only the first column can be set.
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
		col.State = model.StatePublic
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
	col.ID, err = d.genGlobalID()
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

	// Check not empty constraint name whether is duplicated.
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
	tbInfo.ID, err = d.genGlobalID()
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
			State:   model.StatePublic,
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
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
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

	job := &model.Job{
		SchemaID: schema.ID,
		TableID:  tbInfo.ID,
		Type:     model.ActionCreateTable,
		Args:     []interface{}{tbInfo},
	}

	err = d.startJob(ctx, job)
	err = d.onDDLChange(err)
	return errors.Trace(err)
}

func (d *ddl) AlterTable(ctx context.Context, ident table.Ident, specs []*AlterSpecification) (err error) {
	// Get database and table.
	is := d.GetInformationSchema()

	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return errors.Trace(qerror.ErrDatabaseNotExist)
	}

	tbl, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(ErrNotExists)
	}

	// now we only allow one schema changes at the same time.
	if len(specs) != 1 {
		return errors.New("can't run multi schema changes in one DDL")
	}

	for _, spec := range specs {
		switch spec.Action {
		case AlterAddColumn:
			err = d.addColumn(ctx, schema, tbl, spec, is.SchemaMetaVersion())
		case AlterDropIndex:
			err = d.DropIndex(ctx, ident, model.NewCIStr(spec.Name))
		case AlterAddConstr:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case coldef.ConstrKey, coldef.ConstrIndex:
				err = d.CreateIndex(ctx, ident, false, model.NewCIStr(constr.ConstrName), spec.Constraint.Keys)
			case coldef.ConstrUniq, coldef.ConstrUniqIndex, coldef.ConstrUniqKey:
				err = d.CreateIndex(ctx, ident, true, model.NewCIStr(constr.ConstrName), spec.Constraint.Keys)
			default:
				// nothing to do now.
			}
		default:
			// nothing to do now.
		}

		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Add a column into table
func (d *ddl) addColumn(ctx context.Context, schema *model.DBInfo, tbl table.Table, spec *AlterSpecification, schemaMetaVersion int64) error {
	// Find position
	cols := tbl.Cols()
	position := len(cols)
	name := spec.Column.Name
	// Check column name duplicate.
	dc := column.FindCol(cols, name)
	if dc != nil {
		return errors.Errorf("Try to add a column with the same name of an already exists column.")
	}
	if spec.Position.Type == ColumnPositionFirst {
		position = 0
	} else if spec.Position.Type == ColumnPositionAfter {
		// Find the mentioned column.
		c := column.FindCol(cols, spec.Position.RelativeColumn)
		if c == nil {
			return errors.Errorf("No such column: %v", name)
		}
		// Insert position is after the mentioned column.
		position = c.Offset + 1
	}
	// TODO: set constraint
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
	if err = updateOldRows(ctx, tb, col); err != nil {
		return errors.Trace(err)
	}

	// update infomation schema
	err = kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := d.verifySchemaMetaVersion(t, schemaMetaVersion)
		if err != nil {
			return errors.Trace(err)
		}

		err = t.UpdateTable(schema.ID, tb.Meta())
		return errors.Trace(err)
	})

	err = d.onDDLChange(err)
	return errors.Trace(err)
}

func updateOldRows(ctx context.Context, t *tables.Table, col *column.Col) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	it, err := txn.Seek([]byte(t.FirstKey()))
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	prefix := t.KeyPrefix()
	for it.Valid() && strings.HasPrefix(it.Key(), prefix) {
		handle, err0 := util.DecodeHandleFromRowKey(it.Key())
		if err0 != nil {
			return errors.Trace(err0)
		}
		k := t.RecordKey(handle, col)

		// TODO: check and get timestamp/datetime default value.
		// refer to getDefaultValue in stmt/stmts/stmt_helper.go.
		if err0 = t.SetColValue(txn, k, col.DefaultValue); err0 != nil {
			return errors.Trace(err0)
		}

		rk := t.RecordKey(handle, nil)
		if it, err0 = kv.NextUntil(it, util.RowKeyPrefixFilter(rk)); err0 != nil {
			return errors.Trace(err0)
		}
	}

	return nil
}

// DropTable will proceed even if some table in the list does not exists.
func (d *ddl) DropTable(ctx context.Context, ti table.Ident) (err error) {
	is := d.GetInformationSchema()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(qerror.ErrDatabaseNotExist)
	}

	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(ErrNotExists)
	}

	job := &model.Job{
		SchemaID: schema.ID,
		TableID:  tb.Meta().ID,
		Type:     model.ActionDropTable,
	}

	err = d.startJob(ctx, job)
	err = d.onDDLChange(err)
	return errors.Trace(err)
}

func (d *ddl) deleteTableData(ctx context.Context, t table.Table) error {
	// Remove data.
	err := t.Truncate(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	// Remove indices.
	for _, v := range t.Indices() {
		if v != nil && v.X != nil {
			if err = v.X.Drop(txn); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (d *ddl) CreateIndex(ctx context.Context, ti table.Ident, unique bool, indexName model.CIStr, idxColNames []*coldef.IndexColName) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(qerror.ErrDatabaseNotExist)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(ErrNotExists)
	}

	job := &model.Job{
		SchemaID: schema.ID,
		TableID:  t.Meta().ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{unique, indexName, idxColNames},
	}

	err = d.startJob(ctx, job)
	err = d.onDDLChange(err)
	return errors.Trace(err)
}

func (d *ddl) DropIndex(ctx context.Context, ti table.Ident, indexName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(qerror.ErrDatabaseNotExist)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(ErrNotExists)
	}

	job := &model.Job{
		SchemaID: schema.ID,
		TableID:  t.Meta().ID,
		Type:     model.ActionDropIndex,
		Args:     []interface{}{indexName},
	}

	err = d.startJob(ctx, job)
	err = d.onDDLChange(err)
	return errors.Trace(err)
}

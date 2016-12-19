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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"github.com/twinj/uuid"
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = terror.ClassDDL.New(codeInvalidWorker, "invalid worker")
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = terror.ClassDDL.New(codeNotOwner, "not Owner")
	errInvalidDDLJob         = terror.ClassDDL.New(codeInvalidDDLJob, "invalid ddl job")
	errInvalidBgJob          = terror.ClassDDL.New(codeInvalidBgJob, "invalid background job")
	errInvalidJobFlag        = terror.ClassDDL.New(codeInvalidJobFlag, "invalid job flag")
	errRunMultiSchemaChanges = terror.ClassDDL.New(codeRunMultiSchemaChanges, "can't run multi schema change")
	errWaitReorgTimeout      = terror.ClassDDL.New(codeWaitReorgTimeout, "wait for reorganization timeout")
	errInvalidStoreVer       = terror.ClassDDL.New(codeInvalidStoreVer, "invalid storage current version")

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex    = terror.ClassDDL.New(codeCantDropColWithIndex, "can't drop column with index")
	errUnsupportedAddColumn    = terror.ClassDDL.New(codeUnsupportedAddColumn, "unsupported add column")
	errUnsupportedModifyColumn = terror.ClassDDL.New(codeUnsupportedModifyColumn, "unsupported modify column")
	errUnsupportedPKHandle     = terror.ClassDDL.New(codeUnsupportedDropPKHandle,
		"unsupported drop integer primary key")

	errBlobKeyWithoutLength  = terror.ClassDDL.New(codeBlobKeyWithoutLength, "index for BLOB/TEXT column must specificate a key length")
	errIncorrectPrefixKey    = terror.ClassDDL.New(codeIncorrectPrefixKey, "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys")
	errTooLongKey            = terror.ClassDDL.New(codeTooLongKey, fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLength))
	errKeyColumnDoesNotExits = terror.ClassDDL.New(codeKeyColumnDoesNotExits, "this key column doesn't exist in table")
	errDupKeyName            = terror.ClassDDL.New(codeDupKeyName, "duplicate key name")
	errWrongDBName           = terror.ClassDDL.New(codeWrongDBName, "Incorrect database name '%s'")
	errWrongTableName        = terror.ClassDDL.New(codeWrongTableName, "Incorrect table name '%s'")

	// ErrInvalidDBState returns for invalid database state.
	ErrInvalidDBState = terror.ClassDDL.New(codeInvalidDBState, "invalid database state")
	// ErrInvalidTableState returns for invalid Table state.
	ErrInvalidTableState = terror.ClassDDL.New(codeInvalidTableState, "invalid table state")
	// ErrInvalidColumnState returns for invalid column state.
	ErrInvalidColumnState = terror.ClassDDL.New(codeInvalidColumnState, "invalid column state")
	// ErrInvalidIndexState returns for invalid index state.
	ErrInvalidIndexState = terror.ClassDDL.New(codeInvalidIndexState, "invalid index state")
	// ErrInvalidForeignKeyState returns for invalid foreign key state.
	ErrInvalidForeignKeyState = terror.ClassDDL.New(codeInvalidForeignKeyState, "invalid foreign key state")

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = terror.ClassDDL.New(codeBadNull, "column cann't be null")
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = terror.ClassDDL.New(codeCantRemoveAllFields, "can't delete all columns with ALTER TABLE")
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = terror.ClassDDL.New(codeCantDropFieldOrKey, "can't drop field; check that column/key exists")
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = terror.ClassDDL.New(codeInvalidOnUpdate, "invalid ON UPDATE clause for the column")
	// ErrTooLongIdent returns for too long name of database/table/column.
	ErrTooLongIdent = terror.ClassDDL.New(codeTooLongIdent, "Identifier name too long")
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx context.Context, name model.CIStr, charsetInfo *ast.CharsetOpt) error
	DropSchema(ctx context.Context, schema model.CIStr) error
	CreateTable(ctx context.Context, ident ast.Ident, cols []*ast.ColumnDef,
		constrs []*ast.Constraint, options []*ast.TableOption) error
	DropTable(ctx context.Context, tableIdent ast.Ident) (err error)
	CreateIndex(ctx context.Context, tableIdent ast.Ident, unique bool, indexName model.CIStr,
		columnNames []*ast.IndexColName) error
	DropIndex(ctx context.Context, tableIdent ast.Ident, indexName model.CIStr) error
	GetInformationSchema() infoschema.InfoSchema
	AlterTable(ctx context.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx context.Context, tableIdent ast.Ident) error
	// SetLease will reset the lease time for online DDL change,
	// it's a very dangerous function and you must guarantee that all servers have the same lease time.
	SetLease(lease time.Duration)
	// GetLease returns current schema lease time.
	GetLease() time.Duration
	// Stats returns the DDL statistics.
	Stats() (map[string]interface{}, error)
	// GetScope gets the status variables scope.
	GetScope(status string) variable.ScopeFlag
	// Stop stops DDL worker.
	Stop() error
	// Start starts DDL worker.
	Start() error
}

type ddl struct {
	m sync.RWMutex

	infoHandle *infoschema.Handle
	hook       Callback
	hookMu     sync.RWMutex
	store      kv.Storage
	// Schema lease seconds.
	lease        time.Duration
	uuid         string
	ddlJobCh     chan struct{}
	ddlJobDoneCh chan struct{}
	// Drop database/table job that runs in the background.
	bgJobCh chan struct{}
	// reorgDoneCh is for reorganization, if the reorganization job is done,
	// we will use this channel to notify outer.
	// TODO: Now we use goroutine to simulate reorganization jobs, later we may
	// use a persistent job list.
	reorgDoneCh chan error

	quitCh chan struct{}
	wait   sync.WaitGroup
}

// NewDDL creates a new DDL.
func NewDDL(store kv.Storage, infoHandle *infoschema.Handle, hook Callback, lease time.Duration) DDL {
	return newDDL(store, infoHandle, hook, lease)
}

func newDDL(store kv.Storage, infoHandle *infoschema.Handle, hook Callback, lease time.Duration) *ddl {
	if hook == nil {
		hook = &BaseCallback{}
	}

	d := &ddl{
		infoHandle:   infoHandle,
		hook:         hook,
		store:        store,
		lease:        lease,
		uuid:         uuid.NewV4().String(),
		ddlJobCh:     make(chan struct{}, 1),
		ddlJobDoneCh: make(chan struct{}, 1),
		bgJobCh:      make(chan struct{}, 1),
	}

	d.start()

	variable.RegisterStatistics(d)

	return d
}

func (d *ddl) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()

	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		owner, err1 := t.GetDDLJobOwner()
		if err1 != nil {
			return errors.Trace(err1)
		}
		if owner == nil || owner.OwnerID != d.uuid {
			return nil
		}

		// DDL job's owner is me, clean it so other servers can complete it quickly.
		return t.SetDDLJobOwner(&model.Owner{})
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		owner, err1 := t.GetBgJobOwner()
		if err1 != nil {
			return errors.Trace(err1)
		}
		if owner == nil || owner.OwnerID != d.uuid {
			return nil
		}

		// Background job's owner is me, clean it so other servers can complete it quickly.
		return t.SetBgJobOwner(&model.Owner{})
	})

	return errors.Trace(err)
}

func (d *ddl) Start() error {
	d.m.Lock()
	defer d.m.Unlock()

	if !d.isClosed() {
		return nil
	}

	d.start()

	return nil
}

func (d *ddl) start() {
	d.quitCh = make(chan struct{})
	d.wait.Add(2)
	go d.onBackgroundWorker()
	go d.onDDLWorker()
	// For every start, we will send a fake job to let worker
	// check owner firstly and try to find whether a job exists and run.
	asyncNotify(d.ddlJobCh)
	asyncNotify(d.bgJobCh)
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

func (d *ddl) SetLease(lease time.Duration) {
	d.m.Lock()
	defer d.m.Unlock()

	if lease == d.lease {
		return
	}

	log.Warnf("[ddl] change schema lease %s -> %s", d.lease, lease)

	if d.isClosed() {
		// If already closed, just set lease and return.
		d.lease = lease
		return
	}

	// Close the running worker and start again.
	d.close()
	d.lease = lease
	d.start()
}

func (d *ddl) GetLease() time.Duration {
	d.m.RLock()
	lease := d.lease
	d.m.RUnlock()
	return lease
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

func (d *ddl) CreateSchema(ctx context.Context, schema model.CIStr, charsetInfo *ast.CharsetOpt) (err error) {
	is := d.GetInformationSchema()
	_, ok := is.SchemaByName(schema)
	if ok {
		return errors.Trace(infoschema.ErrDatabaseExists.GenByArgs(schema))
	}

	if err = checkTooLongSchema(schema); err != nil {
		return errors.Trace(err)
	}

	schemaID, err := d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	dbInfo := &model.DBInfo{
		Name: schema,
	}
	if charsetInfo != nil {
		dbInfo.Charset = charsetInfo.Chs
		dbInfo.Collate = charsetInfo.Col
	} else {
		dbInfo.Charset, dbInfo.Collate = getDefaultCharsetAndCollate()
	}

	job := &model.Job{
		SchemaID:   schemaID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropSchema(ctx context.Context, schema model.CIStr) (err error) {
	is := d.GetInformationSchema()
	old, ok := is.SchemaByName(schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}

	job := &model.Job{
		SchemaID:   old.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func checkTooLongSchema(schema model.CIStr) error {
	if len(schema.L) > mysql.MaxDatabaseNameLength {
		return ErrTooLongIdent.Gen("too long schema %s", schema)
	}
	return nil
}

func checkTooLongTable(table model.CIStr) error {
	if len(table.L) > mysql.MaxTableNameLength {
		return ErrTooLongIdent.Gen("too long table %s", table)
	}
	return nil
}

func getDefaultCharsetAndCollate() (string, string) {
	// TODO: TableDefaultCharset-->DatabaseDefaultCharset-->SystemDefaultCharset.
	// TODO: Change TableOption parser to parse collate.
	// This is a tmp solution.
	return "utf8", "utf8_unicode_ci"
}

func setColumnFlagWithConstraint(colMap map[string]*table.Column, v *ast.Constraint) {
	switch v.Tp {
	case ast.ConstraintPrimaryKey:
		for _, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			c.Flag |= mysql.PriKeyFlag
			// Primary key can not be NULL.
			c.Flag |= mysql.NotNullFlag
		}
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		for i, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set
				// if unique index has multi columns,
				// the flag should be MultipleKeyFlag.
				// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
				if len(v.Keys) > 1 {
					c.Flag |= mysql.MultipleKeyFlag
				} else {
					c.Flag |= mysql.UniqueKeyFlag
				}
			}
		}
	case ast.ConstraintKey, ast.ConstraintIndex:
		for i, key := range v.Keys {
			c, ok := colMap[key.Column.Name.L]
			if !ok {
				continue
			}
			if i == 0 {
				// Only the first column can be set.
				c.Flag |= mysql.MultipleKeyFlag
			}
		}
	}
}

func (d *ddl) buildColumnsAndConstraints(ctx context.Context, colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint) ([]*table.Column, []*ast.Constraint, error) {
	var cols []*table.Column
	colMap := map[string]*table.Column{}
	for i, colDef := range colDefs {
		col, cts, err := d.buildColumnAndConstraint(ctx, i, colDef)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		col.State = model.StatePublic
		constraints = append(constraints, cts...)
		cols = append(cols, col)
		colMap[colDef.Name.Name.L] = col
	}
	// Traverse table Constraints and set col.flag.
	for _, v := range constraints {
		setColumnFlagWithConstraint(colMap, v)
	}
	return cols, constraints, nil
}

func (d *ddl) setCharsetCollationFlenDecimal(tp *types.FieldType) {
	if len(tp.Charset) == 0 {
		switch tp.Tp {
		case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			tp.Charset, tp.Collate = getDefaultCharsetAndCollate()
		default:
			tp.Charset = charset.CharsetBin
			tp.Collate = charset.CharsetBin
		}
	}
	// If flen is not assigned, assigned it by type.
	if tp.Flen == types.UnspecifiedLength {
		tp.Flen = mysql.GetDefaultFieldLength(tp.Tp)
	}
	if tp.Decimal == types.UnspecifiedLength {
		tp.Decimal = mysql.GetDefaultDecimal(tp.Tp)
	}
}

func (d *ddl) buildColumnAndConstraint(ctx context.Context, offset int,
	colDef *ast.ColumnDef) (*table.Column, []*ast.Constraint, error) {
	d.setCharsetCollationFlenDecimal(colDef.Tp)
	col, cts, err := columnDefToCol(ctx, offset, colDef)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return col, cts, nil
}

// columnDefToCol converts ColumnDef to Col and TableConstraints.
func columnDefToCol(ctx context.Context, offset int, colDef *ast.ColumnDef) (*table.Column, []*ast.Constraint, error) {
	constraints := []*ast.Constraint{}
	col := &table.Column{
		Offset:    offset,
		Name:      colDef.Name.Name,
		FieldType: *colDef.Tp,
	}

	// Check and set TimestampFlag and OnUpdateNowFlag.
	if col.Tp == mysql.TypeTimestamp {
		col.Flag |= mysql.TimestampFlag
		col.Flag |= mysql.OnUpdateNowFlag
		col.Flag |= mysql.NotNullFlag
	}

	setOnUpdateNow := false
	hasDefaultValue := false
	if colDef.Options != nil {
		keys := []*ast.IndexColName{
			{
				Column: colDef.Name,
				Length: colDef.Tp.Flen,
			},
		}
		for _, v := range colDef.Options {
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				col.Flag |= mysql.NotNullFlag
			case ast.ColumnOptionNull:
				col.Flag &= ^uint(mysql.NotNullFlag)
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionAutoIncrement:
				col.Flag |= mysql.AutoIncrementFlag
			case ast.ColumnOptionPrimaryKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintPrimaryKey, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.PriKeyFlag
			case ast.ColumnOptionUniq:
				constraint := &ast.Constraint{Tp: ast.ConstraintUniq, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ast.ColumnOptionIndex:
				constraint := &ast.Constraint{Tp: ast.ConstraintIndex, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
			case ast.ColumnOptionUniqIndex:
				constraint := &ast.Constraint{Tp: ast.ConstraintUniqIndex, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ast.ColumnOptionKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintKey, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
			case ast.ColumnOptionUniqKey:
				constraint := &ast.Constraint{Tp: ast.ConstraintUniqKey, Name: colDef.Name.Name.O, Keys: keys}
				constraints = append(constraints, constraint)
				col.Flag |= mysql.UniqueKeyFlag
			case ast.ColumnOptionDefaultValue:
				value, err := getDefaultValue(ctx, v, colDef.Tp.Tp, colDef.Tp.Decimal)
				if err != nil {
					return nil, nil, ErrColumnBadNull.Gen("invalid default value - %s", err)
				}
				col.DefaultValue = value
				hasDefaultValue = true
				removeOnUpdateNowFlag(col)
			case ast.ColumnOptionOnUpdate:
				if !evaluator.IsCurrentTimeExpr(v.Expr) {
					return nil, nil, ErrInvalidOnUpdate.Gen("invalid ON UPDATE for - %s", col.Name)
				}

				col.Flag |= mysql.OnUpdateNowFlag
				setOnUpdateNow = true
			case ast.ColumnOptionComment:
				value, err := plan.EvalAstExpr(v.Expr, ctx)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				col.Comment, err = value.ToString()
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
			case ast.ColumnOptionFulltext:
				// Do nothing.
			}
		}
	}

	setTimestampDefaultValue(col, hasDefaultValue, setOnUpdateNow)

	// Set `NoDefaultValueFlag` if this field doesn't have a default value and
	// it is `not null` and not an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	setNoDefaultValueFlag(col, hasDefaultValue)

	err := checkDefaultValue(col, hasDefaultValue)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if col.Charset == charset.CharsetBin {
		col.Flag |= mysql.BinaryFlag
	}
	return col, constraints, nil
}

func getDefaultValue(ctx context.Context, c *ast.ColumnOption, tp byte, fsp int) (interface{}, error) {
	if tp == mysql.TypeTimestamp || tp == mysql.TypeDatetime {
		vd, err := evaluator.GetTimeValue(ctx, c.Expr, tp, fsp)
		value := vd.GetValue()
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Value is nil means `default null`.
		if value == nil {
			return nil, nil
		}

		// If value is types.Time, convert it to string.
		if vv, ok := value.(types.Time); ok {
			return vv.String(), nil
		}

		return value, nil
	}
	v, err := plan.EvalAstExpr(c.Expr, ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if v.IsNull() {
		return nil, nil
	}
	return v.ToString()
}

func removeOnUpdateNowFlag(c *table.Column) {
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.Flag) {
		c.Flag &= ^uint(mysql.OnUpdateNowFlag)
	}
}

func setTimestampDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	if hasDefaultValue {
		return
	}

	// For timestamp Col, if is not set default value or not set null, use current timestamp.
	if mysql.HasTimestampFlag(c.Flag) && mysql.HasNotNullFlag(c.Flag) {
		if setOnUpdateNow {
			c.DefaultValue = evaluator.ZeroTimestamp
		} else {
			c.DefaultValue = evaluator.CurrentTimestamp
		}
	}
}

func setNoDefaultValueFlag(c *table.Column, hasDefaultValue bool) {
	if hasDefaultValue {
		return
	}

	if !mysql.HasNotNullFlag(c.Flag) {
		return
	}

	// Check if it is an `AUTO_INCREMENT` field or `TIMESTAMP` field.
	if !mysql.HasAutoIncrementFlag(c.Flag) && !mysql.HasTimestampFlag(c.Flag) {
		c.Flag |= mysql.NoDefaultValueFlag
	}
}

func checkDefaultValue(c *table.Column, hasDefaultValue bool) error {
	if !hasDefaultValue {
		return nil
	}

	if c.DefaultValue != nil {
		return nil
	}

	// Set not null but default null is invalid.
	if mysql.HasNotNullFlag(c.Flag) {
		return ErrColumnBadNull.Gen("invalid default value for %s", c.Name)
	}

	return nil
}

func checkDuplicateColumn(colDefs []*ast.ColumnDef) error {
	colNames := map[string]bool{}
	for _, colDef := range colDefs {
		nameLower := colDef.Name.Name.O
		if colNames[nameLower] {
			return infoschema.ErrColumnExists.GenByArgs(colDef.Name.Name)
		}
		colNames[nameLower] = true
	}
	return nil
}

func checkTooLongColumn(colDefs []*ast.ColumnDef) error {
	for _, colDef := range colDefs {
		if len(colDef.Name.Name.O) > mysql.MaxColumnNameLength {
			return ErrTooLongIdent.Gen("too long column %s", colDef.Name.Name)
		}
	}
	return nil
}

func checkDuplicateConstraint(namesMap map[string]bool, name string, foreign bool) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		if foreign {
			return infoschema.ErrCannotAddForeign
		}
		return errDupKeyName.Gen("duplicate key name %s", name)
	}
	namesMap[nameLower] = true
	return nil
}

func setEmptyConstraintName(namesMap map[string]bool, constr *ast.Constraint, foreign bool) {
	if constr.Name == "" && len(constr.Keys) > 0 {
		colName := constr.Keys[0].Column.Name.L
		constrName := colName
		i := 2
		for namesMap[constrName] {
			// We loop forever until we find constrName that haven't been used.
			if foreign {
				constrName = fmt.Sprintf("fk_%s_%d", colName, i)
			} else {
				constrName = fmt.Sprintf("%s_%d", colName, i)
			}
			i++
		}
		constr.Name = constrName
		namesMap[constrName] = true
	}
}

func (d *ddl) checkConstraintNames(constraints []*ast.Constraint) error {
	constrNames := map[string]bool{}
	fkNames := map[string]bool{}

	// Check not empty constraint name whether is duplicated.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			err := checkDuplicateConstraint(fkNames, constr.Name, true)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			err := checkDuplicateConstraint(constrNames, constr.Name, false)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// Set empty constraint names.
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			setEmptyConstraintName(fkNames, constr, true)
		} else {
			setEmptyConstraintName(constrNames, constr, false)
		}
	}

	return nil
}

func (d *ddl) buildTableInfo(tableName model.CIStr, cols []*table.Column, constraints []*ast.Constraint) (tbInfo *model.TableInfo, err error) {
	tbInfo = &model.TableInfo{
		Name: tableName,
	}
	tbInfo.ID, err = d.genGlobalID()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, v := range cols {
		v.ID = allocateColumnID(tbInfo)
		tbInfo.Columns = append(tbInfo.Columns, v.ToInfo())
	}
	for _, constr := range constraints {
		if constr.Tp == ast.ConstraintForeignKey {
			for _, fk := range tbInfo.ForeignKeys {
				if fk.Name.L == strings.ToLower(constr.Name) {
					return nil, infoschema.ErrCannotAddForeign
				}
			}
			var fk model.FKInfo
			fk.Name = model.NewCIStr(constr.Name)
			fk.RefTable = constr.Refer.Table.Name
			fk.State = model.StatePublic
			for _, key := range constr.Keys {
				fk.Cols = append(fk.Cols, key.Column.Name)
			}
			for _, key := range constr.Refer.IndexColNames {
				fk.RefCols = append(fk.RefCols, key.Column.Name)
			}
			fk.OnDelete = int(constr.Refer.OnDelete.ReferOpt)
			fk.OnUpdate = int(constr.Refer.OnUpdate.ReferOpt)
			if len(fk.Cols) != len(fk.RefCols) {
				return nil, infoschema.ErrForeignKeyNotMatch
			}
			if len(fk.Cols) == 0 {
				// TODO: In MySQL, this case will report a parse error.
				return nil, infoschema.ErrCannotAddForeign
			}
			tbInfo.ForeignKeys = append(tbInfo.ForeignKeys, &fk)
			continue
		}
		if constr.Tp == ast.ConstraintPrimaryKey {
			if len(constr.Keys) == 1 {
				key := constr.Keys[0]
				col := table.FindCol(cols, key.Column.Name.O)
				if col == nil {
					return nil, errKeyColumnDoesNotExits.Gen("key column %s doesn't exist in table", key.Column.Name)
				}
				switch col.Tp {
				case mysql.TypeLong, mysql.TypeLonglong,
					mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
					tbInfo.PKIsHandle = true
					// Avoid creating index for PK handle column.
					continue
				}
			}
		}

		// 1. check if the column is exists
		// 2. add index
		indexColumns := make([]*model.IndexColumn, 0, len(constr.Keys))
		for _, key := range constr.Keys {
			col := table.FindCol(cols, key.Column.Name.O)
			if col == nil {
				return nil, errKeyColumnDoesNotExits.Gen("key column %s doesn't exist in table", key.Column.Name)
			}
			indexColumns = append(indexColumns, &model.IndexColumn{
				Name:   key.Column.Name,
				Offset: col.Offset,
				Length: key.Length,
			})
		}
		idxInfo := &model.IndexInfo{
			Name:    model.NewCIStr(constr.Name),
			Columns: indexColumns,
			State:   model.StatePublic,
		}
		switch constr.Tp {
		case ast.ConstraintPrimaryKey:
			idxInfo.Unique = true
			idxInfo.Primary = true
			idxInfo.Name = model.NewCIStr(table.PrimaryKeyName)
		case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			idxInfo.Unique = true
		}
		if constr.Option != nil {
			idxInfo.Comment = constr.Option.Comment
			idxInfo.Tp = constr.Option.Tp
		} else {
			// Use btree as default index type.
			idxInfo.Tp = model.IndexTypeBtree
		}
		idxInfo.ID = allocateIndexID(tbInfo)
		tbInfo.Indices = append(tbInfo.Indices, idxInfo)
	}
	return
}

func (d *ddl) CreateTable(ctx context.Context, ident ast.Ident, colDefs []*ast.ColumnDef,
	constraints []*ast.Constraint, options []*ast.TableOption) (err error) {
	is := d.GetInformationSchema()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ident.Schema)
	}
	if is.TableExists(ident.Schema, ident.Name) {
		return errors.Trace(infoschema.ErrTableExists.GenByArgs(ident))
	}
	if err = checkTooLongTable(ident.Name); err != nil {
		return errors.Trace(err)
	}
	if err = checkDuplicateColumn(colDefs); err != nil {
		return errors.Trace(err)
	}
	if err = checkTooLongColumn(colDefs); err != nil {
		return errors.Trace(err)
	}

	cols, newConstraints, err := d.buildColumnsAndConstraints(ctx, colDefs, constraints)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.checkConstraintNames(newConstraints)
	if err != nil {
		return errors.Trace(err)
	}

	tbInfo, err := d.buildTableInfo(ident.Name, cols, newConstraints)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbInfo},
	}

	d.handleTableOptions(options, tbInfo, schema.ID)
	err = d.doDDLJob(ctx, job)
	if err == nil {
		if tbInfo.AutoIncID > 1 {
			// Default tableAutoIncID base is 0.
			// If the first id is expected to greater than 1, we need to do rebase.
			d.handleAutoIncID(tbInfo, schema.ID)
		}
	}
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// If create table with auto_increment option, we should rebase tableAutoIncID value.
func (d *ddl) handleAutoIncID(tbInfo *model.TableInfo, schemaID int64) error {
	alloc := autoid.NewAllocator(d.store, schemaID)
	tbInfo.State = model.StatePublic
	tb, err := table.TableFromMeta(alloc, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	if err = tb.RebaseAutoID(tbInfo.AutoIncID-1, false); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Add create table options into TableInfo.
func (d *ddl) handleTableOptions(options []*ast.TableOption, tbInfo *model.TableInfo, schemaID int64) {
	for _, op := range options {
		switch op.Tp {
		case ast.TableOptionAutoIncrement:
			tbInfo.AutoIncID = int64(op.UintValue)
		case ast.TableOptionComment:
			tbInfo.Comment = op.StrValue
		case ast.TableOptionCharset:
			tbInfo.Charset = op.StrValue
		case ast.TableOptionCollate:
			tbInfo.Charset = op.StrValue
		}
	}
}

func (d *ddl) AlterTable(ctx context.Context, ident ast.Ident, specs []*ast.AlterTableSpec) (err error) {
	// Now we only allow one schema changing at the same time.
	if len(specs) != 1 {
		return errRunMultiSchemaChanges
	}

	for _, spec := range specs {
		switch spec.Tp {
		case ast.AlterTableAddColumn:
			err = d.AddColumn(ctx, ident, spec)
		case ast.AlterTableDropColumn:
			err = d.DropColumn(ctx, ident, spec.OldColumnName.Name)
		case ast.AlterTableDropIndex:
			err = d.DropIndex(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableAddConstraint:
			constr := spec.Constraint
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex:
				err = d.CreateIndex(ctx, ident, false, model.NewCIStr(constr.Name), spec.Constraint.Keys)
			case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				err = d.CreateIndex(ctx, ident, true, model.NewCIStr(constr.Name), spec.Constraint.Keys)
			case ast.ConstraintForeignKey:
				err = d.CreateForeignKey(ctx, ident, model.NewCIStr(constr.Name), spec.Constraint.Keys, spec.Constraint.Refer)
			default:
				// Nothing to do now.
			}
		case ast.AlterTableDropForeignKey:
			err = d.DropForeignKey(ctx, ident, model.NewCIStr(spec.Name))
		case ast.AlterTableModifyColumn:
			err = d.ModifyColumn(ctx, ident, spec)
		case ast.AlterTableChangeColumn:
			err = d.ChangeColumn(ctx, ident, spec)
		default:
			// Nothing to do now.
		}

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func checkColumnConstraint(constraints []*ast.ColumnOption) error {
	for _, constraint := range constraints {
		switch constraint.Tp {
		case ast.ColumnOptionAutoIncrement, ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniq, ast.ColumnOptionUniqKey:
			return errUnsupportedAddColumn.Gen("unsupported add column constraint - %v", constraint.Tp)
		}
	}

	return nil
}

// AddColumn will add a new column to the table.
func (d *ddl) AddColumn(ctx context.Context, ti ast.Ident, spec *ast.AlterTableSpec) error {
	// Check whether the added column constraints are supported.
	err := checkColumnConstraint(spec.NewColumn.Options)
	if err != nil {
		return errors.Trace(err)
	}

	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	// Check whether added column has existed.
	colName := spec.NewColumn.Name.Name.O
	col := table.FindCol(t.Cols(), colName)
	if col != nil {
		return infoschema.ErrColumnExists.GenByArgs(colName)
	}

	if len(colName) > mysql.MaxColumnNameLength {
		return ErrTooLongIdent.Gen("too long column %s", colName)
	}

	// Ingore table constraints now, maybe return error later.
	// We use length(t.Cols()) as the default offset firstly, later we will change the
	// column's offset later.
	col, _, err = d.buildColumnAndConstraint(ctx, len(t.Cols()), spec.NewColumn)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col, spec.Position, 0},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropColumn will drop a column from the table, now we don't support drop the column with index covered.
func (d *ddl) DropColumn(ctx context.Context, ti ast.Ident, colName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	// Check whether dropped column has existed.
	col := table.FindCol(t.Cols(), colName.L)
	if col == nil {
		return ErrCantDropFieldOrKey.Gen("column %s doesn't exist", colName)
	}

	tblInfo := t.Meta()
	// We don't support dropping column with index covered now.
	// We must drop the index first, then drop the column.
	if isColumnWithIndex(colName.L, tblInfo.Indices) {
		return errCantDropColWithIndex.Gen("can't drop column %s with index covered now", colName)
	}
	// We don't support dropping column with PK handle covered now.
	if col.IsPKHandleColumn(tblInfo) {
		return errUnsupportedPKHandle
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// Modifiable checks if the 'origin' type can be modified to 'to' type with out the need to
// change or check existing data in the table.
// It returns true if the two types has the same Charset and Collation, the same sign, both are
// integer types or string types, and new Flen and Decimal must be greater than or equal to origin.
func (d *ddl) modifiable(origin *types.FieldType, to *types.FieldType) bool {
	if to.Flen > 0 && to.Flen < origin.Flen {
		return false
	}
	if to.Decimal > 0 && to.Decimal < origin.Decimal {
		return false
	}
	if origin.Charset != to.Charset || origin.Collate != to.Collate {
		return false
	}
	if mysql.HasUnsignedFlag(uint(origin.Flag)) != mysql.HasUnsignedFlag(uint(to.Flag)) {
		return false
	}
	switch origin.Tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch to.Tp {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString,
			mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return true
		default:
			return false
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch to.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func (d *ddl) getModifiableColumnJob(ctx context.Context, ident ast.Ident, originalColName model.CIStr,
	spec *ast.AlterTableSpec) (*model.Job, error) {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return nil, errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ident.Schema, ident.Name)
	if err != nil {
		return nil, errors.Trace(infoschema.ErrTableNotExists)
	}

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenByArgs(originalColName, ident.Name)
	}
	if spec.Constraint != nil || (spec.Position != nil && spec.Position.Tp != ast.ColumnPositionNone) ||
		len(spec.NewColumn.Options) != 0 || spec.NewColumn.Tp == nil {
		// Make sure the column definition is simple field type.
		return nil, errUnsupportedModifyColumn
	}
	d.setCharsetCollationFlenDecimal(spec.NewColumn.Tp)
	if !d.modifiable(&col.FieldType, spec.NewColumn.Tp) {
		return nil, errUnsupportedModifyColumn
	}

	newCol := *col
	newCol.FieldType = *spec.NewColumn.Tp
	newCol.Name = spec.NewColumn.Name.Name
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionModifyColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{&newCol, originalColName},
	}
	return job, nil
}

// ChangeColumn renames an existing column and modifies the column's definition,
// currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ChangeColumn(ctx context.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	if len(spec.NewColumn.Name.Schema.O) != 0 && ident.Schema.L != spec.NewColumn.Name.Schema.L {
		return errWrongDBName.GenByArgs(spec.NewColumn.Name.Schema.O)
	}
	if len(spec.OldColumnName.Schema.O) != 0 && ident.Schema.L != spec.OldColumnName.Schema.L {
		return errWrongDBName.GenByArgs(spec.OldColumnName.Schema.O)
	}
	if len(spec.NewColumn.Name.Table.O) != 0 && ident.Name.L != spec.NewColumn.Name.Table.L {
		return errWrongTableName.GenByArgs(spec.NewColumn.Name.Table.O)
	}
	if len(spec.OldColumnName.Table.O) != 0 && ident.Name.L != spec.OldColumnName.Table.L {
		return errWrongTableName.GenByArgs(spec.OldColumnName.Table.O)
	}

	job, err := d.getModifiableColumnJob(ctx, ident, spec.OldColumnName.Name, spec)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// ModifyColumn does modification on an existing column, currently we only support limited kind of changes
// that do not need to change or check data on the table.
func (d *ddl) ModifyColumn(ctx context.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	if len(spec.NewColumn.Name.Schema.O) != 0 && ident.Schema.L != spec.NewColumn.Name.Schema.L {
		return errWrongDBName.GenByArgs(spec.NewColumn.Name.Schema.O)
	}
	if len(spec.NewColumn.Name.Table.O) != 0 && ident.Name.L != spec.NewColumn.Name.Table.L {
		return errWrongTableName.GenByArgs(spec.NewColumn.Name.Table.O)
	}

	originalColName := spec.NewColumn.Name.Name
	job, err := d.getModifiableColumnJob(ctx, ident, originalColName, spec)
	if err != nil {
		return errors.Trace(err)
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// DropTable will proceed even if some table in the list does not exists.
func (d *ddl) DropTable(ctx context.Context, ti ast.Ident) (err error) {
	is := d.GetInformationSchema()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}

	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenByArgs(ti))
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) TruncateTable(ctx context.Context, ti ast.Ident) error {
	is := d.GetInformationSchema()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}
	tb, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}
	newTableID, err := d.genGlobalID()
	if err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func getAnonymousIndex(t table.Table, colName model.CIStr) model.CIStr {
	id := 2
	l := len(t.Indices())
	indexName := colName
	for i := 0; i < l; i++ {
		if t.Indices()[i].Meta().Name.L == indexName.L {
			indexName = model.NewCIStr(fmt.Sprintf("%s_%d", colName.O, id))
			i = -1
			id++
		}
	}
	return indexName
}

func (d *ddl) CreateIndex(ctx context.Context, ti ast.Ident, unique bool, indexName model.CIStr, idxColNames []*ast.IndexColName) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	// Deal with anonymous index.
	if len(indexName.L) == 0 {
		indexName = getAnonymousIndex(t, idxColNames[0].Column.Name)
	}

	if indexInfo := findIndexByName(indexName.L, t.Meta().Indices); indexInfo != nil {
		return errDupKeyName.Gen("index already exist %s", indexName)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, idxColNames},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) buildFKInfo(fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef) (*model.FKInfo, error) {
	var fkInfo model.FKInfo
	fkInfo.Name = fkName
	fkInfo.RefTable = refer.Table.Name

	fkInfo.Cols = make([]model.CIStr, len(keys))
	for i, key := range keys {
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.RefCols = make([]model.CIStr, len(refer.IndexColNames))
	for i, key := range refer.IndexColNames {
		fkInfo.RefCols[i] = key.Column.Name
	}

	fkInfo.OnDelete = int(refer.OnDelete.ReferOpt)
	fkInfo.OnUpdate = int(refer.OnUpdate.ReferOpt)

	return &fkInfo, nil

}

func (d *ddl) CreateForeignKey(ctx context.Context, ti ast.Ident, fkName model.CIStr, keys []*ast.IndexColName, refer *ast.ReferenceDef) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	fkInfo, err := d.buildFKInfo(fkName, keys, refer)
	if err != nil {
		return errors.Trace(err)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionAddForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkInfo},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)

}

func (d *ddl) DropForeignKey(ctx context.Context, ti ast.Ident, fkName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenByArgs(ti.Schema)
	}

	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropForeignKey,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{fkName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) DropIndex(ctx context.Context, ti ast.Ident, indexName model.CIStr) error {
	is := d.infoHandle.Get()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists)
	}

	if indexInfo := findIndexByName(indexName.L, t.Meta().Indices); indexInfo == nil {
		return ErrCantDropFieldOrKey.Gen("index %s doesn't exist", indexName)
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    t.Meta().ID,
		Type:       model.ActionDropIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexName},
	}

	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) doDDLJob(ctx context.Context, job *model.Job) error {
	// For every DDL, we must commit current transaction.
	if err := ctx.CommitTxn(); err != nil {
		return errors.Trace(err)
	}

	// Get a global job ID and put the DDL job in the queue.
	err := d.addDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}

	// Notice worker that we push a new job and wait the job done.
	asyncNotify(d.ddlJobCh)
	log.Infof("[ddl] start DDL job %s", job)

	var historyJob *model.Job
	jobID := job.ID
	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, 10*time.Second))
	startTime := time.Now()
	jobsGauge.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		jobsGauge.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String()).Dec()
		retLabel := handleJobSucc
		if err != nil {
			retLabel = handleJobFailed
		}
		handleJobHistogram.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String(),
			retLabel).Observe(time.Since(startTime).Seconds())
	}()
	for {
		select {
		case <-d.ddlJobDoneCh:
		case <-ticker.C:
		}

		historyJob, err = d.getHistoryDDLJob(jobID)
		if err != nil {
			log.Errorf("[ddl] get history DDL job err %v, check again", err)
			continue
		} else if historyJob == nil {
			log.Warnf("[ddl] DDL job %d is not in history, maybe not run", jobID)
			continue
		}

		// If a job is a history job, the state must be JobDone or JobCancel.
		if historyJob.State == model.JobDone {
			log.Infof("[ddl] DDL job %d is finished", jobID)
			return nil
		}

		return errors.Trace(historyJob.Error)
	}
}

func (d *ddl) callHookOnChanged(err error) error {
	d.hookMu.Lock()
	defer d.hookMu.Unlock()

	err = d.hook.OnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) setHook(h Callback) {
	d.hookMu.Lock()
	defer d.hookMu.Unlock()

	d.hook = h
}

// findCol finds column in cols by name.
func findCol(cols []*model.ColumnInfo, name string) *model.ColumnInfo {
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}

// DDL error codes.
const (
	codeInvalidWorker         terror.ErrCode = 1
	codeNotOwner                             = 2
	codeInvalidDDLJob                        = 3
	codeInvalidBgJob                         = 4
	codeInvalidJobFlag                       = 5
	codeRunMultiSchemaChanges                = 6
	codeWaitReorgTimeout                     = 7
	codeInvalidStoreVer                      = 8

	codeInvalidDBState         = 100
	codeInvalidTableState      = 101
	codeInvalidColumnState     = 102
	codeInvalidIndexState      = 103
	codeInvalidForeignKeyState = 104

	codeCantDropColWithIndex    = 201
	codeUnsupportedAddColumn    = 202
	codeUnsupportedModifyColumn = 203
	codeUnsupportedDropPKHandle = 204

	codeBadNull               = 1048
	codeTooLongIdent          = 1059
	codeDupKeyName            = 1061
	codeTooLongKey            = 1071
	codeKeyColumnDoesNotExits = 1072
	codeIncorrectPrefixKey    = 1089
	codeCantRemoveAllFields   = 1090
	codeCantDropFieldOrKey    = 1091
	codeWrongDBName           = 1102
	codeWrongTableName        = 1103
	codeBlobKeyWithoutLength  = 1170
	codeInvalidOnUpdate       = 1294
)

func init() {
	ddlMySQLErrCodes := map[terror.ErrCode]uint16{
		codeBadNull:               mysql.ErrBadNull,
		codeCantRemoveAllFields:   mysql.ErrCantRemoveAllFields,
		codeCantDropFieldOrKey:    mysql.ErrCantDropFieldOrKey,
		codeInvalidOnUpdate:       mysql.ErrInvalidOnUpdate,
		codeBlobKeyWithoutLength:  mysql.ErrBlobKeyWithoutLength,
		codeIncorrectPrefixKey:    mysql.ErrWrongSubKey,
		codeTooLongIdent:          mysql.ErrTooLongIdent,
		codeTooLongKey:            mysql.ErrTooLongKey,
		codeKeyColumnDoesNotExits: mysql.ErrKeyColumnDoesNotExits,
		codeDupKeyName:            mysql.ErrDupKeyName,
		codeWrongDBName:           mysql.ErrWrongDBName,
		codeWrongTableName:        mysql.ErrWrongTableName,
	}
	terror.ErrClassToMySQLCodes[terror.ClassDDL] = ddlMySQLErrCodes
}

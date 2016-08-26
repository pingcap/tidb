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

package server

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// TiDBDriver implements IDriver.
type TiDBDriver struct {
	store kv.Storage
}

// NewTiDBDriver creates a new TiDBDriver.
func NewTiDBDriver(store kv.Storage) *TiDBDriver {
	driver := &TiDBDriver{
		store: store,
	}
	return driver
}

// TiDBContext implements IContext.
type TiDBContext struct {
	session      tidb.Session
	currentDB    string
	warningCount uint16
	stmts        map[int]*TiDBStatement
}

// TiDBStatement implements IStatement.
type TiDBStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	ctx         *TiDBContext
}

// ID implements IStatement ID method.
func (ts *TiDBStatement) ID() int {
	return int(ts.id)
}

// Execute implements IStatement Execute method.
func (ts *TiDBStatement) Execute(args ...interface{}) (rs ResultSet, err error) {
	tidbRecordset, err := ts.ctx.session.ExecutePreparedStmt(ts.id, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tidbRecordset == nil {
		return
	}
	rs = &tidbResultSet{
		recordSet: tidbRecordset,
	}
	return
}

// AppendParam implements IStatement AppendParam method.
func (ts *TiDBStatement) AppendParam(paramID int, data []byte) error {
	if paramID >= len(ts.boundParams) {
		return mysql.NewErr(mysql.ErrWrongArguments, "stmt_send_longdata")
	}
	ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
	return nil
}

// NumParams implements IStatement NumParams method.
func (ts *TiDBStatement) NumParams() int {
	return ts.numParams
}

// BoundParams implements IStatement BoundParams method.
func (ts *TiDBStatement) BoundParams() [][]byte {
	return ts.boundParams
}

// Reset implements IStatement Reset method.
func (ts *TiDBStatement) Reset() {
	for i := range ts.boundParams {
		ts.boundParams[i] = nil
	}
}

// Close implements IStatement Close method.
func (ts *TiDBStatement) Close() error {
	//TODO close at tidb level
	err := ts.ctx.session.DropPreparedStmt(ts.id)
	if err != nil {
		return errors.Trace(err)
	}
	delete(ts.ctx.stmts, int(ts.id))
	return nil
}

// OpenCtx implements IDriver.
func (qd *TiDBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string) (IContext, error) {
	session, _ := tidb.CreateSession(qd.store)
	session.SetClientCapability(capability)
	session.SetConnectionID(connID)
	if dbname != "" {
		_, err := session.Execute("use " + dbname)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	tc := &TiDBContext{
		session:   session,
		currentDB: dbname,
		stmts:     make(map[int]*TiDBStatement),
	}
	return tc, nil
}

// Status implements IContext Status method.
func (tc *TiDBContext) Status() uint16 {
	return tc.session.Status()
}

// LastInsertID implements IContext LastInsertID method.
func (tc *TiDBContext) LastInsertID() uint64 {
	return tc.session.LastInsertID()
}

// Value implements IContext Value method.
func (tc *TiDBContext) Value(key fmt.Stringer) interface{} {
	return tc.session.Value(key)
}

// SetValue implements IContext SetValue method.
func (tc *TiDBContext) SetValue(key fmt.Stringer, value interface{}) {
	tc.session.SetValue(key, value)
}

// CommitTxn implements IContext CommitTxn method.
func (tc *TiDBContext) CommitTxn() error {
	return tc.session.CommitTxn()
}

// RollbackTxn implements IContext RollbackTxn method.
func (tc *TiDBContext) RollbackTxn() error {
	return tc.session.RollbackTxn()
}

// AffectedRows implements IContext AffectedRows method.
func (tc *TiDBContext) AffectedRows() uint64 {
	return tc.session.AffectedRows()
}

// CurrentDB implements IContext CurrentDB method.
func (tc *TiDBContext) CurrentDB() string {
	return tc.currentDB
}

// WarningCount implements IContext WarningCount method.
func (tc *TiDBContext) WarningCount() uint16 {
	return tc.warningCount
}

// Execute implements IContext Execute method.
func (tc *TiDBContext) Execute(sql string) (rs []ResultSet, err error) {
	rsList, err := tc.session.Execute(sql)
	if err != nil {
		return
	}
	if len(rsList) == 0 { // result ok
		return
	}
	rs = make([]ResultSet, len(rsList))
	for i := 0; i < len(rsList); i++ {
		rs[i] = &tidbResultSet{
			recordSet: rsList[i],
		}
	}
	return
}

// SetClientCapability implements IContext SetClientCapability method.
func (tc *TiDBContext) SetClientCapability(flags uint32) {
	tc.session.SetClientCapability(flags)
}

// Close implements IContext Close method.
func (tc *TiDBContext) Close() (err error) {
	return tc.session.Close()
}

// Auth implements IContext Auth method.
func (tc *TiDBContext) Auth(user string, auth []byte, salt []byte) bool {
	return tc.session.Auth(user, auth, salt)
}

// FieldList implements IContext FieldList method.
func (tc *TiDBContext) FieldList(table string) (colums []*ColumnInfo, err error) {
	rs, err := tc.Execute("SELECT * FROM " + table + " LIMIT 0")
	if err != nil {
		return nil, errors.Trace(err)
	}
	colums, err = rs[0].Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

// GetStatement implements IContext GetStatement method.
func (tc *TiDBContext) GetStatement(stmtID int) IStatement {
	tcStmt := tc.stmts[stmtID]
	if tcStmt != nil {
		return tcStmt
	}
	return nil
}

// Prepare implements IContext Prepare method.
func (tc *TiDBContext) Prepare(sql string) (statement IStatement, columns, params []*ColumnInfo, err error) {
	stmtID, paramCount, fields, err := tc.session.PrepareStmt(sql)
	if err != nil {
		return
	}
	stmt := &TiDBStatement{
		id:          stmtID,
		numParams:   paramCount,
		boundParams: make([][]byte, paramCount),
		ctx:         tc,
	}
	statement = stmt
	columns = make([]*ColumnInfo, len(fields))
	for i := range fields {
		columns[i] = convertColumnInfo(fields[i])
	}
	params = make([]*ColumnInfo, paramCount)
	for i := range params {
		params[i] = &ColumnInfo{
			Type: mysql.TypeBlob,
		}
	}
	tc.stmts[int(stmtID)] = stmt
	return
}

type tidbResultSet struct {
	recordSet ast.RecordSet
}

func (trs *tidbResultSet) Next() ([]types.Datum, error) {
	row, err := trs.recordSet.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row != nil {
		return row.Data, nil
	}
	return nil, nil
}

func (trs *tidbResultSet) Close() error {
	return trs.recordSet.Close()
}

func (trs *tidbResultSet) Columns() ([]*ColumnInfo, error) {
	fields, err := trs.recordSet.Fields()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var columns []*ColumnInfo
	for _, v := range fields {
		columns = append(columns, convertColumnInfo(v))
	}
	return columns, nil
}

func convertColumnInfo(fld *ast.ResultField) (ci *ColumnInfo) {
	ci = new(ColumnInfo)
	ci.Name = fld.ColumnAsName.O
	ci.OrgName = fld.Column.Name.O
	ci.Table = fld.TableAsName.O
	if fld.Table != nil {
		ci.OrgTable = fld.Table.Name.O
	}
	ci.Schema = fld.DBName.O
	ci.Flag = uint16(fld.Column.Flag)
	ci.Charset = uint16(mysql.CharsetIDs[fld.Column.Charset])
	if fld.Column.Flen == types.UnspecifiedLength {
		ci.ColumnLength = 0
	} else {
		ci.ColumnLength = uint32(fld.Column.Flen)
	}
	if fld.Column.Decimal == types.UnspecifiedLength {
		ci.Decimal = 0
	} else {
		ci.Decimal = uint8(fld.Column.Decimal)
	}
	ci.Type = uint8(fld.Column.Tp)

	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	if ci.Type == mysql.TypeVarchar {
		ci.Type = mysql.TypeVarString
	}
	return
}

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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	mysql "github.com/pingcap/tidb/mysqldef"
)

// TiDBDriver implements IDriver
type TiDBDriver struct {
	store kv.Storage
}

// NewTiDBDriver creates a new TiDBDriver
func NewTiDBDriver(store kv.Storage) *TiDBDriver {
	driver := &TiDBDriver{
		store: store,
	}
	return driver
}

// TiDBContext implements IContext
type TiDBContext struct {
	session      tidb.Session
	currentDB    string
	warningCount uint16
	stmts        map[int]*TiDBStatement
}

// TiDBStatement implements IStatement
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
func (ts *TiDBStatement) Execute(args ...interface{}) (rs *ResultSet, err error) {
	tidbRecordset, err := ts.ctx.session.ExecutePreparedStmt(ts.id, args...)
	if err != nil {
		return nil, err
	}
	if tidbRecordset == nil {
		return
	}
	rs = new(ResultSet)
	fields, err := tidbRecordset.Fields()
	if err != nil {
		return
	}
	rs.Rows, err = tidbRecordset.Rows(-1, 0)
	fields, err = tidbRecordset.Fields()
	if err != nil {
		return
	}
	for _, v := range fields {
		rs.Columns = append(rs.Columns, convertColumnInfo(v))
	}
	if err != nil {
		return
	}
	return
}

// AppendParam implements IStatement AppendParam method.
func (ts *TiDBStatement) AppendParam(paramID int, data []byte) error {
	if paramID >= len(ts.boundParams) {
		return mysql.NewDefaultError(mysql.ErWrongArguments, "stmt_send_longdata")
	}
	ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
	return nil
}

// NumParams implements IStatement  NumParams method.
func (ts *TiDBStatement) NumParams() int {
	return ts.numParams
}

// BoundParams implements IStatement  BoundParams method.
func (ts *TiDBStatement) BoundParams() [][]byte {
	return ts.boundParams
}

// Reset implements IStatement  Reset method.
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
		return err
	}
	delete(ts.ctx.stmts, int(ts.id))
	return nil
}

// OpenCtx implements IDriver
func (qd *TiDBDriver) OpenCtx(capability uint32, collation uint8, dbname string) (IContext, error) {
	session, _ := tidb.CreateSession(qd.store)
	session.SetClientCapability(capability)
	if dbname != "" {
		_, err := session.Execute("use " + dbname)
		if err != nil {
			return nil, err
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

// LastInsertID implements IContext Status method.
func (tc *TiDBContext) LastInsertID() uint64 {
	return tc.session.LastInsertID()
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
func (tc *TiDBContext) Execute(sql string) (rs *ResultSet, err error) {
	qrsList, err := tc.session.Execute(sql)
	if err != nil {
		return
	}
	if len(qrsList) == 0 { // result ok
		return
	}
	qrs := qrsList[0]

	rs = new(ResultSet)
	fields, err := qrs.Fields()
	if err != nil {
		return
	}

	rs.Rows, err = qrs.Rows(-1, 0)
	if err != nil {
		return
	}
	fields, err = qrs.Fields()
	if err != nil {
		return
	}
	for _, v := range fields {
		rs.Columns = append(rs.Columns, convertColumnInfo(v))
	}
	return
}

// Close implements IContext Close method.
func (tc *TiDBContext) Close() (err error) {
	return tc.session.Close()
}

// FieldList implements IContext FieldList method.
func (tc *TiDBContext) FieldList(table string) (colums []*ColumnInfo, err error) {
	rs, err := tc.Execute("SELECT * FROM " + table + " LIMIT 0")
	if err != nil {
		return
	}
	colums = rs.Columns
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

func convertColumnInfo(qlfield *field.ResultField) (ci *ColumnInfo) {
	ci = new(ColumnInfo)
	ci.Schema = ""
	ci.Flag = uint16(qlfield.Flag)
	ci.Name = qlfield.Name
	ci.Table = qlfield.TableName
	ci.Charset = uint16(mysql.CharsetIDs[qlfield.Charset])
	ci.ColumnLength = uint32(qlfield.Flen)
	ci.Type = uint8(qlfield.Tp)
	return
}

// CreateTiDBTestDatabase creates test and gotest database for test usage.
func CreateTiDBTestDatabase(store kv.Storage) {
	td := NewTiDBDriver(store)
	tc, err := td.OpenCtx(defaultCapability, mysql.DefaultCollationID, "")
	if err != nil {
		log.Fatal(err)
	}
	tc.Execute("CREATE DATABASE IF NOT EXISTS test")
	tc.Execute("CREATE DATABASE IF NOT EXISTS gotest")
	tc.Close()
}

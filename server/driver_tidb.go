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
	"crypto/tls"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/chunk"
	goctx "golang.org/x/net/context"
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

// TiDBContext implements QueryCtx.
type TiDBContext struct {
	session   tidb.Session
	currentDB string
	stmts     map[int]*TiDBStatement
}

// TiDBStatement implements PreparedStatement.
type TiDBStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	paramsType  []byte
	ctx         *TiDBContext
}

// ID implements PreparedStatement ID method.
func (ts *TiDBStatement) ID() int {
	return int(ts.id)
}

// Execute implements PreparedStatement Execute method.
func (ts *TiDBStatement) Execute(goCtx goctx.Context, args ...interface{}) (rs ResultSet, err error) {
	tidbRecordset, err := ts.ctx.session.ExecutePreparedStmt(goCtx, ts.id, args...)
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

// AppendParam implements PreparedStatement AppendParam method.
func (ts *TiDBStatement) AppendParam(paramID int, data []byte) error {
	if paramID >= len(ts.boundParams) {
		return mysql.NewErr(mysql.ErrWrongArguments, "stmt_send_longdata")
	}
	ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
	return nil
}

// NumParams implements PreparedStatement NumParams method.
func (ts *TiDBStatement) NumParams() int {
	return ts.numParams
}

// BoundParams implements PreparedStatement BoundParams method.
func (ts *TiDBStatement) BoundParams() [][]byte {
	return ts.boundParams
}

// SetParamsType implements PreparedStatement SetParamsType method.
func (ts *TiDBStatement) SetParamsType(paramsType []byte) {
	ts.paramsType = paramsType
}

// GetParamsType implements PreparedStatement GetParamsType method.
func (ts *TiDBStatement) GetParamsType() []byte {
	return ts.paramsType
}

// Reset implements PreparedStatement Reset method.
func (ts *TiDBStatement) Reset() {
	for i := range ts.boundParams {
		ts.boundParams[i] = nil
	}
}

// Close implements PreparedStatement Close method.
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
func (qd *TiDBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (QueryCtx, error) {
	session, err := tidb.CreateSession(qd.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	session.SetTLSState(tlsState)
	err = session.SetCollation(int(collation))
	if err != nil {
		return nil, errors.Trace(err)
	}
	session.SetClientCapability(capability)
	session.SetConnectionID(connID)
	tc := &TiDBContext{
		session:   session,
		currentDB: dbname,
		stmts:     make(map[int]*TiDBStatement),
	}
	return tc, nil
}

// EnableChunk enables TiDBContext to use chunk.
func (tc *TiDBContext) EnableChunk() {
	tc.session.GetSessionVars().EnableChunk = true
}

// Status implements QueryCtx Status method.
func (tc *TiDBContext) Status() uint16 {
	return tc.session.Status()
}

// LastInsertID implements QueryCtx LastInsertID method.
func (tc *TiDBContext) LastInsertID() uint64 {
	return tc.session.LastInsertID()
}

// Value implements QueryCtx Value method.
func (tc *TiDBContext) Value(key fmt.Stringer) interface{} {
	return tc.session.Value(key)
}

// SetValue implements QueryCtx SetValue method.
func (tc *TiDBContext) SetValue(key fmt.Stringer, value interface{}) {
	tc.session.SetValue(key, value)
}

// CommitTxn implements QueryCtx CommitTxn method.
func (tc *TiDBContext) CommitTxn(goCtx goctx.Context) error {
	return tc.session.CommitTxn(goCtx)
}

// RollbackTxn implements QueryCtx RollbackTxn method.
func (tc *TiDBContext) RollbackTxn() error {
	return tc.session.RollbackTxn(goctx.TODO())
}

// AffectedRows implements QueryCtx AffectedRows method.
func (tc *TiDBContext) AffectedRows() uint64 {
	return tc.session.AffectedRows()
}

// CurrentDB implements QueryCtx CurrentDB method.
func (tc *TiDBContext) CurrentDB() string {
	return tc.currentDB
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *TiDBContext) WarningCount() uint16 {
	return tc.session.GetSessionVars().StmtCtx.WarningCount()
}

// Execute implements QueryCtx Execute method.
func (tc *TiDBContext) Execute(goCtx goctx.Context, sql string) (rs []ResultSet, err error) {
	rsList, err := tc.session.Execute(goCtx, sql)
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

// SetSessionManager implements the QueryCtx interface.
func (tc *TiDBContext) SetSessionManager(sm util.SessionManager) {
	tc.session.SetSessionManager(sm)
}

// SetClientCapability implements QueryCtx SetClientCapability method.
func (tc *TiDBContext) SetClientCapability(flags uint32) {
	tc.session.SetClientCapability(flags)
}

// Close implements QueryCtx Close method.
func (tc *TiDBContext) Close() error {
	tc.session.Close()
	return nil
}

// Auth implements QueryCtx Auth method.
func (tc *TiDBContext) Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool {
	return tc.session.Auth(user, auth, salt)
}

// FieldList implements QueryCtx FieldList method.
func (tc *TiDBContext) FieldList(table string) (columns []*ColumnInfo, err error) {
	fields, err := tc.session.FieldList(table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	columns = make([]*ColumnInfo, 0, len(fields))
	for _, f := range fields {
		columns = append(columns, convertColumnInfo(f))
	}
	return columns, nil
}

// GetStatement implements QueryCtx GetStatement method.
func (tc *TiDBContext) GetStatement(stmtID int) PreparedStatement {
	tcStmt := tc.stmts[stmtID]
	if tcStmt != nil {
		return tcStmt
	}
	return nil
}

// Prepare implements QueryCtx Prepare method.
func (tc *TiDBContext) Prepare(sql string) (statement PreparedStatement, columns, params []*ColumnInfo, err error) {
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

// ShowProcess implements QueryCtx ShowProcess method.
func (tc *TiDBContext) ShowProcess() util.ProcessInfo {
	return tc.session.ShowProcess()
}

type tidbResultSet struct {
	recordSet ast.RecordSet
	columns   []*ColumnInfo
}

func (trs *tidbResultSet) Next(goCtx goctx.Context) (types.Row, error) {
	return trs.recordSet.Next(goCtx)
}

func (trs *tidbResultSet) NewChunk() *chunk.Chunk {
	return trs.recordSet.NewChunk()
}

func (trs *tidbResultSet) NextChunk(ctx goctx.Context, chk *chunk.Chunk) error {
	return trs.recordSet.NextChunk(ctx, chk)
}

func (trs *tidbResultSet) SupportChunk() bool {
	return trs.recordSet.SupportChunk()
}

func (trs *tidbResultSet) Close() error {
	return trs.recordSet.Close()
}

func (trs *tidbResultSet) Columns() []*ColumnInfo {
	if trs.columns == nil {
		fields := trs.recordSet.Fields()
		for _, v := range fields {
			trs.columns = append(trs.columns, convertColumnInfo(v))
		}
	}
	return trs.columns
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
	if fld.Column.Tp == mysql.TypeNewDecimal {
		// Consider the negative sign.
		ci.ColumnLength++
		if fld.Column.Decimal > types.DefaultFsp {
			// Consider the decimal point.
			ci.ColumnLength++
		}
	} else if fld.Column.Tp != mysql.TypeBit {
		// Fix issue #4540.
		// The flen is a hint, not a precise value, so most client will not use the value.
		// But we found in race MySQL client, like Navicat for MySQL(version before 12) will truncate
		// the `show create table` result. To fix this case, we must use a large enough flen to prevent
		// the truncation, in MySQL, it will multiply bytes length by a multiple based on character set.
		// For examples:
		// * latin, the multiple is 1
		// * gb2312, the multiple is 2
		// * Utf-8, the multiple is 3
		// * utf8mb4, the multiple is 4
		// So the large enough multiple is 4 in here.
		ci.ColumnLength = ci.ColumnLength * mysql.MaxBytesOfCharacter
	}

	if fld.Column.Decimal == types.UnspecifiedLength {
		ci.Decimal = mysql.NotFixedDec
	} else {
		ci.Decimal = uint8(fld.Column.Decimal)
	}
	ci.Type = fld.Column.Tp

	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	if ci.Type == mysql.TypeVarchar {
		ci.Type = mysql.TypeVarString
	}
	return
}

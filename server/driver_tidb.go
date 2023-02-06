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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/sessionstates"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
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
	session.Session
	stmts map[int]*TiDBStatement
}

// TiDBStatement implements PreparedStatement.
type TiDBStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	paramsType  []byte
	ctx         *TiDBContext
	rs          ResultSet
	sql         string
}

// ID implements PreparedStatement ID method.
func (ts *TiDBStatement) ID() int {
	return int(ts.id)
}

// Execute implements PreparedStatement Execute method.
func (ts *TiDBStatement) Execute(ctx context.Context, args []expression.Expression) (rs ResultSet, err error) {
	tidbRecordset, err := ts.ctx.ExecutePreparedStmt(ctx, ts.id, args)
	if err != nil {
		return nil, err
	}
	if tidbRecordset == nil {
		return
	}
	rs = &tidbResultSet{
		recordSet:    tidbRecordset,
		preparedStmt: ts.ctx.GetSessionVars().PreparedStmts[ts.id].(*core.PlanCacheStmt),
	}
	return
}

// AppendParam implements PreparedStatement AppendParam method.
func (ts *TiDBStatement) AppendParam(paramID int, data []byte) error {
	if paramID >= len(ts.boundParams) {
		return mysql.NewErr(mysql.ErrWrongArguments, "stmt_send_longdata")
	}
	// If len(data) is 0, append an empty byte slice to the end to distinguish no data and no parameter.
	if len(data) == 0 {
		ts.boundParams[paramID] = []byte{}
	} else {
		ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
	}
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

// StoreResultSet stores ResultSet for stmt fetching
func (ts *TiDBStatement) StoreResultSet(rs ResultSet) {
	// refer to https://dev.mysql.com/doc/refman/5.7/en/cursor-restrictions.html
	// You can have open only a single cursor per prepared statement.
	// closing previous ResultSet before associating a new ResultSet with this statement
	// if it exists
	if ts.rs != nil {
		terror.Call(ts.rs.Close)
	}
	ts.rs = rs
}

// GetResultSet gets ResultSet associated this statement
func (ts *TiDBStatement) GetResultSet() ResultSet {
	return ts.rs
}

// Reset implements PreparedStatement Reset method.
func (ts *TiDBStatement) Reset() {
	for i := range ts.boundParams {
		ts.boundParams[i] = nil
	}

	// closing previous ResultSet if it exists
	if ts.rs != nil {
		terror.Call(ts.rs.Close)
		ts.rs = nil
	}
}

// Close implements PreparedStatement Close method.
func (ts *TiDBStatement) Close() error {
	// TODO close at tidb level
	if ts.ctx.GetSessionVars().TxnCtx != nil && ts.ctx.GetSessionVars().TxnCtx.CouldRetry {
		err := ts.ctx.DropPreparedStmt(ts.id)
		if err != nil {
			return err
		}
	} else {
		if ts.ctx.GetSessionVars().EnablePreparedPlanCache {
			preparedPointer := ts.ctx.GetSessionVars().PreparedStmts[ts.id]
			preparedObj, ok := preparedPointer.(*core.PlanCacheStmt)
			if !ok {
				return errors.Errorf("invalid PlanCacheStmt type")
			}
			bindSQL, _ := core.GetBindSQL4PlanCache(ts.ctx, preparedObj)
			cacheKey, err := core.NewPlanCacheKey(ts.ctx.GetSessionVars(), preparedObj.StmtText, preparedObj.StmtDB,
				preparedObj.PreparedAst.SchemaVersion, 0, bindSQL)
			if err != nil {
				return err
			}
			if !ts.ctx.GetSessionVars().IgnorePreparedCacheCloseStmt { // keep the plan in cache
				ts.ctx.GetPlanCache(false).Delete(cacheKey)
			}
		}
		ts.ctx.GetSessionVars().RemovePreparedStmt(ts.id)
	}
	delete(ts.ctx.stmts, int(ts.id))

	// close ResultSet associated with this statement
	if ts.rs != nil {
		terror.Call(ts.rs.Close)
	}
	return nil
}

// OpenCtx implements IDriver.
func (qd *TiDBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState, extensions *extension.SessionExtensions) (*TiDBContext, error) {
	se, err := session.CreateSession(qd.store)
	if err != nil {
		return nil, err
	}
	se.SetTLSState(tlsState)
	err = se.SetCollation(int(collation))
	if err != nil {
		return nil, err
	}
	se.SetClientCapability(capability)
	se.SetConnectionID(connID)
	tc := &TiDBContext{
		Session: se,
		stmts:   make(map[int]*TiDBStatement),
	}
	se.SetSessionStatesHandler(sessionstates.StatePrepareStmt, tc)
	se.SetExtensions(extensions)
	return tc, nil
}

// GetWarnings implements QueryCtx GetWarnings method.
func (tc *TiDBContext) GetWarnings() []stmtctx.SQLWarn {
	return tc.GetSessionVars().StmtCtx.GetWarnings()
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *TiDBContext) WarningCount() uint16 {
	return tc.GetSessionVars().StmtCtx.WarningCount()
}

func (tc *TiDBContext) checkSandBoxMode(stmt ast.StmtNode) error {
	if !tc.Session.GetSessionVars().InRestrictedSQL && tc.InSandBoxMode() {
		switch stmt.(type) {
		case *ast.SetPwdStmt, *ast.AlterUserStmt:
		default:
			return errMustChangePassword.GenWithStackByArgs()
		}
	}
	return nil
}

// ExecuteStmt implements QueryCtx interface.
func (tc *TiDBContext) ExecuteStmt(ctx context.Context, stmt ast.StmtNode) (ResultSet, error) {
	var rs sqlexec.RecordSet
	var err error
	if err = tc.checkSandBoxMode(stmt); err != nil {
		return nil, err
	}
	if s, ok := stmt.(*ast.NonTransactionalDMLStmt); ok {
		rs, err = session.HandleNonTransactionalDML(ctx, s, tc.Session)
	} else {
		rs, err = tc.Session.ExecuteStmt(ctx, stmt)
	}
	if err != nil {
		tc.Session.GetSessionVars().StmtCtx.AppendError(err)
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	return &tidbResultSet{
		recordSet: rs,
	}, nil
}

// Close implements QueryCtx Close method.
func (tc *TiDBContext) Close() error {
	// close PreparedStatement associated with this connection
	for _, v := range tc.stmts {
		terror.Call(v.Close)
	}

	tc.Session.Close()
	return nil
}

// FieldList implements QueryCtx FieldList method.
func (tc *TiDBContext) FieldList(table string) (columns []*ColumnInfo, err error) {
	fields, err := tc.Session.FieldList(table)
	if err != nil {
		return nil, err
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
	stmtID, paramCount, fields, err := tc.Session.PrepareStmt(sql)
	if err != nil {
		return
	}
	stmt := &TiDBStatement{
		sql:         sql,
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

// GetStmtStats implements the sessionctx.Context interface.
func (tc *TiDBContext) GetStmtStats() *stmtstats.StatementStats {
	return tc.Session.GetStmtStats()
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (tc *TiDBContext) EncodeSessionStates(ctx context.Context, sctx sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	sessionVars := tc.Session.GetSessionVars()
	sessionStates.PreparedStmts = make(map[uint32]*sessionstates.PreparedStmtInfo, len(sessionVars.PreparedStmts))
	for preparedID, preparedObj := range sessionVars.PreparedStmts {
		preparedStmt, ok := preparedObj.(*core.PlanCacheStmt)
		if !ok {
			return errors.Errorf("invalid CachedPreparedStmt type")
		}
		sessionStates.PreparedStmts[preparedID] = &sessionstates.PreparedStmtInfo{
			StmtText: preparedStmt.StmtText,
			StmtDB:   preparedStmt.StmtDB,
		}
	}
	for name, id := range sessionVars.PreparedStmtNameToID {
		// Only text protocol statements have names.
		if preparedStmtInfo, ok := sessionStates.PreparedStmts[id]; ok {
			preparedStmtInfo.Name = name
		}
	}
	for id, stmt := range tc.stmts {
		// Only binary protocol statements have paramTypes.
		preparedStmtInfo, ok := sessionStates.PreparedStmts[uint32(id)]
		if !ok {
			return errors.Errorf("prepared statement %d not found", id)
		}
		// Bound params are sent by CMD_STMT_SEND_LONG_DATA, the proxy can wait for COM_STMT_EXECUTE.
		for _, boundParam := range stmt.BoundParams() {
			if boundParam != nil {
				return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("prepared statements have bound params")
			}
		}
		if rs := stmt.GetResultSet(); rs != nil && !rs.IsClosed() {
			return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("prepared statements have open result sets")
		}
		preparedStmtInfo.ParamTypes = stmt.GetParamsType()
	}
	return nil
}

// DecodeSessionStates implements SessionStatesHandler.DecodeSessionStates interface.
func (tc *TiDBContext) DecodeSessionStates(ctx context.Context, sctx sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	if len(sessionStates.PreparedStmts) == 0 {
		return nil
	}
	sessionVars := tc.Session.GetSessionVars()
	savedPreparedStmtID := sessionVars.GetNextPreparedStmtID()
	savedCurrentDB := sessionVars.CurrentDB
	defer func() {
		sessionVars.SetNextPreparedStmtID(savedPreparedStmtID - 1)
		sessionVars.CurrentDB = savedCurrentDB
	}()

	for id, preparedStmtInfo := range sessionStates.PreparedStmts {
		// Set the next id and currentDB manually.
		sessionVars.SetNextPreparedStmtID(id - 1)
		sessionVars.CurrentDB = preparedStmtInfo.StmtDB
		if preparedStmtInfo.Name == "" {
			// Binary protocol: add to sessionVars.PreparedStmts and TiDBContext.stmts.
			stmt, _, _, err := tc.Prepare(preparedStmtInfo.StmtText)
			if err != nil {
				return err
			}
			// Only binary protocol uses paramsType, which is passed from the first COM_STMT_EXECUTE.
			stmt.SetParamsType(preparedStmtInfo.ParamTypes)
		} else {
			// Text protocol: add to sessionVars.PreparedStmts and sessionVars.PreparedStmtNameToID.
			stmtText := strings.ReplaceAll(preparedStmtInfo.StmtText, "\\", "\\\\")
			stmtText = strings.ReplaceAll(stmtText, "'", "\\'")
			// Add single quotes because the sql_mode might contain ANSI_QUOTES.
			sql := fmt.Sprintf("PREPARE `%s` FROM '%s'", preparedStmtInfo.Name, stmtText)
			stmts, err := tc.Parse(ctx, sql)
			if err != nil {
				return err
			}
			if _, err = tc.ExecuteStmt(ctx, stmts[0]); err != nil {
				return err
			}
		}
	}
	return nil
}

type tidbResultSet struct {
	recordSet    sqlexec.RecordSet
	columns      []*ColumnInfo
	rows         []chunk.Row
	closed       int32
	preparedStmt *core.PlanCacheStmt
}

func (trs *tidbResultSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	return trs.recordSet.NewChunk(alloc)
}

func (trs *tidbResultSet) Next(ctx context.Context, req *chunk.Chunk) error {
	return trs.recordSet.Next(ctx, req)
}

func (trs *tidbResultSet) StoreFetchedRows(rows []chunk.Row) {
	trs.rows = rows
}

func (trs *tidbResultSet) GetFetchedRows() []chunk.Row {
	if trs.rows == nil {
		trs.rows = make([]chunk.Row, 0, 1024)
	}
	return trs.rows
}

func (trs *tidbResultSet) Close() error {
	if !atomic.CompareAndSwapInt32(&trs.closed, 0, 1) {
		return nil
	}
	err := trs.recordSet.Close()
	trs.recordSet = nil
	return err
}

// IsClosed implements ResultSet.IsClosed interface.
func (trs *tidbResultSet) IsClosed() bool {
	return atomic.LoadInt32(&trs.closed) == 1
}

// OnFetchReturned implements fetchNotifier#OnFetchReturned
func (trs *tidbResultSet) OnFetchReturned() {
	if cl, ok := trs.recordSet.(fetchNotifier); ok {
		cl.OnFetchReturned()
	}
}

func (trs *tidbResultSet) Columns() []*ColumnInfo {
	if trs.columns != nil {
		return trs.columns
	}
	// for prepare statement, try to get cached columnInfo array
	if trs.preparedStmt != nil {
		ps := trs.preparedStmt
		if colInfos, ok := ps.ColumnInfos.([]*ColumnInfo); ok {
			trs.columns = colInfos
		}
	}
	if trs.columns == nil {
		fields := trs.recordSet.Fields()
		for _, v := range fields {
			trs.columns = append(trs.columns, convertColumnInfo(v))
		}
		if trs.preparedStmt != nil {
			// if ColumnInfo struct has allocated object,
			// here maybe we need deep copy ColumnInfo to do caching
			trs.preparedStmt.ColumnInfos = trs.columns
		}
	}
	return trs.columns
}

// rsWithHooks wraps a ResultSet with some hooks (currently only onClosed).
type rsWithHooks struct {
	ResultSet
	onClosed func()
}

// Close implements ResultSet#Close
func (rs *rsWithHooks) Close() error {
	closed := rs.IsClosed()
	err := rs.ResultSet.Close()
	if !closed && rs.onClosed != nil {
		rs.onClosed()
	}
	return err
}

// OnFetchReturned implements fetchNotifier#OnFetchReturned
func (rs *rsWithHooks) OnFetchReturned() {
	if impl, ok := rs.ResultSet.(fetchNotifier); ok {
		impl.OnFetchReturned()
	}
}

// Unwrap returns the underlying result set
func (rs *rsWithHooks) Unwrap() ResultSet {
	return rs.ResultSet
}

// unwrapResultSet likes errors.Cause but for ResultSet
func unwrapResultSet(rs ResultSet) ResultSet {
	var unRS ResultSet
	if u, ok := rs.(interface{ Unwrap() ResultSet }); ok {
		unRS = u.Unwrap()
	}
	if unRS == nil {
		return rs
	}
	return unwrapResultSet(unRS)
}

func convertColumnInfo(fld *ast.ResultField) (ci *ColumnInfo) {
	ci = &ColumnInfo{
		Name:         fld.ColumnAsName.O,
		OrgName:      fld.Column.Name.O,
		Table:        fld.TableAsName.O,
		Schema:       fld.DBName.O,
		Flag:         uint16(fld.Column.GetFlag()),
		Charset:      uint16(mysql.CharsetNameToID(fld.Column.GetCharset())),
		Type:         fld.Column.GetType(),
		DefaultValue: fld.Column.GetDefaultValue(),
	}

	if fld.Table != nil {
		ci.OrgTable = fld.Table.Name.O
	}
	if fld.Column.GetFlen() != types.UnspecifiedLength {
		ci.ColumnLength = uint32(fld.Column.GetFlen())
	}
	if fld.Column.GetType() == mysql.TypeNewDecimal {
		// Consider the negative sign.
		ci.ColumnLength++
		if fld.Column.GetDecimal() > types.DefaultFsp {
			// Consider the decimal point.
			ci.ColumnLength++
		}
	} else if types.IsString(fld.Column.GetType()) ||
		fld.Column.GetType() == mysql.TypeEnum || fld.Column.GetType() == mysql.TypeSet { // issue #18870
		// Fix issue #4540.
		// The flen is a hint, not a precise value, so most client will not use the value.
		// But we found in rare MySQL client, like Navicat for MySQL(version before 12) will truncate
		// the `show create table` result. To fix this case, we must use a large enough flen to prevent
		// the truncation, in MySQL, it will multiply bytes length by a multiple based on character set.
		// For examples:
		// * latin, the multiple is 1
		// * gb2312, the multiple is 2
		// * Utf-8, the multiple is 3
		// * utf8mb4, the multiple is 4
		// We used to check non-string types to avoid the truncation problem in some MySQL
		// client such as Navicat. Now we only allow string type enter this branch.
		charsetDesc, err := charset.GetCharsetInfo(fld.Column.GetCharset())
		if err != nil {
			ci.ColumnLength *= 4
		} else {
			ci.ColumnLength *= uint32(charsetDesc.Maxlen)
		}
	}

	if fld.Column.GetDecimal() == types.UnspecifiedLength {
		if fld.Column.GetType() == mysql.TypeDuration {
			ci.Decimal = uint8(types.DefaultFsp)
		} else {
			ci.Decimal = mysql.NotFixedDec
		}
	} else {
		ci.Decimal = uint8(fld.Column.GetDecimal())
	}

	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	if ci.Type == mysql.TypeVarchar {
		ci.Type = mysql.TypeVarString
	}
	return
}

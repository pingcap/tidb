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

// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

package server

import (
	"context"
	"encoding/binary"
	"runtime/trace"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/server/internal/dump"
	"github.com/pingcap/tidb/pkg/server/internal/parse"
	"github.com/pingcap/tidb/pkg/server/internal/resultset"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (cc *clientConn) HandleStmtPrepare(ctx context.Context, sql string) error {
	stmt, columns, params, err := cc.ctx.Prepare(sql)
	if err != nil {
		return err
	}
	data := make([]byte, 4, 128)

	// status ok
	data = append(data, 0)
	// stmt id
	data = dump.Uint32(data, uint32(stmt.ID()))
	// number columns
	data = dump.Uint16(data, uint16(len(columns)))
	// number params
	data = dump.Uint16(data, uint16(len(params)))
	// filter [00]
	data = append(data, 0)
	// warning count
	data = append(data, 0, 0) // TODO support warning count

	if err := cc.writePacket(data); err != nil {
		return err
	}

	cc.initResultEncoder(ctx)
	defer cc.rsEncoder.Clean()
	if len(params) > 0 {
		for i := 0; i < len(params); i++ {
			data = data[0:4]
			data = params[i].Dump(data, cc.rsEncoder)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if cc.capability&mysql.ClientDeprecateEOF == 0 {
			// metadata only needs EOF marker for old clients without ClientDeprecateEOF
			if err := cc.writeEOF(ctx, cc.ctx.Status()); err != nil {
				return err
			}
		}
	}

	if len(columns) > 0 {
		for i := 0; i < len(columns); i++ {
			data = data[0:4]
			data = columns[i].Dump(data, cc.rsEncoder)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if cc.capability&mysql.ClientDeprecateEOF == 0 {
			// metadata only needs EOF marker for old clients without ClientDeprecateEOF
			if err := cc.writeEOF(ctx, cc.ctx.Status()); err != nil {
				return err
			}
		}
	}
	return cc.flush(ctx)
}

func (cc *clientConn) handleStmtExecute(ctx context.Context, data []byte) (err error) {
	defer trace.StartRegion(ctx, "HandleStmtExecute").End()
	if len(data) < 9 {
		return mysql.ErrMalformPacket
	}
	pos := 0
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmt := cc.ctx.GetStatement(int(stmtID))
	if stmt == nil {
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_execute")
	}

	flag := data[pos]
	pos++
	// Please refer to https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	// The client indicates that it wants to use cursor by setting this flag.
	// Now we only support forward-only, read-only cursor.
	useCursor := false
	if flag&mysql.CursorTypeReadOnly > 0 {
		useCursor = true
	}
	if flag&mysql.CursorTypeForUpdate > 0 {
		return mysql.NewErrf(mysql.ErrUnknown, "unsupported flag: CursorTypeForUpdate", nil)
	}
	if flag&mysql.CursorTypeScrollable > 0 {
		return mysql.NewErrf(mysql.ErrUnknown, "unsupported flag: CursorTypeScrollable", nil)
	}

	if useCursor {
		cc.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
		defer cc.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, false)
	} else {
		// not using streaming ,can reuse chunk
		cc.ctx.GetSessionVars().SetAlloc(cc.chunkAlloc)
	}
	// skip iteration-count, always 1
	pos += 4

	var (
		nullBitmaps []byte
		paramTypes  []byte
		paramValues []byte
	)
	cc.initInputEncoder(ctx)
	numParams := stmt.NumParams()
	args := make([]param.BinaryParam, numParams)
	if numParams > 0 {
		nullBitmapLen := (numParams + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return mysql.ErrMalformPacket
		}
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		// new param bound flag
		if data[pos] == 1 {
			pos++
			if len(data) < (pos + (numParams << 1)) {
				return mysql.ErrMalformPacket
			}

			paramTypes = data[pos : pos+(numParams<<1)]
			pos += numParams << 1
			paramValues = data[pos:]
			// Just the first StmtExecute packet contain parameters type,
			// we need save it for further use.
			stmt.SetParamsType(paramTypes)
		} else {
			paramValues = data[pos+1:]
		}

		err = parseBinaryParams(args, stmt.BoundParams(), nullBitmaps, stmt.GetParamsType(), paramValues, cc.inputDecoder)
		// This `.Reset` resets the arguments, so it's fine to just ignore the error (and the it'll be reset again in the following routine)
		errReset := stmt.Reset()
		if errReset != nil {
			logutil.Logger(ctx).Warn("fail to reset statement in EXECUTE command", zap.Error(errReset))
		}
		if err != nil {
			return errors.Annotate(err, cc.preparedStmt2String(stmtID))
		}
	}

	sessVars := cc.ctx.GetSessionVars()
	// expiredTaskID is the task ID of the previous statement. When executing a stmt,
	// the StmtCtx will be reinit and the TaskID will change. We can compare the StmtCtx.TaskID
	// with the previous one to determine whether StmtCtx has been inited for the current stmt.
	expiredTaskID := sessVars.StmtCtx.TaskID
	err = cc.executePlanCacheStmt(ctx, stmt, args, useCursor)
	cc.onExtensionBinaryExecuteEnd(stmt, args, sessVars.StmtCtx.TaskID != expiredTaskID, err)
	return err
}

func (cc *clientConn) executePlanCacheStmt(ctx context.Context, stmt any, args []param.BinaryParam, useCursor bool) (err error) {
	ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, &execdetails.StmtExecDetails{})
	ctx = context.WithValue(ctx, util.ExecDetailsKey, &util.ExecDetails{})
	ctx = context.WithValue(ctx, util.RUDetailsCtxKey, util.NewRUDetails())
	retryable, err := cc.executePreparedStmtAndWriteResult(ctx, stmt.(PreparedStatement), args, useCursor)
	if err != nil {
		action, txnErr := sessiontxn.GetTxnManager(&cc.ctx).OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterQuery, err)
		if txnErr != nil {
			return txnErr
		}

		if retryable && action == sessiontxn.StmtActionRetryReady {
			cc.ctx.GetSessionVars().RetryInfo.Retrying = true
			_, err = cc.executePreparedStmtAndWriteResult(ctx, stmt.(PreparedStatement), args, useCursor)
			cc.ctx.GetSessionVars().RetryInfo.Retrying = false
			return err
		}
	}
	_, allowTiFlashFallback := cc.ctx.GetSessionVars().AllowFallbackToTiKV[kv.TiFlash]
	if allowTiFlashFallback && err != nil && errors.ErrorEqual(err, storeerr.ErrTiFlashServerTimeout) && retryable {
		// When the TiFlash server seems down, we append a warning to remind the user to check the status of the TiFlash
		// server and fallback to TiKV.
		prevErr := err
		delete(cc.ctx.GetSessionVars().IsolationReadEngines, kv.TiFlash)
		defer func() {
			cc.ctx.GetSessionVars().IsolationReadEngines[kv.TiFlash] = struct{}{}
		}()
		_, err = cc.executePreparedStmtAndWriteResult(ctx, stmt.(PreparedStatement), args, useCursor)
		// We append warning after the retry because `ResetContextOfStmt` may be called during the retry, which clears warnings.
		cc.ctx.GetSessionVars().StmtCtx.AppendError(prevErr)
	}
	return err
}

// The first return value indicates whether the call of executePreparedStmtAndWriteResult has no side effect and can be retried.
// Currently the first return value is used to fallback to TiKV when TiFlash is down.
func (cc *clientConn) executePreparedStmtAndWriteResult(ctx context.Context, stmt PreparedStatement, args []param.BinaryParam, useCursor bool) (bool, error) {
	vars := (&cc.ctx).GetSessionVars()
	prepStmt, err := vars.GetPreparedStmtByID(uint32(stmt.ID()))
	if err != nil {
		return true, errors.Annotate(err, cc.preparedStmt2String(uint32(stmt.ID())))
	}
	execStmt := &ast.ExecuteStmt{
		BinaryArgs: args,
		PrepStmt:   prepStmt,
	}

	// first, try to clear the left cursor if there is one
	if useCursor && stmt.GetCursorActive() {
		if stmt.GetResultSet() != nil && stmt.GetResultSet().GetRowContainerReader() != nil {
			stmt.GetResultSet().GetRowContainerReader().Close()
		}
		if stmt.GetRowContainer() != nil {
			stmt.GetRowContainer().GetMemTracker().Detach()
			stmt.GetRowContainer().GetDiskTracker().Detach()
			err := stmt.GetRowContainer().Close()
			if err != nil {
				logutil.Logger(ctx).Error(
					"Fail to close rowContainer before executing statement. May cause resource leak",
					zap.Error(err))
			}
			stmt.StoreRowContainer(nil)
		}
		stmt.StoreResultSet(nil)
		stmt.SetCursorActive(false)
	}

	// For the combination of `ComPrepare` and `ComExecute`, the statement name is stored in the client side, and the
	// TiDB only has the ID, so don't try to construct an `EXECUTE SOMETHING`. Use the original prepared statement here
	// instead.
	sql := ""
	planCacheStmt, ok := prepStmt.(*plannercore.PlanCacheStmt)
	if ok {
		sql = planCacheStmt.StmtText
	}
	execStmt.SetText(charset.EncodingUTF8Impl, sql)
	rs, err := (&cc.ctx).ExecuteStmt(ctx, execStmt)
	if rs != nil {
		defer rs.Close()
	}
	if err != nil {
		// If error is returned during the planner phase or the executor.Open
		// phase, the rs will be nil, and StmtCtx.MemTracker StmtCtx.DiskTracker
		// will not be detached. We need to detach them manually.
		if sv := cc.ctx.GetSessionVars(); sv != nil && sv.StmtCtx != nil {
			sv.StmtCtx.DetachMemDiskTracker()
		}
		return true, errors.Annotate(err, cc.preparedStmt2String(uint32(stmt.ID())))
	}

	if rs == nil {
		if useCursor {
			vars.SetStatusFlag(mysql.ServerStatusCursorExists, false)
		}
		return false, cc.writeOK(ctx)
	}
	if planCacheStmt, ok := prepStmt.(*plannercore.PlanCacheStmt); ok {
		rs.SetPreparedStmt(planCacheStmt)
	}

	// if the client wants to use cursor
	// we should hold the ResultSet in PreparedStatement for next stmt_fetch, and only send back ColumnInfo.
	// Tell the client cursor exists in server by setting proper serverStatus.
	if useCursor {
		crs := resultset.WrapWithCursor(rs)

		cc.initResultEncoder(ctx)
		defer cc.rsEncoder.Clean()
		// fetch all results of the resultSet, and stored them locally, so that the future `FETCH` command can read
		// the rows directly to avoid running executor and accessing shared params/variables in the session
		// NOTE: chunk should not be allocated from the connection allocator, which will reset after executing this command
		// but the rows are still needed in the following FETCH command.

		// create the row container to manage spill
		// this `rowContainer` will be released when the statement (or the connection) is closed.
		rowContainer := chunk.NewRowContainer(crs.FieldTypes(), vars.MaxChunkSize)
		rowContainer.GetMemTracker().AttachTo(vars.MemTracker)
		rowContainer.GetMemTracker().SetLabel(memory.LabelForCursorFetch)
		rowContainer.GetDiskTracker().AttachTo(vars.DiskTracker)
		rowContainer.GetDiskTracker().SetLabel(memory.LabelForCursorFetch)
		if variable.EnableTmpStorageOnOOM.Load() {
			failpoint.Inject("testCursorFetchSpill", func(val failpoint.Value) {
				if val, ok := val.(bool); val && ok {
					actionSpill := rowContainer.ActionSpillForTest()
					defer actionSpill.WaitForTest()
				}
			})
			action := memory.NewActionWithPriority(rowContainer.ActionSpill(), memory.DefCursorFetchSpillPriority)
			vars.MemTracker.FallbackOldAndSetNewAction(action)
		}
		defer func() {
			if err != nil {
				rowContainer.GetMemTracker().Detach()
				rowContainer.GetDiskTracker().Detach()
				errCloseRowContainer := rowContainer.Close()
				if errCloseRowContainer != nil {
					logutil.Logger(ctx).Error("Fail to close rowContainer in error handler. May cause resource leak",
						zap.NamedError("original-error", err), zap.NamedError("close-error", errCloseRowContainer))
				}
			}
		}()

		for {
			chk := crs.NewChunk(nil)

			if err = crs.Next(ctx, chk); err != nil {
				return false, err
			}
			rowCount := chk.NumRows()
			if rowCount == 0 {
				break
			}

			err = rowContainer.Add(chk)
			if err != nil {
				return false, err
			}
		}

		reader := chunk.NewRowContainerReader(rowContainer)
		crs.StoreRowContainerReader(reader)
		stmt.StoreResultSet(crs)
		stmt.StoreRowContainer(rowContainer)
		if cl, ok := crs.(resultset.FetchNotifier); ok {
			cl.OnFetchReturned()
		}
		stmt.SetCursorActive(true)
		defer func() {
			if err != nil {
				reader.Close()

				// the resultSet and rowContainer have been closed in former "defer" statement.
				stmt.StoreResultSet(nil)
				stmt.StoreRowContainer(nil)
				stmt.SetCursorActive(false)
			}
		}()

		if err = cc.writeColumnInfo(crs.Columns()); err != nil {
			return false, err
		}

		// explicitly flush columnInfo to client.
		err = cc.writeEOF(ctx, cc.ctx.Status())
		if err != nil {
			return false, err
		}

		return false, cc.flush(ctx)
	}
	retryable, err := cc.writeResultSet(ctx, rs, true, cc.ctx.Status(), 0)
	if err != nil {
		return retryable, errors.Annotate(err, cc.preparedStmt2String(uint32(stmt.ID())))
	}
	return false, nil
}

func (cc *clientConn) handleStmtFetch(ctx context.Context, data []byte) (err error) {
	cc.ctx.GetSessionVars().StartTime = time.Now()
	cc.ctx.GetSessionVars().ClearAlloc(nil, false)
	cc.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
	defer cc.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, false)
	// Reset the warn count. TODO: consider whether it's better to reset the whole session context/statement context.
	if cc.ctx.GetSessionVars().StmtCtx != nil {
		cc.ctx.GetSessionVars().StmtCtx.SetWarnings(nil)
	}
	cc.ctx.GetSessionVars().SysErrorCount = 0
	cc.ctx.GetSessionVars().SysWarningCount = 0

	stmtID, fetchSize, err := parse.StmtFetchCmd(data)
	if err != nil {
		return err
	}

	stmt := cc.ctx.GetStatement(int(stmtID))
	if stmt == nil {
		return errors.Annotate(mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_fetch"), cc.preparedStmt2String(stmtID))
	}
	if !stmt.GetCursorActive() {
		return errors.Annotate(mysql.NewErr(mysql.ErrSpCursorNotOpen), cc.preparedStmt2String(stmtID))
	}
	// from now on, we have made sure: the statement has an active cursor
	// then if facing any error, this cursor should be reset
	defer func() {
		if err != nil {
			errReset := stmt.Reset()
			if errReset != nil {
				logutil.Logger(ctx).Error("Fail to reset statement in error handler. May cause resource leak.",
					zap.NamedError("original-error", err), zap.NamedError("reset-error", errReset))
			}
		}
	}()

	if topsqlstate.TopSQLEnabled() {
		prepareObj, _ := cc.preparedStmtID2CachePreparedStmt(stmtID)
		if prepareObj != nil && prepareObj.SQLDigest != nil {
			ctx = topsql.AttachAndRegisterSQLInfo(ctx, prepareObj.NormalizedSQL, prepareObj.SQLDigest, false)
		}
	}
	sql := ""
	if prepared, ok := cc.ctx.GetStatement(int(stmtID)).(*TiDBStatement); ok {
		sql = prepared.sql
	}
	cc.ctx.SetProcessInfo(sql, time.Now(), mysql.ComStmtExecute, 0)
	rs := stmt.GetResultSet()

	_, err = cc.writeResultSet(ctx, rs, true, cc.ctx.Status(), int(fetchSize))
	// if the iterator reached the end before writing result, we could say the `FETCH` command will send EOF
	if rs.GetRowContainerReader().Current() == rs.GetRowContainerReader().End() {
		// also reset the statement when the cursor reaches the end
		// don't overwrite the `err` in outer scope, to avoid redundant `Reset()` in `defer` statement (though, it's not
		// a big problem, as the `Reset()` function call is idempotent.)
		err := stmt.Reset()
		if err != nil {
			logutil.Logger(ctx).Error("Fail to reset statement when FETCH command reaches the end. May cause resource leak",
				zap.NamedError("error", err))
		}
	}
	if err != nil {
		return errors.Annotate(err, cc.preparedStmt2String(stmtID))
	}

	return nil
}

func (cc *clientConn) handleStmtClose(data []byte) (err error) {
	if len(data) < 4 {
		return
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt != nil {
		return stmt.Close()
	}

	return
}

func (cc *clientConn) handleStmtSendLongData(data []byte) (err error) {
	if len(data) < 6 {
		return mysql.ErrMalformPacket
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))

	stmt := cc.ctx.GetStatement(stmtID)
	if stmt == nil {
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.Itoa(stmtID), "stmt_send_longdata")
	}

	paramID := int(binary.LittleEndian.Uint16(data[4:6]))
	return stmt.AppendParam(paramID, data[6:])
}

func (cc *clientConn) handleStmtReset(ctx context.Context, data []byte) (err error) {
	// A reset command should reset the statement to the state when it was right after prepare
	// Then the following state should be cleared:
	// 1.The opened cursor, including the rowContainer (and its cursor/memTracker).
	// 2.The argument sent through `SEND_LONG_DATA`.
	if len(data) < 4 {
		return mysql.ErrMalformPacket
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt == nil {
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.Itoa(stmtID), "stmt_reset")
	}
	err = stmt.Reset()
	if err != nil {
		// Both server and client cannot handle the error case well, so just left an error and return OK.
		// It's fine to receive further `EXECUTE` command even the `Reset` function call failed.
		logutil.Logger(ctx).Error("Fail to close statement in error handler of RESET command. May cause resource leak",
			zap.NamedError("original-error", err), zap.NamedError("close-error", err))

		return cc.writeOK(ctx)
	}

	return cc.writeOK(ctx)
}

// handleSetOption refer to https://dev.mysql.com/doc/internals/en/com-set-option.html
func (cc *clientConn) handleSetOption(ctx context.Context, data []byte) (err error) {
	if len(data) < 2 {
		return mysql.ErrMalformPacket
	}

	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		cc.capability |= mysql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	case 1:
		cc.capability &^= mysql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	default:
		return mysql.ErrMalformPacket
	}

	if err = cc.writeEOF(ctx, cc.ctx.Status()); err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) preparedStmt2String(stmtID uint32) string {
	sv := cc.ctx.GetSessionVars()
	if sv == nil {
		return ""
	}
	sql := parser.Normalize(cc.preparedStmt2StringNoArgs(stmtID), sv.EnableRedactLog)
	if m := sv.EnableRedactLog; m != errors.RedactLogEnable {
		sql += redact.String(sv.EnableRedactLog, sv.PlanCacheParams.String())
	}
	return sql
}

func (cc *clientConn) preparedStmt2StringNoArgs(stmtID uint32) string {
	sv := cc.ctx.GetSessionVars()
	if sv == nil {
		return ""
	}
	preparedObj, invalid := cc.preparedStmtID2CachePreparedStmt(stmtID)
	if invalid {
		return "invalidate PlanCacheStmt type, ID: " + strconv.FormatUint(uint64(stmtID), 10)
	}
	if preparedObj == nil {
		return "prepared statement not found, ID: " + strconv.FormatUint(uint64(stmtID), 10)
	}
	return preparedObj.PreparedAst.Stmt.Text()
}

func (cc *clientConn) preparedStmtID2CachePreparedStmt(stmtID uint32) (_ *plannercore.PlanCacheStmt, invalid bool) {
	sv := cc.ctx.GetSessionVars()
	if sv == nil {
		return nil, false
	}
	preparedPointer, ok := sv.PreparedStmts[stmtID]
	if !ok {
		// not found
		return nil, false
	}
	preparedObj, ok := preparedPointer.(*plannercore.PlanCacheStmt)
	if !ok {
		// invalid cache. should never happen.
		return nil, true
	}
	return preparedObj, false
}

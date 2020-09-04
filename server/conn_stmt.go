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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
)

func (cc *clientConn) handleStmtPrepare(sql string) error {
	stmt, columns, params, err := cc.ctx.Prepare(sql)
	if err != nil {
		return err
	}
	data := make([]byte, 4, 128)

	//status ok
	data = append(data, 0)
	//stmt id
	data = dumpUint32(data, uint32(stmt.ID()))
	//number columns
	data = dumpUint16(data, uint16(len(columns)))
	//number params
	data = dumpUint16(data, uint16(len(params)))
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0) //TODO support warning count

	if err := cc.writePacket(data); err != nil {
		return err
	}

	if len(params) > 0 {
		for i := 0; i < len(params); i++ {
			data = data[0:4]
			data = params[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(0); err != nil {
			return err
		}
	}

	if len(columns) > 0 {
		for i := 0; i < len(columns); i++ {
			data = data[0:4]
			data = columns[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(0); err != nil {
			return err
		}

	}
	return cc.flush()
}

func (cc *clientConn) handleStmtExecute(ctx context.Context, data []byte) (err error) {
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
	// Please refer to https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
	// The client indicates that it wants to use cursor by setting this flag.
	// 0x00 CURSOR_TYPE_NO_CURSOR
	// 0x01 CURSOR_TYPE_READ_ONLY
	// 0x02 CURSOR_TYPE_FOR_UPDATE
	// 0x04 CURSOR_TYPE_SCROLLABLE
	// Now we only support forward-only, read-only cursor.
	var useCursor bool
	switch flag {
	case 0:
		useCursor = false
	case 1:
		useCursor = true
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "unsupported flag %d", flag)
	}

	// skip iteration-count, always 1
	pos += 4

	var (
		nullBitmaps []byte
		paramTypes  []byte
		paramValues []byte
	)
	numParams := stmt.NumParams()
	args := make([]types.Datum, numParams)
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

		err = parseExecArgs(cc.ctx.GetSessionVars().StmtCtx, args, stmt.BoundParams(), nullBitmaps, stmt.GetParamsType(), paramValues)
		stmt.Reset()
		if err != nil {
			return errors.Annotate(err, cc.preparedStmt2String(stmtID))
		}
	}
	ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, &execdetails.StmtExecDetails{})
	rs, err := stmt.Execute(ctx, args)
	if err != nil {
		return errors.Annotate(err, cc.preparedStmt2String(stmtID))
	}
	if rs == nil {
		return cc.writeOK()
	}

	// if the client wants to use cursor
	// we should hold the ResultSet in PreparedStatement for next stmt_fetch, and only send back ColumnInfo.
	// Tell the client cursor exists in server by setting proper serverStatus.
	if useCursor {
		stmt.StoreResultSet(rs)
		err = cc.writeColumnInfo(rs.Columns(), mysql.ServerStatusCursorExists)
		if err != nil {
			return err
		}
		if cl, ok := rs.(fetchNotifier); ok {
			cl.OnFetchReturned()
		}
		// explicitly flush columnInfo to client.
		return cc.flush()
	}
	defer terror.Call(rs.Close)
	err = cc.writeResultset(ctx, rs, true, 0, 0)
	if err != nil {
		return errors.Annotate(err, cc.preparedStmt2String(stmtID))
	}
	return nil
}

// maxFetchSize constants
const (
	maxFetchSize = 1024
)

func (cc *clientConn) handleStmtFetch(ctx context.Context, data []byte) (err error) {
	cc.ctx.GetSessionVars().StartTime = time.Now()

	stmtID, fetchSize, err := parseStmtFetchCmd(data)
	if err != nil {
		return err
	}

	stmt := cc.ctx.GetStatement(int(stmtID))
	if stmt == nil {
		return errors.Annotate(mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_fetch"), cc.preparedStmt2String(stmtID))
	}
	sql := ""
	if prepared, ok := cc.ctx.GetStatement(int(stmtID)).(*TiDBStatement); ok {
		sql = prepared.sql
	}
	cc.ctx.SetProcessInfo(sql, time.Now(), mysql.ComStmtExecute, 0)
	rs := stmt.GetResultSet()
	if rs == nil {
		return errors.Annotate(mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_fetch_rs"), cc.preparedStmt2String(stmtID))
	}

	err = cc.writeResultset(ctx, rs, true, mysql.ServerStatusCursorExists, int(fetchSize))
	if err != nil {
		return errors.Annotate(err, cc.preparedStmt2String(stmtID))
	}
	return nil
}

func parseStmtFetchCmd(data []byte) (uint32, uint32, error) {
	if len(data) != 8 {
		return 0, 0, mysql.ErrMalformPacket
	}
	// Please refer to https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	fetchSize := binary.LittleEndian.Uint32(data[4:8])
	if fetchSize > maxFetchSize {
		fetchSize = maxFetchSize
	}
	return stmtID, fetchSize, nil
}

func parseExecArgs(sc *stmtctx.StatementContext, args []types.Datum, boundParams [][]byte, nullBitmap, paramTypes, paramValues []byte) (err error) {
	pos := 0
	var (
		tmp    interface{}
		v      []byte
		n      int
		isNull bool
	)

	for i := 0; i < len(args); i++ {
		// if params had received via ComStmtSendLongData, use them directly.
		// ref https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
		// see clientConn#handleStmtSendLongData
		if boundParams[i] != nil {
			args[i] = types.NewBytesDatum(boundParams[i])
			continue
		}

		// check nullBitMap to determine the NULL arguments.
		// ref https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
		// notice: some client(e.g. mariadb) will set nullBitMap even if data had be sent via ComStmtSendLongData,
		// so this check need place after boundParam's check.
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			var nilDatum types.Datum
			nilDatum.SetNull()
			args[i] = nilDatum
			continue
		}

		if (i<<1)+1 >= len(paramTypes) {
			return mysql.ErrMalformPacket
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.TypeNull:
			var nilDatum types.Datum
			nilDatum.SetNull()
			args[i] = nilDatum
			continue

		case mysql.TypeTiny:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(paramValues[pos]))
			} else {
				args[i] = types.NewIntDatum(int64(int8(paramValues[pos])))
			}

			pos++
			continue

		case mysql.TypeShort, mysql.TypeYear:
			if len(paramValues) < (pos + 2) {
				err = mysql.ErrMalformPacket
				return
			}
			valU16 := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU16))
			} else {
				args[i] = types.NewIntDatum(int64(int16(valU16)))
			}
			pos += 2
			continue

		case mysql.TypeInt24, mysql.TypeLong:
			if len(paramValues) < (pos + 4) {
				err = mysql.ErrMalformPacket
				return
			}
			valU32 := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU32))
			} else {
				args[i] = types.NewIntDatum(int64(int32(valU32)))
			}
			pos += 4
			continue

		case mysql.TypeLonglong:
			if len(paramValues) < (pos + 8) {
				err = mysql.ErrMalformPacket
				return
			}
			valU64 := binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			if isUnsigned {
				args[i] = types.NewUintDatum(valU64)
			} else {
				args[i] = types.NewIntDatum(int64(valU64))
			}
			pos += 8
			continue

		case mysql.TypeFloat:
			if len(paramValues) < (pos + 4) {
				err = mysql.ErrMalformPacket
				return
			}

			args[i] = types.NewFloat32Datum(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case mysql.TypeDouble:
			if len(paramValues) < (pos + 8) {
				err = mysql.ErrMalformPacket
				return
			}

			args[i] = types.NewFloat64Datum(math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8])))
			pos += 8
			continue

		case mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeDatetime:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}
			// See https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
			// for more details.
			length := paramValues[pos]
			pos++
			switch length {
			case 0:
				tmp = types.ZeroDatetimeStr
			case 4:
				pos, tmp = parseBinaryDate(pos, paramValues)
			case 7:
				pos, tmp = parseBinaryDateTime(pos, paramValues)
			case 11:
				pos, tmp = parseBinaryTimestamp(pos, paramValues)
			default:
				err = mysql.ErrMalformPacket
				return
			}
			args[i] = types.NewDatum(tmp) // FIXME: After check works!!!!!!
			continue

		case mysql.TypeDuration:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}
			// See https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
			// for more details.
			length := paramValues[pos]
			pos++
			switch length {
			case 0:
				tmp = "0"
			case 8:
				isNegative := paramValues[pos]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				pos++
				pos, tmp = parseBinaryDuration(pos, paramValues, isNegative)
			case 12:
				isNegative := paramValues[pos]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				pos++
				pos, tmp = parseBinaryDurationWithMS(pos, paramValues, isNegative)
			default:
				err = mysql.ErrMalformPacket
				return
			}
			args[i] = types.NewDatum(tmp)
			continue
		case mysql.TypeNewDecimal:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}

			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if isNull {
				args[i] = types.NewDecimalDatum(nil)
			} else {
				var dec types.MyDecimal
				err = sc.HandleTruncate(dec.FromString(v))
				if err != nil {
					return err
				}
				args[i] = types.NewDecimalDatum(&dec)
			}
			continue
		case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}
			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if isNull {
				args[i] = types.NewBytesDatum(nil)
			} else {
				args[i] = types.NewBytesDatum(v)
			}
			continue
		case mysql.TypeUnspecified, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeGeometry, mysql.TypeBit:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}

			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if !isNull {
				tmp = string(hack.String(v))
			} else {
				tmp = nil
			}
			args[i] = types.NewDatum(tmp)
			continue
		default:
			err = errUnknownFieldType.GenWithStack("stmt unknown field type %d", tp)
			return
		}
	}
	return
}

func parseBinaryDate(pos int, paramValues []byte) (int, string) {
	year := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
	pos += 2
	month := paramValues[pos]
	pos++
	day := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func parseBinaryDateTime(pos int, paramValues []byte) (int, string) {
	pos, date := parseBinaryDate(pos, paramValues)
	hour := paramValues[pos]
	pos++
	minute := paramValues[pos]
	pos++
	second := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func parseBinaryTimestamp(pos int, paramValues []byte) (int, string) {
	pos, dateTime := parseBinaryDateTime(pos, paramValues)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func parseBinaryDuration(pos int, paramValues []byte, isNegative uint8) (int, string) {
	sign := ""
	if isNegative == 1 {
		sign = "-"
	}
	days := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	hours := paramValues[pos]
	pos++
	minutes := paramValues[pos]
	pos++
	seconds := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s%d %02d:%02d:%02d", sign, days, hours, minutes, seconds)
}

func parseBinaryDurationWithMS(pos int, paramValues []byte,
	isNegative uint8) (int, string) {
	pos, dur := parseBinaryDuration(pos, paramValues, isNegative)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dur, microSecond)
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

func (cc *clientConn) handleStmtReset(data []byte) (err error) {
	if len(data) < 4 {
		return mysql.ErrMalformPacket
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt == nil {
		return mysql.NewErr(mysql.ErrUnknownStmtHandler,
			strconv.Itoa(stmtID), "stmt_reset")
	}
	stmt.Reset()
	stmt.StoreResultSet(nil)
	return cc.writeOK()
}

// handleSetOption refer to https://dev.mysql.com/doc/internals/en/com-set-option.html
func (cc *clientConn) handleSetOption(data []byte) (err error) {
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
	if err = cc.writeEOF(0); err != nil {
		return err
	}

	return cc.flush()
}

func (cc *clientConn) preparedStmt2String(stmtID uint32) string {
	sv := cc.ctx.GetSessionVars()
	if sv == nil {
		return ""
	}
	if config.GetGlobalConfig().EnableLogDesensitization {
		return cc.preparedStmt2StringNoArgs(stmtID)
	}
	return cc.preparedStmt2StringNoArgs(stmtID) + sv.PreparedParams.String()
}

func (cc *clientConn) preparedStmt2StringNoArgs(stmtID uint32) string {
	sv := cc.ctx.GetSessionVars()
	if sv == nil {
		return ""
	}
	preparedPointer, ok := sv.PreparedStmts[stmtID]
	if !ok {
		return "prepared statement not found, ID: " + strconv.FormatUint(uint64(stmtID), 10)
	}
	preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
	if !ok {
		return "invalidate CachedPrepareStmt type, ID: " + strconv.FormatUint(uint64(stmtID), 10)
	}
	preparedAst := preparedObj.PreparedAst
	return preparedAst.Stmt.Text()
}

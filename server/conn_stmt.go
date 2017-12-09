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
	"encoding/binary"
	"math"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/hack"
	goctx "golang.org/x/net/context"
)

func (cc *clientConn) handleStmtPrepare(sql string) error {
	stmt, columns, params, err := cc.ctx.Prepare(sql)
	if err != nil {
		return errors.Trace(err)
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
		return errors.Trace(err)
	}

	if len(params) > 0 {
		for i := 0; i < len(params); i++ {
			data = data[0:4]
			data = params[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return errors.Trace(err)
			}
		}

		if err := cc.writeEOF(false); err != nil {
			return errors.Trace(err)
		}
	}

	if len(columns) > 0 {
		for i := 0; i < len(columns); i++ {
			data = data[0:4]
			data = columns[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return errors.Trace(err)
			}
		}

		if err := cc.writeEOF(false); err != nil {
			return errors.Trace(err)
		}

	}
	return errors.Trace(cc.flush())
}

func (cc *clientConn) handleStmtExecute(goCtx goctx.Context, data []byte) (err error) {
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
	// Now we only support CURSOR_TYPE_NO_CURSOR flag.
	if flag != 0 {
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
	args := make([]interface{}, numParams)
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
			pos += (numParams << 1)
			paramValues = data[pos:]
			// Just the first StmtExecute packet contain parameters type,
			// we need save it for further use.
			stmt.SetParamsType(paramTypes)
		} else {
			paramValues = data[pos+1:]
		}

		err = parseStmtArgs(args, stmt.BoundParams(), nullBitmaps, stmt.GetParamsType(), paramValues)
		if err != nil {
			return errors.Trace(err)
		}
	}
	rs, err := stmt.Execute(goCtx, args...)
	if err != nil {
		return errors.Trace(err)
	}
	if rs == nil {
		return errors.Trace(cc.writeOK())
	}

	return errors.Trace(cc.writeResultset(goCtx, rs, true, false))
}

func parseStmtArgs(args []interface{}, boundParams [][]byte, nullBitmap, paramTypes, paramValues []byte) (err error) {
	pos := 0
	var v []byte
	var n int
	var isNull bool

	for i := 0; i < len(args); i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}
		if boundParams[i] != nil {
			args[i] = boundParams[i]
			continue
		}

		if (i<<1)+1 >= len(paramTypes) {
			return mysql.ErrMalformPacket
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.TypeNull:
			args[i] = nil
			continue

		case mysql.TypeTiny:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = uint64(paramValues[pos])
			} else {
				args[i] = int64(paramValues[pos])
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
				args[i] = uint64(valU16)
			} else {
				args[i] = int64(valU16)
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
				args[i] = uint64(valU32)
			} else {
				args[i] = int64(valU32)
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
				args[i] = valU64
			} else {
				args[i] = int64(valU64)
			}
			pos += 8
			continue

		case mysql.TypeFloat:
			if len(paramValues) < (pos + 4) {
				err = mysql.ErrMalformPacket
				return
			}

			args[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case mysql.TypeDouble:
			if len(paramValues) < (pos + 8) {
				err = mysql.ErrMalformPacket
				return
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.TypeUnspecified, mysql.TypeNewDecimal, mysql.TypeVarchar,
			mysql.TypeBit, mysql.TypeEnum, mysql.TypeSet, mysql.TypeTinyBlob,
			mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
			mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry,
			mysql.TypeDate, mysql.TypeNewDate,
			mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
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
				args[i] = hack.String(v)
			} else {
				args[i] = nil
			}
			continue
		default:
			err = errUnknownFieldType.Gen("stmt unknown field type %d", tp)
			return
		}
	}
	return
}

func (cc *clientConn) handleStmtClose(data []byte) (err error) {
	if len(data) < 4 {
		return
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt != nil {
		return errors.Trace(stmt.Close())
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
	return cc.writeOK()
}

// See https://dev.mysql.com/doc/internals/en/com-set-option.html
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
	if err = cc.writeEOF(false); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

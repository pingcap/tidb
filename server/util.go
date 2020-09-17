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
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
)

func parseNullTermString(b []byte) (str []byte, remain []byte) {
	off := bytes.IndexByte(b, 0)
	if off == -1 {
		return nil, b
	}
	return b[:off], b[off+1:]
}

func parseLengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {
	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer: If the first byte of a packet is a length-encoded integer and its byte value is 0xfe, you must check the length of the packet to verify that it has enough space for a 8-byte integer.
	// TODO: 0xff is undefined

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

func dumpLengthEncodedInt(buffer []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(buffer, byte(n))

	case n <= 0xffff:
		return append(buffer, 0xfc, byte(n), byte(n>>8))

	case n <= 0xffffff:
		return append(buffer, 0xfd, byte(n), byte(n>>8), byte(n>>16))

	case n <= 0xffffffffffffffff:
		return append(buffer, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
			byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
	}

	return buffer
}

func parseLengthEncodedBytes(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := parseLengthEncodedInt(b)
	if num < 1 {
		return nil, isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n], false, n, nil
	}

	return nil, false, n, io.EOF
}

func dumpLengthEncodedString(buffer []byte, bytes []byte) []byte {
	buffer = dumpLengthEncodedInt(buffer, uint64(len(bytes)))
	buffer = append(buffer, bytes...)
	return buffer
}

func dumpUint16(buffer []byte, n uint16) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	return buffer
}

func dumpUint32(buffer []byte, n uint32) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	return buffer
}

func dumpUint64(buffer []byte, n uint64) []byte {
	buffer = append(buffer, byte(n))
	buffer = append(buffer, byte(n>>8))
	buffer = append(buffer, byte(n>>16))
	buffer = append(buffer, byte(n>>24))
	buffer = append(buffer, byte(n>>32))
	buffer = append(buffer, byte(n>>40))
	buffer = append(buffer, byte(n>>48))
	buffer = append(buffer, byte(n>>56))
	return buffer
}

func dumpBinaryTime(dur time.Duration) (data []byte) {
	if dur == 0 {
		return []byte{0}
	}
	data = make([]byte, 13)
	data[0] = 12
	if dur < 0 {
		data[1] = 1
		dur = -dur
	}
	days := dur / (24 * time.Hour)
	dur -= days * 24 * time.Hour
	data[2] = byte(days)
	hours := dur / time.Hour
	dur -= hours * time.Hour
	data[6] = byte(hours)
	minutes := dur / time.Minute
	dur -= minutes * time.Minute
	data[7] = byte(minutes)
	seconds := dur / time.Second
	dur -= seconds * time.Second
	data[8] = byte(seconds)
	if dur == 0 {
		data[0] = 8
		return data[:9]
	}
	binary.LittleEndian.PutUint32(data[9:13], uint32(dur/time.Microsecond))
	return
}

func dumpBinaryDateTime(data []byte, t types.Time) []byte {
	year, mon, day := t.Year(), t.Month(), t.Day()
	switch t.Type() {
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		if t.IsZero() {
			data = append(data, 0)
		} else {
			data = append(data, 11)
			data = dumpUint16(data, uint16(year))
			data = append(data, byte(mon), byte(day), byte(t.Hour()), byte(t.Minute()), byte(t.Second()))
			data = dumpUint32(data, uint32(t.Microsecond()))
		}
	case mysql.TypeDate:
		if t.IsZero() {
			data = append(data, 0)
		} else {
			data = append(data, 4)
			data = dumpUint16(data, uint16(year)) //year
			data = append(data, byte(mon), byte(day))
		}
	}
	return data
}

func dumpBinaryRow(buffer []byte, columns []*ColumnInfo, row chunk.Row) ([]byte, error) {
	buffer = append(buffer, mysql.OKHeader)
	nullBitmapOff := len(buffer)
	numBytes4Null := (len(columns) + 7 + 2) / 8
	for i := 0; i < numBytes4Null; i++ {
		buffer = append(buffer, 0)
	}
	for i := range columns {
		if row.IsNull(i) {
			bytePos := (i + 2) / 8
			bitPos := byte((i + 2) % 8)
			buffer[nullBitmapOff+bytePos] |= 1 << bitPos
			continue
		}
		switch columns[i].Type {
		case mysql.TypeTiny:
			buffer = append(buffer, byte(row.GetInt64(i)))
		case mysql.TypeShort, mysql.TypeYear:
			buffer = dumpUint16(buffer, uint16(row.GetInt64(i)))
		case mysql.TypeInt24, mysql.TypeLong:
			buffer = dumpUint32(buffer, uint32(row.GetInt64(i)))
		case mysql.TypeLonglong:
			buffer = dumpUint64(buffer, row.GetUint64(i))
		case mysql.TypeFloat:
			buffer = dumpUint32(buffer, math.Float32bits(row.GetFloat32(i)))
		case mysql.TypeDouble:
			buffer = dumpUint64(buffer, math.Float64bits(row.GetFloat64(i)))
		case mysql.TypeNewDecimal:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetMyDecimal(i).String()))
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			buffer = dumpLengthEncodedString(buffer, row.GetBytes(i))
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			buffer = dumpBinaryDateTime(buffer, row.GetTime(i))
		case mysql.TypeDuration:
			buffer = append(buffer, dumpBinaryTime(row.GetDuration(i, 0).Duration)...)
		case mysql.TypeEnum:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetEnum(i).String()))
		case mysql.TypeSet:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetSet(i).String()))
		case mysql.TypeJSON:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetJSON(i).String()))
		default:
			return nil, errInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	return buffer, nil
}

func dumpTextRow(buffer []byte, columns []*ColumnInfo, row chunk.Row) ([]byte, error) {
	tmp := make([]byte, 0, 20)
	for i, col := range columns {
		if row.IsNull(i) {
			buffer = append(buffer, 0xfb)
			continue
		}
		switch col.Type {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
			tmp = strconv.AppendInt(tmp[:0], row.GetInt64(i), 10)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeYear:
			year := row.GetInt64(i)
			tmp = tmp[:0]
			if year == 0 {
				tmp = append(tmp, '0', '0', '0', '0')
			} else {
				tmp = strconv.AppendInt(tmp, year, 10)
			}
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeLonglong:
			if mysql.HasUnsignedFlag(uint(columns[i].Flag)) {
				tmp = strconv.AppendUint(tmp[:0], row.GetUint64(i), 10)
			} else {
				tmp = strconv.AppendInt(tmp[:0], row.GetInt64(i), 10)
			}
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeFloat:
			prec := -1
			if columns[i].Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec {
				prec = int(col.Decimal)
			}
			tmp = appendFormatFloat(tmp[:0], float64(row.GetFloat32(i)), prec, 32)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeDouble:
			prec := types.UnspecifiedLength
			if col.Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec {
				prec = int(col.Decimal)
			}
			tmp = appendFormatFloat(tmp[:0], row.GetFloat64(i), prec, 64)
			buffer = dumpLengthEncodedString(buffer, tmp)
		case mysql.TypeNewDecimal:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetMyDecimal(i).String()))
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			buffer = dumpLengthEncodedString(buffer, row.GetBytes(i))
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetTime(i).String()))
		case mysql.TypeDuration:
			dur := row.GetDuration(i, int(col.Decimal))
			buffer = dumpLengthEncodedString(buffer, hack.Slice(dur.String()))
		case mysql.TypeEnum:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetEnum(i).String()))
		case mysql.TypeSet:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetSet(i).String()))
		case mysql.TypeJSON:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(row.GetJSON(i).String()))
		default:
			return nil, errInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	return buffer, nil
}

func lengthEncodedIntSize(n uint64) int {
	switch {
	case n <= 250:
		return 1

	case n <= 0xffff:
		return 3

	case n <= 0xffffff:
		return 4
	}

	return 9
}

const (
	expFormatBig   = 1e15
	expFormatSmall = 1e-15
)

func appendFormatFloat(in []byte, fVal float64, prec, bitSize int) []byte {
	absVal := math.Abs(fVal)
	var out []byte
	if prec == types.UnspecifiedLength && (absVal >= expFormatBig || (absVal != 0 && absVal < expFormatSmall)) {
		out = strconv.AppendFloat(in, fVal, 'e', prec, bitSize)
		valStr := out[len(in):]
		// remove the '+' from the string for compatibility.
		plusPos := bytes.IndexByte(valStr, '+')
		if plusPos > 0 {
			plusPosInOut := len(in) + plusPos
			out = append(out[:plusPosInOut], out[plusPosInOut+1:]...)
		}
	} else {
		out = strconv.AppendFloat(in, fVal, 'f', prec, bitSize)
	}
	return out
}

// CorsHandler adds Cors Header if `cors` config is set.
type CorsHandler struct {
	handler http.Handler
	cfg     *config.Config
}

func (h CorsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if h.cfg.Cors != "" {
		w.Header().Set("Access-Control-Allow-Origin", h.cfg.Cors)
		w.Header().Set("Access-Control-Allow-Methods", "GET")
	}
	h.handler.ServeHTTP(w, req)
}

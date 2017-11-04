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
	"io"
	"math"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
)

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

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

func dumpLengthEncodedInt(buffer []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(buffer, tinyIntCache[n]...)

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

var tinyIntCache [251][]byte

func init() {
	for i := 0; i < len(tinyIntCache); i++ {
		tinyIntCache[i] = []byte{byte(i)}
	}
}

func dumpBinaryTime(dur time.Duration) (data []byte) {
	if dur == 0 {
		data = tinyIntCache[0]
		return
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

func dumpBinaryDateTime(t types.Time, loc *time.Location) (data []byte, err error) {
	if t.Type == mysql.TypeTimestamp && loc != nil {
		// TODO: Consider time_zone variable.
		t1, err := t.Time.GoTime(time.Local)
		if err != nil {
			return nil, errors.Errorf("FATAL: convert timestamp %v go time return error!", t.Time)
		}
		t.Time = types.FromGoTime(t1.In(loc))
	}

	year, mon, day := t.Time.Year(), t.Time.Month(), t.Time.Day()
	if t.IsZero() {
		year, mon, day = 1, int(time.January), 1
	}
	switch t.Type {
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		data = append(data, 11)
		data = dumpUint16(data, uint16(year))
		data = append(data, byte(mon), byte(day), byte(t.Time.Hour()), byte(t.Time.Minute()), byte(t.Time.Second()))
		data = dumpUint32(data, uint32(t.Time.Microsecond()))
	case mysql.TypeDate, mysql.TypeNewDate:
		data = append(data, 4)
		data = dumpUint16(data, uint16(year)) //year
		data = append(data, byte(mon), byte(day))
	}
	return
}

func dumpRowValuesBinary(buffer []byte, columns []*ColumnInfo, row []types.Datum) ([]byte, error) {
	if len(columns) != len(row) {
		return nil, mysql.ErrMalformPacket
	}
	buffer[0] = mysql.OKHeader
	nulls := buffer[1:]
	for i, val := range row {
		if val.IsNull() {
			bytePos := (i + 2) / 8
			bitPos := byte((i + 2) % 8)
			nulls[bytePos] |= 1 << bitPos
		}
	}
	for i, val := range row {
		switch val.Kind() {
		case types.KindInt64:
			v := val.GetInt64()
			switch columns[i].Type {
			case mysql.TypeTiny:
				buffer = append(buffer, byte(v))
			case mysql.TypeShort, mysql.TypeYear:
				buffer = dumpUint16(buffer, uint16(v))
			case mysql.TypeInt24, mysql.TypeLong:
				buffer = dumpUint32(buffer, uint32(v))
			case mysql.TypeLonglong:
				buffer = dumpUint64(buffer, uint64(v))
			}
		case types.KindUint64:
			v := val.GetUint64()
			switch columns[i].Type {
			case mysql.TypeTiny:
				buffer = append(buffer, byte(v))
			case mysql.TypeShort, mysql.TypeYear:
				buffer = dumpUint16(buffer, uint16(v))
			case mysql.TypeInt24, mysql.TypeLong:
				buffer = dumpUint32(buffer, uint32(v))
			case mysql.TypeLonglong:
				buffer = dumpUint64(buffer, v)
			}
		case types.KindFloat32:
			floatBits := math.Float32bits(val.GetFloat32())
			buffer = dumpUint32(buffer, floatBits)
		case types.KindFloat64:
			floatBits := math.Float64bits(val.GetFloat64())
			buffer = dumpUint64(buffer, floatBits)
		case types.KindString, types.KindBytes:
			buffer = dumpLengthEncodedString(buffer, val.GetBytes())
		case types.KindMysqlDecimal:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(val.GetMysqlDecimal().String()))
		case types.KindMysqlTime:
			tmp, err := dumpBinaryDateTime(val.GetMysqlTime(), nil)
			if err != nil {
				return buffer, errors.Trace(err)
			}
			buffer = append(buffer, tmp...)
		case types.KindMysqlDuration:
			buffer = append(buffer, dumpBinaryTime(val.GetMysqlDuration().Duration)...)
		case types.KindMysqlSet:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(val.GetMysqlSet().String()))
		case types.KindMysqlEnum:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(val.GetMysqlEnum().String()))
		case types.KindBinaryLiteral, types.KindMysqlBit:
			buffer = dumpLengthEncodedString(buffer, hack.Slice(val.GetBinaryLiteral().ToString()))
		}
	}
	return buffer, nil
}

func dumpTextValue(colInfo *ColumnInfo, value types.Datum) ([]byte, error) {
	switch value.Kind() {
	case types.KindInt64:
		return strconv.AppendInt(nil, value.GetInt64(), 10), nil
	case types.KindUint64:
		return strconv.AppendUint(nil, value.GetUint64(), 10), nil
	case types.KindFloat32:
		prec := -1
		if colInfo.Decimal > 0 && int(colInfo.Decimal) != mysql.NotFixedDec {
			prec = int(colInfo.Decimal)
		}
		return strconv.AppendFloat(nil, value.GetFloat64(), 'f', prec, 32), nil
	case types.KindFloat64:
		prec := -1
		if colInfo.Decimal > 0 && int(colInfo.Decimal) != mysql.NotFixedDec {
			prec = int(colInfo.Decimal)
		}
		return strconv.AppendFloat(nil, value.GetFloat64(), 'f', prec, 64), nil
	case types.KindString, types.KindBytes:
		return value.GetBytes(), nil
	case types.KindMysqlTime:
		return hack.Slice(value.GetMysqlTime().String()), nil
	case types.KindMysqlDuration:
		return hack.Slice(value.GetMysqlDuration().String()), nil
	case types.KindMysqlDecimal:
		return hack.Slice(value.GetMysqlDecimal().String()), nil
	case types.KindMysqlEnum:
		return hack.Slice(value.GetMysqlEnum().String()), nil
	case types.KindMysqlSet:
		return hack.Slice(value.GetMysqlSet().String()), nil
	case types.KindMysqlJSON:
		return hack.Slice(value.GetMysqlJSON().String()), nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		return hack.Slice(value.GetBinaryLiteral().ToString()), nil
	default:
		return nil, errInvalidType.Gen("invalid type %v", value.Kind())
	}
}

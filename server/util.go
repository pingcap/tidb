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
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/types"
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

func dumpLengthEncodedInt(n uint64) []byte {
	switch {
	case n <= 250:
		return tinyIntCache[n]

	case n <= 0xffff:
		return []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

	case n <= 0xffffffffffffffff:
		return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
	}

	return nil
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

func dumpLengthEncodedString(b []byte, alloc arena.Allocator) []byte {
	data := alloc.Alloc(len(b) + 9)
	data = append(data, dumpLengthEncodedInt(uint64(len(b)))...)
	data = append(data, b...)
	return data
}

func dumpUint16(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func dumpUint32(n uint32) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func dumpUint64(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
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
		data = append(data, dumpUint16(uint16(year))...)
		data = append(data, byte(mon), byte(day), byte(t.Time.Hour()), byte(t.Time.Minute()), byte(t.Time.Second()))
		data = append(data, dumpUint32(uint32(t.Time.Microsecond()))...)
	case mysql.TypeDate, mysql.TypeNewDate:
		data = append(data, 4)
		data = append(data, dumpUint16(uint16(year))...) //year
		data = append(data, byte(mon), byte(day))
	}
	return
}

func uniformValue(value interface{}) interface{} {
	switch v := value.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)
	default:
		return value
	}
}

func dumpRowValuesBinary(alloc arena.Allocator, columns []*ColumnInfo, row []types.Datum) (data []byte, err error) {
	if len(columns) != len(row) {
		err = mysql.ErrMalformPacket
		return
	}
	data = append(data, mysql.OKHeader)
	nullsLen := ((len(columns) + 7 + 2) / 8)
	nulls := make([]byte, nullsLen)
	for i, val := range row {
		if val.IsNull() {
			bytePos := (i + 2) / 8
			bitPos := byte((i + 2) % 8)
			nulls[bytePos] |= 1 << bitPos
		}
	}
	data = append(data, nulls...)
	for i, val := range row {
		switch val.Kind() {
		case types.KindInt64:
			v := val.GetInt64()
			switch columns[i].Type {
			case mysql.TypeTiny:
				data = append(data, byte(v))
			case mysql.TypeShort, mysql.TypeYear:
				data = append(data, dumpUint16(uint16(v))...)
			case mysql.TypeInt24, mysql.TypeLong:
				data = append(data, dumpUint32(uint32(v))...)
			case mysql.TypeLonglong:
				data = append(data, dumpUint64(uint64(v))...)
			}
		case types.KindUint64:
			v := val.GetUint64()
			switch columns[i].Type {
			case mysql.TypeTiny:
				data = append(data, byte(v))
			case mysql.TypeShort, mysql.TypeYear:
				data = append(data, dumpUint16(uint16(v))...)
			case mysql.TypeInt24, mysql.TypeLong:
				data = append(data, dumpUint32(uint32(v))...)
			case mysql.TypeLonglong:
				data = append(data, dumpUint64(uint64(v))...)
			}
		case types.KindFloat32:
			floatBits := math.Float32bits(val.GetFloat32())
			data = append(data, dumpUint32(floatBits)...)
		case types.KindFloat64:
			floatBits := math.Float64bits(val.GetFloat64())
			data = append(data, dumpUint64(floatBits)...)
		case types.KindString, types.KindBytes:
			data = append(data, dumpLengthEncodedString(val.GetBytes(), alloc)...)
		case types.KindMysqlDecimal:
			data = append(data, dumpLengthEncodedString(hack.Slice(val.GetMysqlDecimal().String()), alloc)...)
		case types.KindMysqlTime:
			tmp, err := dumpBinaryDateTime(val.GetMysqlTime(), nil)
			if err != nil {
				return data, errors.Trace(err)
			}
			data = append(data, tmp...)
		case types.KindMysqlDuration:
			data = append(data, dumpBinaryTime(val.GetMysqlDuration().Duration)...)
		case types.KindMysqlSet:
			data = append(data, dumpLengthEncodedString(hack.Slice(val.GetMysqlSet().String()), alloc)...)
		case types.KindMysqlHex:
			data = append(data, dumpLengthEncodedString(hack.Slice(val.GetMysqlHex().ToString()), alloc)...)
		case types.KindMysqlEnum:
			data = append(data, dumpLengthEncodedString(hack.Slice(val.GetMysqlEnum().String()), alloc)...)
		case types.KindMysqlBit:
			data = append(data, dumpLengthEncodedString(hack.Slice(val.GetMysqlBit().ToString()), alloc)...)
		}
	}
	return
}

func dumpTextValue(mysqlType uint8, value types.Datum) ([]byte, error) {
	switch value.Kind() {
	case types.KindInt64:
		return strconv.AppendInt(nil, value.GetInt64(), 10), nil
	case types.KindUint64:
		return strconv.AppendUint(nil, value.GetUint64(), 10), nil
	case types.KindFloat32:
		return strconv.AppendFloat(nil, value.GetFloat64(), 'f', -1, 32), nil
	case types.KindFloat64:
		return strconv.AppendFloat(nil, value.GetFloat64(), 'f', -1, 64), nil
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
	case types.KindMysqlBit:
		return hack.Slice(value.GetMysqlBit().ToString()), nil
	case types.KindMysqlHex:
		return hack.Slice(value.GetMysqlHex().ToString()), nil
	default:
		return nil, errInvalidType.Gen("invalid type %T", value)
	}
}

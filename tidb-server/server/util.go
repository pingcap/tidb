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

func dumpBinaryDateTime(t mysql.Time, loc *time.Location) (data []byte) {
	if t.Type == mysql.TypeTimestamp && loc != nil {
		t.Time = t.In(loc)
	}

	year, mon, day := t.Year(), t.Month(), t.Day()
	if t.IsZero() {
		year, mon, day = 1, time.January, 1
	}
	switch t.Type {
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		data = append(data, 11)
		data = append(data, dumpUint16(uint16(year))...)
		data = append(data, byte(mon), byte(day), byte(t.Hour()), byte(t.Minute()), byte(t.Second()))
		data = append(data, dumpUint32(uint32((t.Nanosecond() / 1000)))...)
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

func dumpRowValuesBinary(alloc arena.Allocator, columns []*ColumnInfo, row []interface{}) (data []byte, err error) {
	if len(columns) != len(row) {
		err = mysql.ErrMalformPacket
		return
	}
	data = append(data, mysql.OKHeader)
	nullsLen := ((len(columns) + 7 + 2) / 8)
	nulls := make([]byte, nullsLen)
	for i, val := range row {
		if val == nil {
			bytePos := (i + 2) / 8
			bitPos := byte((i + 2) % 8)
			nulls[bytePos] |= 1 << bitPos
		}
	}
	data = append(data, nulls...)
	for i, val := range row {
		val = uniformValue(val)
		switch v := val.(type) {
		case int64:
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
		case uint64:
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
		case float32:
			floatBits := math.Float32bits(float32(val.(float64)))
			data = append(data, dumpUint32(floatBits)...)
		case float64:
			floatBits := math.Float64bits(val.(float64))
			data = append(data, dumpUint64(floatBits)...)
		case string:
			data = append(data, dumpLengthEncodedString(hack.Slice(v), alloc)...)
		case []byte:
			data = append(data, dumpLengthEncodedString(v, alloc)...)
		case mysql.Time:
			data = append(data, dumpBinaryDateTime(v, nil)...)
		case time.Time:
			myTime := mysql.Time{Time: v, Type: columns[i].Type, Fsp: mysql.DefaultFsp}
			data = append(data, dumpBinaryDateTime(myTime, nil)...)
		case mysql.Duration:
			data = append(data, dumpBinaryTime(v.Duration)...)
		case time.Duration:
			data = append(data, dumpBinaryTime(v)...)
		case mysql.Decimal:
			data = append(data, dumpLengthEncodedString(hack.Slice(v.String()), alloc)...)
		case mysql.Enum:
			data = append(data, dumpLengthEncodedString(hack.Slice(v.String()), alloc)...)
		case mysql.Set:
			data = append(data, dumpLengthEncodedString(hack.Slice(v.String()), alloc)...)
		case mysql.Bit:
			data = append(data, dumpLengthEncodedString(hack.Slice(v.ToString()), alloc)...)
		}
	}
	return
}

func dumpTextValue(mysqlType uint8, value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case uint8:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case float32:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case []byte:
		return v, nil
	case string:
		return hack.Slice(v), nil
	case mysql.Time:
		return hack.Slice(v.String()), nil
	case mysql.Duration:
		return hack.Slice(v.String()), nil
	case mysql.Decimal:
		return hack.Slice(v.String()), nil
	case mysql.Enum:
		return hack.Slice(v.String()), nil
	case mysql.Set:
		return hack.Slice(v.String()), nil
	case mysql.Bit:
		return hack.Slice(v.ToString()), nil
	default:
		return nil, errors.Errorf("invalid type %T", value)
	}
}

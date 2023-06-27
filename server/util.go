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
	"math"
	"strconv"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server/internal/column"
	"github.com/pingcap/tidb/server/internal/dump"
	"github.com/pingcap/tidb/server/internal/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
)

func dumpBinaryRow(buffer []byte, columns []*column.Info, row chunk.Row, d *column.ResultEncoder) ([]byte, error) {
	if d == nil {
		d = column.NewResultEncoder(charset.CharsetUTF8MB4)
	}
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
			buffer = dump.Uint16(buffer, uint16(row.GetInt64(i)))
		case mysql.TypeInt24, mysql.TypeLong:
			buffer = dump.Uint32(buffer, uint32(row.GetInt64(i)))
		case mysql.TypeLonglong:
			buffer = dump.Uint64(buffer, row.GetUint64(i))
		case mysql.TypeFloat:
			buffer = dump.Uint32(buffer, math.Float32bits(row.GetFloat32(i)))
		case mysql.TypeDouble:
			buffer = dump.Uint64(buffer, math.Float64bits(row.GetFloat64(i)))
		case mysql.TypeNewDecimal:
			buffer = dump.LengthEncodedString(buffer, hack.Slice(row.GetMyDecimal(i).String()))
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			d.UpdateDataEncoding(columns[i].Charset)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(row.GetBytes(i)))
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			buffer = dump.BinaryDateTime(buffer, row.GetTime(i))
		case mysql.TypeDuration:
			buffer = append(buffer, dump.BinaryTime(row.GetDuration(i, 0).Duration)...)
		case mysql.TypeEnum:
			d.UpdateDataEncoding(columns[i].Charset)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(hack.Slice(row.GetEnum(i).String())))
		case mysql.TypeSet:
			d.UpdateDataEncoding(columns[i].Charset)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(hack.Slice(row.GetSet(i).String())))
		case mysql.TypeJSON:
			// The collation of JSON type is always binary.
			// To compatible with MySQL, here we treat it as utf-8.
			d.UpdateDataEncoding(mysql.DefaultCollationID)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(hack.Slice(row.GetJSON(i).String())))
		default:
			return nil, errInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	return buffer, nil
}

func dumpTextRow(buffer []byte, columns []*column.Info, row chunk.Row, d *column.ResultEncoder) ([]byte, error) {
	if d == nil {
		d = column.NewResultEncoder(charset.CharsetUTF8MB4)
	}
	tmp := make([]byte, 0, 20)
	for i, col := range columns {
		if row.IsNull(i) {
			buffer = append(buffer, 0xfb)
			continue
		}
		switch col.Type {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
			tmp = strconv.AppendInt(tmp[:0], row.GetInt64(i), 10)
			buffer = dump.LengthEncodedString(buffer, tmp)
		case mysql.TypeYear:
			year := row.GetInt64(i)
			tmp = tmp[:0]
			if year == 0 {
				tmp = append(tmp, '0', '0', '0', '0')
			} else {
				tmp = strconv.AppendInt(tmp, year, 10)
			}
			buffer = dump.LengthEncodedString(buffer, tmp)
		case mysql.TypeLonglong:
			if mysql.HasUnsignedFlag(uint(columns[i].Flag)) {
				tmp = strconv.AppendUint(tmp[:0], row.GetUint64(i), 10)
			} else {
				tmp = strconv.AppendInt(tmp[:0], row.GetInt64(i), 10)
			}
			buffer = dump.LengthEncodedString(buffer, tmp)
		case mysql.TypeFloat:
			prec := -1
			if columns[i].Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec && col.Table == "" {
				prec = int(col.Decimal)
			}
			tmp = util.AppendFormatFloat(tmp[:0], float64(row.GetFloat32(i)), prec, 32)
			buffer = dump.LengthEncodedString(buffer, tmp)
		case mysql.TypeDouble:
			prec := types.UnspecifiedLength
			if col.Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec && col.Table == "" {
				prec = int(col.Decimal)
			}
			tmp = util.AppendFormatFloat(tmp[:0], row.GetFloat64(i), prec, 64)
			buffer = dump.LengthEncodedString(buffer, tmp)
		case mysql.TypeNewDecimal:
			buffer = dump.LengthEncodedString(buffer, hack.Slice(row.GetMyDecimal(i).String()))
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			d.UpdateDataEncoding(col.Charset)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(row.GetBytes(i)))
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			buffer = dump.LengthEncodedString(buffer, hack.Slice(row.GetTime(i).String()))
		case mysql.TypeDuration:
			dur := row.GetDuration(i, int(col.Decimal))
			buffer = dump.LengthEncodedString(buffer, hack.Slice(dur.String()))
		case mysql.TypeEnum:
			d.UpdateDataEncoding(col.Charset)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(hack.Slice(row.GetEnum(i).String())))
		case mysql.TypeSet:
			d.UpdateDataEncoding(col.Charset)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(hack.Slice(row.GetSet(i).String())))
		case mysql.TypeJSON:
			// The collation of JSON type is always binary.
			// To compatible with MySQL, here we treat it as utf-8.
			d.UpdateDataEncoding(mysql.DefaultCollationID)
			buffer = dump.LengthEncodedString(buffer, d.EncodeData(hack.Slice(row.GetJSON(i).String())))
		default:
			return nil, errInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	return buffer, nil
}

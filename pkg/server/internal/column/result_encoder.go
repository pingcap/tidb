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

package column

import (
	"bytes"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// ResultEncoder encodes a column value to a byte slice.
type ResultEncoder struct {
	encoding charset.Encoding

	// dataEncoding can be updated to match the column data charset.
	dataEncoding charset.Encoding

	buffer *bytes.Buffer

	// chsName and encoding are unchanged after the initialization from
	// session variable @@character_set_results.
	chsName string

	isBinary     bool
	isNull       bool
	dataIsBinary bool
}

// NewResultEncoder creates a new ResultEncoder.
func NewResultEncoder(chs string) *ResultEncoder {
	return &ResultEncoder{
		chsName:  chs,
		encoding: charset.FindEncodingTakeUTF8AsNoop(chs),
		buffer:   &bytes.Buffer{},
		isBinary: chs == charset.CharsetBin,
		isNull:   len(chs) == 0,
	}
}

// Clean prevent the ResultEncoder from holding too much memory.
func (d *ResultEncoder) Clean() {
	d.buffer = nil
}

// UpdateDataEncoding updates the data encoding.
func (d *ResultEncoder) UpdateDataEncoding(chsID uint16) {
	chs, _, err := charset.GetCharsetInfoByID(int(chsID))
	if err != nil {
		logutil.BgLogger().Warn("unknown charset ID", zap.Error(err))
	}
	d.dataEncoding = charset.FindEncodingTakeUTF8AsNoop(chs)
	d.dataIsBinary = chsID == mysql.BinaryDefaultCollationID
}

// ColumnTypeInfoCharsetID returns the charset ID for the column type info.
func (d *ResultEncoder) ColumnTypeInfoCharsetID(info *Info) uint16 {
	// Only replace the charset when @@character_set_results is valid and
	// the target column is a non-binary string.
	if d.isNull || len(d.chsName) == 0 || !isStringColumnType(info.Type) {
		return info.Charset
	}
	if info.Charset == mysql.BinaryDefaultCollationID {
		return mysql.BinaryDefaultCollationID
	}
	return uint16(mysql.CharsetNameToID(d.chsName))
}

// EncodeMeta encodes bytes for meta info like column names.
// Note that the result should be consumed immediately.
func (d *ResultEncoder) EncodeMeta(src []byte) []byte {
	return d.EncodeWith(src, d.encoding)
}

// EncodeData encodes bytes for row data.
// Note that the result should be consumed immediately.
func (d *ResultEncoder) EncodeData(src []byte) []byte {
	// For the following cases, TiDB encodes the results with column charset
	// instead of @@character_set_results:
	//   - @@character_set_result = null.
	//   - @@character_set_result = binary.
	//   - The column is binary type like blob, binary char/varchar.
	if d.isNull || d.isBinary || d.dataIsBinary {
		// Use the column charset to encode.
		return d.EncodeWith(src, d.dataEncoding)
	}
	return d.EncodeWith(src, d.encoding)
}

// EncodeWith encodes bytes with the given encoding.
func (d *ResultEncoder) EncodeWith(src []byte, enc charset.Encoding) []byte {
	data, err := enc.Transform(d.buffer, src, charset.OpEncode)
	if err != nil {
		logutil.BgLogger().Debug("encode error", zap.Error(err))
	}
	return data
}

func isStringColumnType(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
		mysql.TypeEnum, mysql.TypeSet, mysql.TypeJSON:
		return true
	}
	return false
}

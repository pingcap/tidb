// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var errUnknownFieldType = dbterror.ClassServer.NewStd(errno.ErrUnknownFieldType)

// parseBinaryParams decodes the binary params according to the protocol
func parseBinaryParams(params []param.BinaryParam, boundParams [][]byte, nullBitmap, paramTypes, paramValues []byte, enc *util2.InputDecoder) (pos int, err error) {
	if enc == nil {
		enc = util2.NewInputDecoder(charset.CharsetUTF8)
	}

	for i := 0; i < len(params); i++ {
		// if params had received via ComStmtSendLongData, use them directly.
		// ref https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
		// see clientConn#handleStmtSendLongData
		if boundParams[i] != nil {
			params[i] = param.BinaryParam{
				Tp:  mysql.TypeBlob,
				Val: boundParams[i],
			}

			// The legacy logic is kept: if the `paramTypes` somehow didn't contain the type information, it will be treated as
			// BLOB type. We didn't return `mysql.ErrMalformPacket` to keep compatibility with older versions, though it's
			// meaningless if every clients work properly.
			if (i<<1)+1 < len(paramTypes) {
				// Only TEXT or BLOB type will be sent through `SEND_LONG_DATA`.
				tp := paramTypes[i<<1]

				switch tp {
				case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBit:
					params[i].Tp = tp
					params[i].Val = enc.DecodeInput(boundParams[i])
				case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
					params[i].Tp = tp
					params[i].Val = boundParams[i]
				}
			}
			continue
		}

		// check nullBitMap to determine the NULL arguments.
		// ref https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
		// notice: some client(e.g. mariadb) will set nullBitMap even if data had be sent via ComStmtSendLongData,
		// so this check need place after boundParam's check.
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			var nilDatum types.Datum
			nilDatum.SetNull()
			params[i] = param.BinaryParam{
				Tp: mysql.TypeNull,
			}
			continue
		}

		if (i<<1)+1 >= len(paramTypes) {
			return pos, mysql.ErrMalformPacket
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0
		isNull := false

		decodeWithDecoder := false

		var length uint64
		switch tp {
		case mysql.TypeNull:
			length = 0
			isNull = true
		case mysql.TypeTiny:
			length = 1
		case mysql.TypeShort, mysql.TypeYear:
			length = 2
		case mysql.TypeInt24, mysql.TypeLong, mysql.TypeFloat:
			length = 4
		case mysql.TypeLonglong, mysql.TypeDouble:
			length = 8
		case mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}
			length = uint64(paramValues[pos])
			pos++
		case mysql.TypeNewDecimal, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}
			var n int
			length, isNull, n = util2.ParseLengthEncodedInt(paramValues[pos:])
			pos += n
		case mysql.TypeUnspecified, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeGeometry, mysql.TypeBit:
			if len(paramValues) < (pos + 1) {
				err = mysql.ErrMalformPacket
				return
			}
			var n int
			length, isNull, n = util2.ParseLengthEncodedInt(paramValues[pos:])
			pos += n
			decodeWithDecoder = true
		default:
			err = errUnknownFieldType.GenWithStack("stmt unknown field type %d", tp)
			return
		}

		if len(paramValues) < (pos + int(length)) {
			err = mysql.ErrMalformPacket
			return
		}
		params[i] = param.BinaryParam{
			Tp:         tp,
			IsUnsigned: isUnsigned,
			IsNull:     isNull,
			Val:        paramValues[pos : pos+int(length)],
		}
		if decodeWithDecoder {
			params[i].Val = enc.DecodeInput(params[i].Val)
		}
		pos += int(length)
	}
	return
}

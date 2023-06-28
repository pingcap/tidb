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

package parse

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	util2 "github.com/pingcap/tidb/server/internal/util"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/hack"
)

var errUnknownFieldType = dbterror.ClassServer.NewStd(errno.ErrUnknownFieldType)

// maxFetchSize constants
const (
	maxFetchSize = 1024
)

// ExecArgs parse execute arguments to datum slice.
func ExecArgs(sc *stmtctx.StatementContext, params []expression.Expression, boundParams [][]byte,
	nullBitmap, paramTypes, paramValues []byte, enc *util2.InputDecoder) (err error) {
	pos := 0
	var (
		tmp    interface{}
		v      []byte
		n      int
		isNull bool
	)
	if enc == nil {
		enc = util2.NewInputDecoder(charset.CharsetUTF8)
	}

	args := make([]types.Datum, len(params))
	for i := 0; i < len(args); i++ {
		// if params had received via ComStmtSendLongData, use them directly.
		// ref https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
		// see clientConn#handleStmtSendLongData
		if boundParams[i] != nil {
			args[i] = types.NewBytesDatum(enc.DecodeInput(boundParams[i]))
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
				pos, tmp = binaryDate(pos, paramValues)
			case 7:
				pos, tmp = binaryDateTime(pos, paramValues)
			case 11:
				pos, tmp = binaryTimestamp(pos, paramValues)
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
				pos, tmp = binaryDuration(pos, paramValues, isNegative)
			case 12:
				isNegative := paramValues[pos]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				pos++
				pos, tmp = binaryDurationWithMS(pos, paramValues, isNegative)
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

			v, isNull, n, err = util2.ParseLengthEncodedBytes(paramValues[pos:])
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
			v, isNull, n, err = util2.ParseLengthEncodedBytes(paramValues[pos:])
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

			v, isNull, n, err = util2.ParseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if !isNull {
				v = enc.DecodeInput(v)
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

	for i := range params {
		ft := new(types.FieldType)
		types.InferParamTypeFromUnderlyingValue(args[i].GetValue(), ft)
		params[i] = &expression.Constant{Value: args[i], RetType: ft}
	}
	return
}

func binaryDate(pos int, paramValues []byte) (int, string) {
	year := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
	pos += 2
	month := paramValues[pos]
	pos++
	day := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func binaryDateTime(pos int, paramValues []byte) (int, string) {
	pos, date := binaryDate(pos, paramValues)
	hour := paramValues[pos]
	pos++
	minute := paramValues[pos]
	pos++
	second := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func binaryTimestamp(pos int, paramValues []byte) (int, string) {
	pos, dateTime := binaryDateTime(pos, paramValues)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func binaryDuration(pos int, paramValues []byte, isNegative uint8) (int, string) {
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

func binaryDurationWithMS(pos int, paramValues []byte,
	isNegative uint8) (int, string) {
	pos, dur := binaryDuration(pos, paramValues, isNegative)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dur, microSecond)
}

// StmtFetchCmd parse COM_STMT_FETCH command
func StmtFetchCmd(data []byte) (stmtID uint32, fetchSize uint32, err error) {
	if len(data) != 8 {
		return 0, 0, mysql.ErrMalformPacket
	}
	// Please refer to https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html
	stmtID = binary.LittleEndian.Uint32(data[0:4])
	fetchSize = binary.LittleEndian.Uint32(data[4:8])
	if fetchSize > maxFetchSize {
		fetchSize = maxFetchSize
	}
	return
}

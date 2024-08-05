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

package param

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/hack"
)

var errUnknownFieldType = dbterror.ClassServer.NewStd(errno.ErrUnknownFieldType)

// BinaryParam stores the information decoded from the binary protocol
// It can be further parsed into `expression.Expression` through the `ExecArgs` function in this package
type BinaryParam struct {
	Tp         byte
	IsUnsigned bool
	IsNull     bool
	Val        []byte
}

// ExecArgs parse execute arguments to datum slice.
func ExecArgs(typectx types.Context, binaryParams []BinaryParam) (params []expression.Expression, err error) {
	var (
		tmp any
	)

	params = make([]expression.Expression, len(binaryParams))
	args := make([]types.Datum, len(binaryParams))
	for i := 0; i < len(args); i++ {
		tp := binaryParams[i].Tp
		isUnsigned := binaryParams[i].IsUnsigned

		switch tp {
		case mysql.TypeNull:
			var nilDatum types.Datum
			nilDatum.SetNull()
			args[i] = nilDatum
			continue

		case mysql.TypeTiny:
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(binaryParams[i].Val[0]))
			} else {
				args[i] = types.NewIntDatum(int64(int8(binaryParams[i].Val[0])))
			}
			continue

		case mysql.TypeShort, mysql.TypeYear:
			valU16 := binary.LittleEndian.Uint16(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU16))
			} else {
				args[i] = types.NewIntDatum(int64(int16(valU16)))
			}
			continue

		case mysql.TypeInt24, mysql.TypeLong:
			valU32 := binary.LittleEndian.Uint32(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(uint64(valU32))
			} else {
				args[i] = types.NewIntDatum(int64(int32(valU32)))
			}
			continue

		case mysql.TypeLonglong:
			valU64 := binary.LittleEndian.Uint64(binaryParams[i].Val)
			if isUnsigned {
				args[i] = types.NewUintDatum(valU64)
			} else {
				args[i] = types.NewIntDatum(int64(valU64))
			}
			continue

		case mysql.TypeFloat:
			args[i] = types.NewFloat32Datum(math.Float32frombits(binary.LittleEndian.Uint32(binaryParams[i].Val)))
			continue

		case mysql.TypeDouble:
			args[i] = types.NewFloat64Datum(math.Float64frombits(binary.LittleEndian.Uint64(binaryParams[i].Val)))
			continue

		case mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeDatetime:
			switch len(binaryParams[i].Val) {
			case 0:
				tmp = types.ZeroDatetimeStr
			case 4:
				_, tmp = binaryDate(0, binaryParams[i].Val)
			case 7:
				_, tmp = binaryDateTime(0, binaryParams[i].Val)
			case 11:
				_, tmp = binaryTimestamp(0, binaryParams[i].Val)
			case 13:
				_, tmp = binaryTimestampWithTZ(0, binaryParams[i].Val)
			default:
				err = mysql.ErrMalformPacket
				return
			}
			// TODO: generate the time datum directly
			var parseTime func(types.Context, string) (types.Time, error)
			switch tp {
			case mysql.TypeDate:
				parseTime = types.ParseDate
			case mysql.TypeDatetime:
				parseTime = types.ParseDatetime
			case mysql.TypeTimestamp:
				// To be compatible with MySQL, even the type of parameter is
				// TypeTimestamp, the return type should also be `Datetime`.
				parseTime = types.ParseDatetime
			}
			var time types.Time
			time, err = parseTime(typectx, tmp.(string))
			err = typectx.HandleTruncate(err)
			if err != nil {
				return
			}
			args[i] = types.NewDatum(time)
			continue

		case mysql.TypeDuration:
			fsp := 0
			switch len(binaryParams[i].Val) {
			case 0:
				tmp = "0"
			case 8:
				isNegative := binaryParams[i].Val[0]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				_, tmp = binaryDuration(1, binaryParams[i].Val, isNegative)
			case 12:
				isNegative := binaryParams[i].Val[0]
				if isNegative > 1 {
					err = mysql.ErrMalformPacket
					return
				}
				_, tmp = binaryDurationWithMS(1, binaryParams[i].Val, isNegative)
				fsp = types.MaxFsp
			default:
				err = mysql.ErrMalformPacket
				return
			}
			// TODO: generate the duration datum directly
			var dur types.Duration
			dur, _, err = types.ParseDuration(typectx, tmp.(string), fsp)
			err = typectx.HandleTruncate(err)
			if err != nil {
				return
			}
			args[i] = types.NewDatum(dur)
			continue
		case mysql.TypeNewDecimal:
			if binaryParams[i].IsNull {
				args[i] = types.NewDecimalDatum(nil)
			} else {
				var dec types.MyDecimal
				err = typectx.HandleTruncate(dec.FromString(binaryParams[i].Val))
				if err != nil {
					return nil, err
				}
				args[i] = types.NewDecimalDatum(&dec)
			}
			continue
		case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if binaryParams[i].IsNull {
				args[i] = types.NewBytesDatum(nil)
			} else {
				args[i] = types.NewBytesDatum(binaryParams[i].Val)
			}
			continue
		case mysql.TypeUnspecified, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeEnum, mysql.TypeSet, mysql.TypeGeometry, mysql.TypeBit:
			if !binaryParams[i].IsNull {
				tmp = string(hack.String(binaryParams[i].Val))
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

func binaryTimestampWithTZ(pos int, paramValues []byte) (int, string) {
	pos, timestamp := binaryTimestamp(pos, paramValues)
	tzShiftInMin := int16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
	tzShiftHour := tzShiftInMin / 60
	tzShiftAbsMin := tzShiftInMin % 60
	if tzShiftAbsMin < 0 {
		tzShiftAbsMin = -tzShiftAbsMin
	}
	pos += 2
	return pos, fmt.Sprintf("%s%+02d:%02d", timestamp, tzShiftHour, tzShiftAbsMin)
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

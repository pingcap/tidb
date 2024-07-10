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

package column

import (
	"fmt"
	"math"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/server/internal/dump"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
)

const maxColumnNameSize = 256

// Info contains information of a column
type Info struct {
	DefaultValue any
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	ColumnLength uint32
	Charset      uint16
	Flag         uint16
	Decimal      uint8
	Type         uint8
}

// Dump dumps Info to bytes.
func (column *Info) Dump(buffer []byte, d *ResultEncoder) []byte {
	return column.dump(buffer, d, false)
}

// DumpWithDefault dumps Info to bytes, including column defaults. This is used for ComFieldList responses.
func (column *Info) DumpWithDefault(buffer []byte, d *ResultEncoder) []byte {
	return column.dump(buffer, d, true)
}

func (column *Info) dump(buffer []byte, d *ResultEncoder, withDefault bool) []byte {
	if d == nil {
		d = NewResultEncoder(charset.CharsetUTF8MB4)
	}
	nameDump, orgnameDump := []byte(column.Name), []byte(column.OrgName)
	if len(nameDump) > maxColumnNameSize {
		nameDump = nameDump[0:maxColumnNameSize]
	}
	if len(orgnameDump) > maxColumnNameSize {
		orgnameDump = orgnameDump[0:maxColumnNameSize]
	}
	buffer = dump.LengthEncodedString(buffer, []byte("def"))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta([]byte(column.Schema)))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta([]byte(column.Table)))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta([]byte(column.OrgTable)))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta(nameDump))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta(orgnameDump))

	buffer = append(buffer, 0x0c)
	buffer = dump.Uint16(buffer, d.ColumnTypeInfoCharsetID(column))
	buffer = dump.Uint32(buffer, column.ColumnLength)
	buffer = append(buffer, dumpType(column.Type))
	buffer = dump.Uint16(buffer, DumpFlag(column.Type, column.Flag))
	buffer = append(buffer, column.Decimal)
	buffer = append(buffer, 0, 0)

	if withDefault {
		switch column.DefaultValue {
		case "CURRENT_TIMESTAMP", "CURRENT_DATE", nil:
			buffer = append(buffer, 251) // NULL
		default:
			defaultValStr := fmt.Sprintf("%v", column.DefaultValue)
			buffer = dump.LengthEncodedString(buffer, []byte(defaultValStr))
		}
	}

	return buffer
}

// DumpFlag dumps flag of a column.
func DumpFlag(tp byte, flag uint16) uint16 {
	switch tp {
	case mysql.TypeSet:
		return flag | uint16(mysql.SetFlag)
	case mysql.TypeEnum:
		return flag | uint16(mysql.EnumFlag)
	default:
		return flag
	}
}

func dumpType(tp byte) byte {
	switch tp {
	case mysql.TypeSet, mysql.TypeEnum:
		return mysql.TypeString
	default:
		return tp
	}
}

// DumpTextRow dumps a row to bytes.
func DumpTextRow(buffer []byte, columns []*Info, row chunk.Row, d *ResultEncoder) ([]byte, error) {
	if d == nil {
		d = NewResultEncoder(charset.CharsetUTF8MB4)
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
			return nil, err.ErrInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	return buffer, nil
}

// DumpBinaryRow dumps a row to bytes.
func DumpBinaryRow(buffer []byte, columns []*Info, row chunk.Row, d *ResultEncoder) ([]byte, error) {
	if d == nil {
		d = NewResultEncoder(charset.CharsetUTF8MB4)
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
			return nil, err.ErrInvalidType.GenWithStack("invalid type %v", columns[i].Type)
		}
	}
	return buffer, nil
}

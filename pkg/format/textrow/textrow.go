// Copyright 2026 PingCAP, Inc.
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

// Package textrow holds the MySQL text-protocol value serializer shared by the
// server (DumpTextRow) and the distributed exporter. AppendValueText produces
// the raw per-value text; callers add their own framing (length-encoding for the
// protocol, CSV/SQL escaping for the exporter).
package textrow

import (
	"bytes"
	"math"
	"slices"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// ErrInvalidType is returned by AppendValueText for a column type it cannot
// serialize. Callers may wrap it with their own error identity.
var ErrInvalidType = errors.New("invalid column type for text serialization")

// ColumnInfo carries the per-column attributes the value formatter needs. It is
// the importable subset of the server's column.Info.
type ColumnInfo struct {
	// Table is empty for expression results and non-empty for real table
	// columns; it gates the float/double precision override, matching the text
	// protocol.
	Table   string
	Charset uint16
	Flag    uint16
	Decimal uint8
	Type    uint8
}

// AppendValueText appends the text representation of row[idx] to dst and returns
// the extended slice. The value is charset-encoded via enc for string-like types
// but carries no length prefix. The caller handles NULL (row.IsNull(idx)).
func AppendValueText(dst []byte, row chunk.Row, idx int, col ColumnInfo, enc *ResultEncoder) ([]byte, error) {
	switch col.Type {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
		return strconv.AppendInt(dst, row.GetInt64(idx), 10), nil
	case mysql.TypeYear:
		year := row.GetInt64(idx)
		if year == 0 {
			return append(dst, '0', '0', '0', '0'), nil
		}
		return strconv.AppendInt(dst, year, 10), nil
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(uint(col.Flag)) {
			return strconv.AppendUint(dst, row.GetUint64(idx), 10), nil
		}
		return strconv.AppendInt(dst, row.GetInt64(idx), 10), nil
	case mysql.TypeFloat:
		prec := -1
		if col.Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec && col.Table == "" {
			prec = int(col.Decimal)
		}
		return AppendFormatFloat(dst, float64(row.GetFloat32(idx)), prec, 32), nil
	case mysql.TypeDouble:
		prec := types.UnspecifiedLength
		if col.Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec && col.Table == "" {
			prec = int(col.Decimal)
		}
		return AppendFormatFloat(dst, row.GetFloat64(idx), prec, 64), nil
	case mysql.TypeNewDecimal:
		return append(dst, hack.Slice(row.GetMyDecimal(idx).String())...), nil
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		enc.UpdateDataEncoding(col.Charset)
		return append(dst, enc.EncodeData(row.GetBytes(idx))...), nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return append(dst, hack.Slice(row.GetTime(idx).String())...), nil
	case mysql.TypeDuration:
		dur := row.GetDuration(idx, int(col.Decimal))
		return append(dst, hack.Slice(dur.String())...), nil
	case mysql.TypeEnum:
		enc.UpdateDataEncoding(col.Charset)
		return append(dst, enc.EncodeData(hack.Slice(row.GetEnum(idx).String()))...), nil
	case mysql.TypeSet:
		enc.UpdateDataEncoding(col.Charset)
		return append(dst, enc.EncodeData(hack.Slice(row.GetSet(idx).String()))...), nil
	case mysql.TypeJSON:
		// The collation of JSON type is always binary.
		// To compatible with MySQL, here we treat it as utf-8.
		enc.UpdateDataEncoding(mysql.DefaultCollationID)
		return append(dst, enc.EncodeData(hack.Slice(row.GetJSON(idx).String()))...), nil
	case mysql.TypeTiDBVectorFloat32:
		enc.UpdateDataEncoding(mysql.DefaultCollationID)
		return append(dst, enc.EncodeData(hack.Slice(row.GetVectorFloat32(idx).String()))...), nil
	default:
		return nil, ErrInvalidType
	}
}

const (
	expFormatBig     = 1e15
	expFormatSmall   = 1e-15
	defaultMySQLPrec = 5
)

// AppendFormatFloat appends a float64 to dst in MySQL format.
func AppendFormatFloat(in []byte, fVal float64, prec, bitSize int) []byte {
	absVal := math.Abs(fVal)
	if absVal > math.MaxFloat64 || math.IsNaN(absVal) {
		return []byte{'0'}
	}
	isEFormat := false
	if bitSize == 32 {
		isEFormat = float32(absVal) >= expFormatBig || (float32(absVal) != 0 && float32(absVal) < expFormatSmall)
	} else {
		isEFormat = absVal >= expFormatBig || (absVal != 0 && absVal < expFormatSmall)
	}
	var out []byte
	if isEFormat {
		if bitSize == 32 {
			prec = defaultMySQLPrec
		}
		out = strconv.AppendFloat(in, fVal, 'e', prec, bitSize)
		valStr := out[len(in):]
		// remove the '+' from the string for compatibility.
		plusPos := bytes.IndexByte(valStr, '+')
		if plusPos > 0 {
			plusPosInOut := len(in) + plusPos
			out = slices.Delete(out, plusPosInOut, plusPosInOut+1)
		}
		// remove extra '0'
		ePos := bytes.IndexByte(valStr, 'e')
		pointPos := bytes.IndexByte(valStr, '.')
		ePosInOut := len(in) + ePos
		pointPosInOut := len(in) + pointPos
		validPos := ePosInOut
		for i := ePosInOut - 1; i >= pointPosInOut; i-- {
			if !(out[i] == '0' || out[i] == '.') {
				break
			}
			validPos = i
		}
		out = append(out[:validPos], out[ePosInOut:]...)
	} else {
		out = strconv.AppendFloat(in, fVal, 'f', prec, bitSize)
	}
	return out
}

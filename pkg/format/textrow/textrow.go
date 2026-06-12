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

// Package textrow holds the MySQL text-protocol value serializer. Today its only
// consumer is the server (DumpTextRow); it is factored out as a low-level package
// so an upcoming distributed exporter can reuse it. FormatValueText produces the
// raw per-value text; callers add their own framing (length-encoding for the
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

// ErrInvalidType is returned by FormatValueText for a column type it cannot
// serialize. Callers may wrap it with their own error identity.
var ErrInvalidType = errors.New("invalid column type for text serialization")

// ColumnInfo carries the per-column attributes the value formatter needs.
type ColumnInfo struct {
	Table   string
	Charset uint16
	Flag    uint16
	Decimal uint8
	Type    uint8
}

// FormatValueText returns the text representation of row[idx], charset-encoded via
// enc for string-like types and without a length prefix. The result is backed by
// enc's internal buffers and must be consumed before the next call. The caller
// handles NULL (row.IsNull(idx)).
func FormatValueText(row chunk.Row, idx int, col ColumnInfo, enc *ResultEncoder) ([]byte, error) {
	tmp := enc.scratch[:0]
	switch col.Type {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
		return strconv.AppendInt(tmp, row.GetInt64(idx), 10), nil
	case mysql.TypeYear:
		year := row.GetInt64(idx)
		if year == 0 {
			return append(tmp, '0', '0', '0', '0'), nil
		}
		return strconv.AppendInt(tmp, year, 10), nil
	case mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(uint(col.Flag)) {
			return strconv.AppendUint(tmp, row.GetUint64(idx), 10), nil
		}
		return strconv.AppendInt(tmp, row.GetInt64(idx), 10), nil
	case mysql.TypeFloat:
		return AppendFormatFloat(tmp, float64(row.GetFloat32(idx)), floatPrec(col), 32), nil
	case mysql.TypeDouble:
		return AppendFormatFloat(tmp, row.GetFloat64(idx), floatPrec(col), 64), nil
	case mysql.TypeNewDecimal:
		return hack.Slice(row.GetMyDecimal(idx).String()), nil
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		enc.UpdateDataEncoding(col.Charset)
		return enc.EncodeData(row.GetBytes(idx)), nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return hack.Slice(row.GetTime(idx).String()), nil
	case mysql.TypeDuration:
		dur := row.GetDuration(idx, int(col.Decimal))
		return hack.Slice(dur.String()), nil
	case mysql.TypeEnum:
		enc.UpdateDataEncoding(col.Charset)
		return enc.EncodeData(hack.Slice(row.GetEnum(idx).String())), nil
	case mysql.TypeSet:
		enc.UpdateDataEncoding(col.Charset)
		return enc.EncodeData(hack.Slice(row.GetSet(idx).String())), nil
	case mysql.TypeJSON:
		// The collation of JSON type is always binary.
		// To compatible with MySQL, here we treat it as utf-8.
		enc.UpdateDataEncoding(mysql.DefaultCollationID)
		return enc.EncodeData(hack.Slice(row.GetJSON(idx).String())), nil
	case mysql.TypeTiDBVectorFloat32:
		enc.UpdateDataEncoding(mysql.DefaultCollationID)
		return enc.EncodeData(hack.Slice(row.GetVectorFloat32(idx).String())), nil
	default:
		return nil, ErrInvalidType
	}
}

// floatPrec returns the precision used to format a float/double column: the
// column's Decimal when it is a fixed precision carried on an expression result
// (Table == ""), otherwise -1 for full precision. Real table columns keep full
// precision, matching the text protocol.
func floatPrec(col ColumnInfo) int {
	if col.Decimal > 0 && int(col.Decimal) != mysql.NotFixedDec && col.Table == "" {
		return int(col.Decimal)
	}
	return types.UnspecifiedLength
}

const (
	expFormatBig     = 1e15
	expFormatSmall   = 1e-15
	defaultMySQLPrec = 5
)

// AppendFormatFloat appends fVal's MySQL-format text to in and returns the
// extended slice.
func AppendFormatFloat(in []byte, fVal float64, prec, bitSize int) []byte {
	absVal := math.Abs(fVal)
	if absVal > math.MaxFloat64 || math.IsNaN(absVal) {
		return append(in, '0')
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

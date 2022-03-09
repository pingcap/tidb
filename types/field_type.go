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

package types

import (
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	ast "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
	utilMath "github.com/pingcap/tidb/util/math"
)

// UnspecifiedLength is unspecified length.
const UnspecifiedLength = -1

// ErrorLength is error length for blob or text.
const ErrorLength = 0

// FieldType records field type information.
type FieldType = ast.FieldType

// NewFieldType returns a FieldType,
// with a type and other information about field type.
func NewFieldType(tp byte) *FieldType {
	ft := &FieldType{
		Tp:      tp,
		Flen:    UnspecifiedLength,
		Decimal: UnspecifiedLength,
	}
	ft.Charset, ft.Collate = DefaultCharsetForType(tp)
	return ft
}

// NewFieldTypeWithCollation returns a FieldType,
// with a type and other information about field type.
func NewFieldTypeWithCollation(tp byte, collation string, length int) *FieldType {
	coll, _ := charset.GetCollationByName(collation)
	return &FieldType{
		Tp:      tp,
		Flen:    length,
		Decimal: UnspecifiedLength,
		Collate: collation,
		Charset: coll.CharsetName,
	}
}

// AggFieldType aggregates field types for a multi-argument function like `IF`, `IFNULL`, `COALESCE`
// whose return type is determined by the arguments' FieldTypes.
// Aggregation is performed by MergeFieldType function.
func AggFieldType(tps []*FieldType) *FieldType {
	var currType FieldType
	isMixedSign := false
	for i, t := range tps {
		if i == 0 && currType.Tp == mysql.TypeUnspecified {
			currType = *t
			continue
		}
		mtp := MergeFieldType(currType.Tp, t.Tp)
		isMixedSign = isMixedSign || (mysql.HasUnsignedFlag(currType.Flag) != mysql.HasUnsignedFlag(t.Flag))
		currType.Tp = mtp
		currType.Flag = mergeTypeFlag(currType.Flag, t.Flag)
	}
	// integral promotion when tps contains signed and unsigned
	if isMixedSign && IsTypeInteger(currType.Tp) {
		bumpRange := false // indicate one of tps bump currType range
		for _, t := range tps {
			bumpRange = bumpRange || (mysql.HasUnsignedFlag(t.Flag) && (t.Tp == currType.Tp || t.Tp == mysql.TypeBit))
		}
		if bumpRange {
			switch currType.Tp {
			case mysql.TypeTiny:
				currType.Tp = mysql.TypeShort
			case mysql.TypeShort:
				currType.Tp = mysql.TypeInt24
			case mysql.TypeInt24:
				currType.Tp = mysql.TypeLong
			case mysql.TypeLong:
				currType.Tp = mysql.TypeLonglong
			case mysql.TypeLonglong:
				currType.Tp = mysql.TypeNewDecimal
			}
		}
	}

	if mysql.HasUnsignedFlag(currType.Flag) && !isMixedSign {
		currType.Flag |= mysql.UnsignedFlag
	}

	return &currType
}

// TryToFixFlenOfDatetime try to fix flen of Datetime for specific func or other field merge cases
func TryToFixFlenOfDatetime(resultTp *FieldType) {
	if resultTp.Tp == mysql.TypeDatetime {
		resultTp.Flen = mysql.MaxDatetimeWidthNoFsp
		if resultTp.Decimal > 0 {
			resultTp.Flen += resultTp.Decimal + 1
		}
	}
}

// AggregateEvalType aggregates arguments' EvalType of a multi-argument function.
func AggregateEvalType(fts []*FieldType, flag *uint) EvalType {
	var (
		aggregatedEvalType = ETString
		unsigned           bool
		gotFirst           bool
		gotBinString       bool
	)
	lft := fts[0]
	for _, ft := range fts {
		if ft.Tp == mysql.TypeNull {
			continue
		}
		et := ft.EvalType()
		rft := ft
		if (IsTypeBlob(ft.Tp) || IsTypeVarchar(ft.Tp) || IsTypeChar(ft.Tp)) && mysql.HasBinaryFlag(ft.Flag) {
			gotBinString = true
		}
		if !gotFirst {
			gotFirst = true
			aggregatedEvalType = et
			unsigned = mysql.HasUnsignedFlag(ft.Flag)
		} else {
			aggregatedEvalType = mergeEvalType(aggregatedEvalType, et, lft, rft, unsigned, mysql.HasUnsignedFlag(ft.Flag))
			unsigned = unsigned && mysql.HasUnsignedFlag(ft.Flag)
		}
		lft = rft
	}
	SetTypeFlag(flag, mysql.UnsignedFlag, unsigned)
	SetTypeFlag(flag, mysql.BinaryFlag, !aggregatedEvalType.IsStringKind() || gotBinString)
	return aggregatedEvalType
}

func mergeEvalType(lhs, rhs EvalType, lft, rft *FieldType, isLHSUnsigned, isRHSUnsigned bool) EvalType {
	if lft.Tp == mysql.TypeUnspecified || rft.Tp == mysql.TypeUnspecified {
		if lft.Tp == rft.Tp {
			return ETString
		}
		if lft.Tp == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() || rhs.IsStringKind() {
		return ETString
	} else if lhs == ETReal || rhs == ETReal {
		return ETReal
	} else if lhs == ETDecimal || rhs == ETDecimal || isLHSUnsigned != isRHSUnsigned {
		return ETDecimal
	}
	return ETInt
}

// SetTypeFlag turns the flagItem on or off.
func SetTypeFlag(flag *uint, flagItem uint, on bool) {
	if on {
		*flag |= flagItem
	} else {
		*flag &= ^flagItem
	}
}

// DefaultParamTypeForValue returns the default FieldType for the parameterized value.
func DefaultParamTypeForValue(value interface{}, tp *FieldType) {
	switch value.(type) {
	case nil:
		tp.Tp = mysql.TypeVarString
		tp.Flen = UnspecifiedLength
		tp.Decimal = UnspecifiedLength
	default:
		DefaultTypeForValue(value, tp, mysql.DefaultCharset, mysql.DefaultCollationName)
		if hasVariantFieldLength(tp) {
			tp.Flen = UnspecifiedLength
		}
		if tp.Tp == mysql.TypeUnspecified {
			tp.Tp = mysql.TypeVarString
		}
	}
}

func hasVariantFieldLength(tp *FieldType) bool {
	switch tp.Tp {
	case mysql.TypeLonglong, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeBlob,
		mysql.TypeBit, mysql.TypeDuration, mysql.TypeEnum, mysql.TypeSet:
		return true
	}
	return false
}

// DefaultTypeForValue returns the default FieldType for the value.
func DefaultTypeForValue(value interface{}, tp *FieldType, char string, collate string) {
	if value != nil {
		tp.Flag |= mysql.NotNullFlag
	}
	switch x := value.(type) {
	case nil:
		tp.Tp = mysql.TypeNull
		tp.Flen = 0
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case bool:
		tp.Tp = mysql.TypeLonglong
		tp.Flen = 1
		tp.Decimal = 0
		tp.Flag |= mysql.IsBooleanFlag
		SetBinChsClnFlag(tp)
	case int:
		tp.Tp = mysql.TypeLonglong
		tp.Flen = utilMath.StrLenOfInt64Fast(int64(x))
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case int64:
		tp.Tp = mysql.TypeLonglong
		tp.Flen = utilMath.StrLenOfInt64Fast(x)
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case uint64:
		tp.Tp = mysql.TypeLonglong
		tp.Flag |= mysql.UnsignedFlag
		tp.Flen = utilMath.StrLenOfUint64Fast(x)
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case string:
		tp.Tp = mysql.TypeVarString
		// TODO: tp.Flen should be len(x) * 3 (max bytes length of CharsetUTF8)
		tp.Flen = len(x)
		tp.Decimal = UnspecifiedLength
		tp.Charset, tp.Collate = char, collate
	case float32:
		tp.Tp = mysql.TypeFloat
		s := strconv.FormatFloat(float64(x), 'f', -1, 32)
		tp.Flen = len(s)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case float64:
		tp.Tp = mysql.TypeDouble
		s := strconv.FormatFloat(x, 'f', -1, 64)
		tp.Flen = len(s)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case []byte:
		tp.Tp = mysql.TypeBlob
		tp.Flen = len(x)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case BitLiteral:
		tp.Tp = mysql.TypeVarString
		tp.Flen = len(x) * 3
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case HexLiteral:
		tp.Tp = mysql.TypeVarString
		tp.Flen = len(x) * 3
		tp.Decimal = 0
		tp.Flag |= mysql.UnsignedFlag
		SetBinChsClnFlag(tp)
	case BinaryLiteral:
		tp.Tp = mysql.TypeVarString
		tp.Flen = len(x)
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
		tp.Flag &= ^mysql.BinaryFlag
		tp.Flag |= mysql.UnsignedFlag
	case Time:
		tp.Tp = x.Type()
		switch x.Type() {
		case mysql.TypeDate:
			tp.Flen = mysql.MaxDateWidth
			tp.Decimal = UnspecifiedLength
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			tp.Flen = mysql.MaxDatetimeWidthNoFsp
			if x.Fsp() > DefaultFsp { // consider point('.') and the fractional part.
				tp.Flen += x.Fsp() + 1
			}
			tp.Decimal = x.Fsp()
		}
		SetBinChsClnFlag(tp)
	case Duration:
		tp.Tp = mysql.TypeDuration
		tp.Flen = len(x.String())
		if x.Fsp > DefaultFsp { // consider point('.') and the fractional part.
			tp.Flen = x.Fsp + 1
		}
		tp.Decimal = x.Fsp
		SetBinChsClnFlag(tp)
	case *MyDecimal:
		tp.Tp = mysql.TypeNewDecimal
		tp.Flen = len(x.ToString())
		tp.Decimal = int(x.digitsFrac)
		// Add the length for `.`.
		tp.Flen++
		SetBinChsClnFlag(tp)
	case Enum:
		tp.Tp = mysql.TypeEnum
		tp.Flen = len(x.Name)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case Set:
		tp.Tp = mysql.TypeSet
		tp.Flen = len(x.Name)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case json.BinaryJSON:
		tp.Tp = mysql.TypeJSON
		tp.Flen = UnspecifiedLength
		tp.Decimal = 0
		tp.Charset = charset.CharsetUTF8MB4
		tp.Collate = charset.CollationUTF8MB4
	default:
		tp.Tp = mysql.TypeUnspecified
		tp.Flen = UnspecifiedLength
		tp.Decimal = UnspecifiedLength
		tp.Charset = charset.CharsetUTF8MB4
		tp.Collate = charset.CollationUTF8MB4
	}
}

// DefaultCharsetForType returns the default charset/collation for mysql type.
func DefaultCharsetForType(tp byte) (string, string) {
	switch tp {
	case mysql.TypeVarString, mysql.TypeString, mysql.TypeVarchar:
		// Default charset for string types is utf8mb4.
		return mysql.DefaultCharset, mysql.DefaultCollationName
	}
	return charset.CharsetBin, charset.CollationBin
}

// MergeFieldType merges two MySQL type to a new type.
// This is used in hybrid field type expression.
// For example "select case c when 1 then 2 when 2 then 'tidb' from t;"
// The result field type of the case expression is the merged type of the two when clause.
// See https://github.com/mysql/mysql-server/blob/8.0/sql/field.cc#L1042
func MergeFieldType(a byte, b byte) byte {
	ia := getFieldTypeIndex(a)
	ib := getFieldTypeIndex(b)
	return fieldTypeMergeRules[ia][ib]
}

// mergeTypeFlag merges two MySQL type flag to a new one
// currently only NotNullFlag and UnsignedFlag is checked
// todo more flag need to be checked
func mergeTypeFlag(a, b uint) uint {
	return a & (b&mysql.NotNullFlag | ^mysql.NotNullFlag) & (b&mysql.UnsignedFlag | ^mysql.UnsignedFlag)
}

func getFieldTypeIndex(tp byte) int {
	itp := int(tp)
	if itp < fieldTypeTearFrom {
		return itp
	}
	return fieldTypeTearFrom + itp - fieldTypeTearTo - 1
}

const (
	fieldTypeTearFrom = int(mysql.TypeBit) + 1
	fieldTypeTearTo   = int(mysql.TypeJSON) - 1
	fieldTypeNum      = fieldTypeTearFrom + (255 - fieldTypeTearTo)
)

// https://github.com/mysql/mysql-server/blob/8.0/sql/field.cc#L248
var fieldTypeMergeRules = [fieldTypeNum][fieldTypeNum]byte{
	/* mysql.TypeUnspecified -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeUnspecified, mysql.TypeUnspecified,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeTiny -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeTiny,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTiny, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeTiny,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeShort -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeShort,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeShort, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeShort,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeLong -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeLong,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLong, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLong, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLong,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeFloat -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeDouble, mysql.TypeFloat,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeFloat, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeFloat, mysql.TypeFloat,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeFloat,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeDouble,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeDouble, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeDouble -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDouble, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeDouble,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeDouble,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeDouble, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeNull -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeTiny,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNull, mysql.TypeTimestamp,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDate, mysql.TypeDuration,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeYear,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeBit,
		// mysql.TypeJSON
		mysql.TypeJSON,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeEnum,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeSet, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeGeometry,
	},
	/* mysql.TypeTimestamp -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTimestamp, mysql.TypeTimestamp,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeLonglong -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeLonglong,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLonglong, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLonglong,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeInt24 -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeInt24,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeInt24, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeInt24, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeInt24,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal    mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeDate -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDate, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDate, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeTime -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDuration, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDuration,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeDatetime -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDatetime, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeYear -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeUnspecified, mysql.TypeTiny,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeYear, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeYear,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLonglong,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeNewDate -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDate, mysql.TypeDatetime,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeNewDate, mysql.TypeDatetime,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeVarchar -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeVarchar, mysql.TypeVarchar,
	},
	/* mysql.TypeBit -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeLonglong,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeBit, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLonglong,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLonglong,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeBit,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeJSON -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewFloat     mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeJSON, mysql.TypeVarchar,
		// mysql.TypeLongLONG     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         MYSQL_TYPE_TIME
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     MYSQL_TYPE_YEAR
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeJSON,
		// mysql.TypeNewDecimal   MYSQL_TYPE_ENUM
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeLongBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeLongBlob, mysql.TypeVarchar,
		// mysql.TypeString       MYSQL_TYPE_GEOMETRY
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeNewDecimal -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeNewDecimal,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeNewDecimal,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeEnum -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeEnum, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeSet -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeSet, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeTinyBlob -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeTinyBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeTinyBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
	},
	/* mysql.TypeMediumBlob -> */
	{
		// mysql.TypeUnspecified    mysql.TypeTiny
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeMediumBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
	},
	/* mysql.TypeLongBlob -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeLongBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeLongBlob, mysql.TypeLongBlob,
	},
	/* mysql.TypeBlob -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeBlob,
		// mysql.TypeJSON
		mysql.TypeLongBlob,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeBlob,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeBlob, mysql.TypeBlob,
	},
	/* mysql.TypeVarString -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeVarchar, mysql.TypeVarchar,
	},
	/* mysql.TypeString -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeString, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeString,
		// mysql.TypeJSON
		mysql.TypeString,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeString, mysql.TypeString,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeString, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeString,
	},
	/* mysql.TypeGeometry -> */
	{
		// mysql.TypeUnspecified  mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeGeometry, mysql.TypeVarchar,
		// mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		// mysql.TypeJSON
		mysql.TypeVarchar,
		// mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		// mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		// mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		// mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		// mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeGeometry,
	},
}

// SetBinChsClnFlag sets charset, collation as 'binary' and adds binaryFlag to FieldType.
func SetBinChsClnFlag(ft *FieldType) {
	ft.Charset = charset.CharsetBin
	ft.Collate = charset.CollationBin
	ft.Flag |= mysql.BinaryFlag
}

// VarStorageLen indicates this column is a variable length column.
const VarStorageLen = ast.VarStorageLen

// CheckModifyTypeCompatible checks whether changes column type to another is compatible and can be changed.
// If types are compatible and can be directly changed, nil err will be returned; otherwise the types are incompatible.
// There are two cases when types incompatible:
// 1. returned canReorg == true: types can be changed by reorg
// 2. returned canReorg == false: type change not supported yet
func CheckModifyTypeCompatible(origin *FieldType, to *FieldType) (canReorg bool, err error) {
	// Deal with the same type.
	if origin.Tp == to.Tp {
		if origin.Tp == mysql.TypeEnum || origin.Tp == mysql.TypeSet {
			typeVar := "set"
			if origin.Tp == mysql.TypeEnum {
				typeVar = "enum"
			}
			if len(to.Elems) < len(origin.Elems) {
				msg := fmt.Sprintf("the number of %s column's elements is less than the original: %d", typeVar, len(origin.Elems))
				return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
			}
			for index, originElem := range origin.Elems {
				toElem := to.Elems[index]
				if originElem != toElem {
					msg := fmt.Sprintf("cannot modify %s column value %s to %s", typeVar, originElem, toElem)
					return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
				}
			}
		}

		if origin.Tp == mysql.TypeNewDecimal {
			// Floating-point and fixed-point types also can be UNSIGNED. As with integer types, this attribute prevents
			// negative values from being stored in the column. Unlike the integer types, the upper range of column values
			// remains the same.
			if to.Flen != origin.Flen || to.Decimal != origin.Decimal || mysql.HasUnsignedFlag(to.Flag) != mysql.HasUnsignedFlag(origin.Flag) {
				msg := fmt.Sprintf("decimal change from decimal(%d, %d) to decimal(%d, %d)", origin.Flen, origin.Decimal, to.Flen, to.Decimal)
				return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
			}
		}

		needReorg, reason := needReorgToChange(origin, to)
		if !needReorg {
			return false, nil
		}
		return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(reason)
	}

	// Deal with the different type.
	if !checkTypeChangeSupported(origin, to) {
		unsupportedMsg := fmt.Sprintf("change from original type %v to %v is currently unsupported yet", origin.CompactStr(), to.CompactStr())
		return false, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(unsupportedMsg)
	}

	// Check if different type can directly convert and no need to reorg.
	stringToString := IsString(origin.Tp) && IsString(to.Tp)
	integerToInteger := mysql.IsIntegerType(origin.Tp) && mysql.IsIntegerType(to.Tp)
	if stringToString || integerToInteger {
		needReorg, reason := needReorgToChange(origin, to)
		if !needReorg {
			return false, nil
		}
		return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(reason)
	}

	notCompatibleMsg := fmt.Sprintf("type %v not match origin %v", to.CompactStr(), origin.CompactStr())
	return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(notCompatibleMsg)
}

func needReorgToChange(origin *FieldType, to *FieldType) (needReorg bool, reasonMsg string) {
	toFlen := to.Flen
	originFlen := origin.Flen
	if mysql.IsIntegerType(to.Tp) && mysql.IsIntegerType(origin.Tp) {
		// For integers, we should ignore the potential display length represented by flen, using
		// the default flen of the type.
		originFlen, _ = mysql.GetDefaultFieldLengthAndDecimal(origin.Tp)
		toFlen, _ = mysql.GetDefaultFieldLengthAndDecimal(to.Tp)
	}

	if ConvertBetweenCharAndVarchar(origin.Tp, to.Tp) {
		return true, "conversion between char and varchar string needs reorganization"
	}

	if toFlen > 0 && toFlen != originFlen {
		if toFlen < originFlen {
			return true, fmt.Sprintf("length %d is less than origin %d", toFlen, originFlen)
		}

		// Due to the behavior of padding \x00 at binary type, we need to reorg when binary length changed
		isBinaryType := func(tp *FieldType) bool { return tp.Tp == mysql.TypeString && IsBinaryStr(tp) }
		if isBinaryType(origin) && isBinaryType(to) {
			return true, "can't change binary types of different length"
		}
	}
	if to.Decimal > 0 && to.Decimal < origin.Decimal {
		return true, fmt.Sprintf("decimal %d is less than origin %d", to.Decimal, origin.Decimal)
	}
	if mysql.HasUnsignedFlag(origin.Flag) != mysql.HasUnsignedFlag(to.Flag) {
		return true, "can't change unsigned integer to signed or vice versa"
	}
	return false, ""
}

func checkTypeChangeSupported(origin *FieldType, to *FieldType) bool {
	if (IsTypeTime(origin.Tp) || origin.Tp == mysql.TypeDuration || origin.Tp == mysql.TypeYear ||
		IsString(origin.Tp) || origin.Tp == mysql.TypeJSON) &&
		to.Tp == mysql.TypeBit {
		// TODO: Currently date/datetime/timestamp/time/year/string/json data type cast to bit are not compatible with mysql, should fix here after compatible.
		return false
	}

	if (IsTypeTime(origin.Tp) || origin.Tp == mysql.TypeDuration || origin.Tp == mysql.TypeYear ||
		origin.Tp == mysql.TypeNewDecimal || origin.Tp == mysql.TypeFloat || origin.Tp == mysql.TypeDouble || origin.Tp == mysql.TypeJSON || origin.Tp == mysql.TypeBit) &&
		(to.Tp == mysql.TypeEnum || to.Tp == mysql.TypeSet) {
		// TODO: Currently date/datetime/timestamp/time/year/decimal/float/double/json/bit cast to enum/set are not support yet, should fix here after supported.
		return false
	}

	if (origin.Tp == mysql.TypeEnum || origin.Tp == mysql.TypeSet || origin.Tp == mysql.TypeBit ||
		origin.Tp == mysql.TypeNewDecimal || origin.Tp == mysql.TypeFloat || origin.Tp == mysql.TypeDouble) &&
		(IsTypeTime(to.Tp)) {
		// TODO: Currently enum/set/bit/decimal/float/double cast to date/datetime/timestamp type are not support yet, should fix here after supported.
		return false
	}

	if (origin.Tp == mysql.TypeEnum || origin.Tp == mysql.TypeSet || origin.Tp == mysql.TypeBit) &&
		to.Tp == mysql.TypeDuration {
		// TODO: Currently enum/set/bit cast to time are not support yet, should fix here after supported.
		return false
	}

	return true
}

// ConvertBetweenCharAndVarchar is that Column type conversion between varchar to char need reorganization because
// 1. varchar -> char: char type is stored with the padding removed. All the indexes need to be rewritten.
// 2. char -> varchar: the index value encoding of secondary index on clustered primary key tables is different.
// These secondary indexes need to be rewritten.
func ConvertBetweenCharAndVarchar(oldCol, newCol byte) bool {
	return (IsTypeVarchar(oldCol) && newCol == mysql.TypeString) ||
		(oldCol == mysql.TypeString && IsTypeVarchar(newCol) && collate.NewCollationEnabled())
}

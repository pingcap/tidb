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

package types

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/format"
)

// UnspecifiedLength is unspecified length.
const (
	UnspecifiedLength int = -1
)

// FieldType records field type information.
type FieldType struct {
	Tp      byte
	Flag    uint
	Flen    int
	Decimal int
	Charset string
	Collate string
	// Elems is the element list for enum and set type.
	Elems []string
}

// NewFieldType returns a FieldType,
// with a type and other information about field type.
func NewFieldType(tp byte) *FieldType {
	return &FieldType{
		Tp:      tp,
		Flen:    UnspecifiedLength,
		Decimal: UnspecifiedLength,
	}
}

// Equal checks whether two FieldType objects are equal.
func (ft *FieldType) Equal(other *FieldType) bool {
	partialEqual := ft.Tp == other.Tp &&
		ft.Flag == other.Flag &&
		ft.Flen == other.Flen &&
		ft.Decimal == other.Decimal &&
		ft.Charset == other.Charset &&
		ft.Collate == other.Collate
	if !partialEqual || len(ft.Elems) != len(other.Elems) {
		return false
	}
	for i := range ft.Elems {
		if ft.Elems[i] != other.Elems[i] {
			return false
		}
	}
	return true
}

// AggFieldType aggregates field types for a multi-argument function like `IF`, `IFNULL`, `COALESCE`
// whose return type is determined by the arguments' FieldTypes.
// Aggregation is performed by MergeFieldType function.
func AggFieldType(tps []*FieldType) *FieldType {
	var currType FieldType
	for i, t := range tps {
		if i == 0 && currType.Tp == mysql.TypeUnspecified {
			currType = *t
			continue
		}
		mtp := MergeFieldType(currType.Tp, t.Tp)
		currType.Tp = mtp
	}

	return &currType
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
	setTypeFlag(flag, mysql.UnsignedFlag, unsigned)
	setTypeFlag(flag, mysql.BinaryFlag, !aggregatedEvalType.IsStringKind() || gotBinString)
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

func setTypeFlag(flag *uint, flagItem uint, on bool) {
	if on {
		*flag |= flagItem
	} else {
		*flag &= ^flagItem
	}
}

// EvalType gets the type in evaluation.
func (ft *FieldType) EvalType() EvalType {
	switch ft.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeBit, mysql.TypeYear:
		return ETInt
	case mysql.TypeFloat, mysql.TypeDouble:
		return ETReal
	case mysql.TypeNewDecimal:
		return ETDecimal
	case mysql.TypeDate, mysql.TypeDatetime:
		return ETDatetime
	case mysql.TypeTimestamp:
		return ETTimestamp
	case mysql.TypeDuration:
		return ETDuration
	case mysql.TypeJSON:
		return ETJson
	}
	return ETString
}

// Hybrid checks whether a type is a hybrid type, which can represent different types of value in specific context.
func (ft *FieldType) Hybrid() bool {
	return ft.Tp == mysql.TypeEnum || ft.Tp == mysql.TypeBit || ft.Tp == mysql.TypeSet
}

// Init initializes the FieldType data.
func (ft *FieldType) Init(tp byte) {
	ft.Tp = tp
	ft.Flen = UnspecifiedLength
	ft.Decimal = UnspecifiedLength
}

// CompactStr only considers Tp/CharsetBin/Flen/Deimal.
// This is used for showing column type in infoschema.
func (ft *FieldType) CompactStr() string {
	ts := TypeToStr(ft.Tp, ft.Charset)
	suffix := ""

	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.Tp)
	isFlenNotDefault := ft.Flen != defaultFlen && ft.Flen != 0 && ft.Flen != UnspecifiedLength
	isDecimalNotDefault := ft.Decimal != defaultDecimal && ft.Decimal != 0 && ft.Decimal != UnspecifiedLength

	// displayFlen and displayDecimal are flen and decimal values with `-1` substituted with default value.
	displayFlen, displayDecimal := ft.Flen, ft.Decimal
	if displayFlen == 0 || displayFlen == UnspecifiedLength {
		displayFlen = defaultFlen
	}
	if displayDecimal == 0 || displayDecimal == UnspecifiedLength {
		displayDecimal = defaultDecimal
	}

	switch ft.Tp {
	case mysql.TypeEnum, mysql.TypeSet:
		// Format is ENUM ('e1', 'e2') or SET ('e1', 'e2')
		es := make([]string, 0, len(ft.Elems))
		for _, e := range ft.Elems {
			e = format.OutputFormat(e)
			es = append(es, e)
		}
		suffix = fmt.Sprintf("('%s')", strings.Join(es, "','"))
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		if isDecimalNotDefault {
			suffix = fmt.Sprintf("(%d)", displayDecimal)
		}
	case mysql.TypeDouble, mysql.TypeFloat:
		// 1. Flen Not Default, Decimal Not Default -> Valid
		// 2. Flen Not Default, Decimal Default (-1) -> Invalid
		// 3. Flen Default, Decimal Not Default -> Valid
		// 4. Flen Default, Decimal Default -> Valid (hide)
		if isDecimalNotDefault {
			suffix = fmt.Sprintf("(%d,%d)", displayFlen, displayDecimal)
		}
	case mysql.TypeNewDecimal:
		if isFlenNotDefault || isDecimalNotDefault {
			suffix = fmt.Sprintf("(%d", displayFlen)
			if isDecimalNotDefault {
				suffix += fmt.Sprintf(",%d", displayDecimal)
			}
			suffix += ")"
		}
	case mysql.TypeBit, mysql.TypeShort, mysql.TypeTiny, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		// Flen is always shown.
		suffix = fmt.Sprintf("(%d)", displayFlen)
	}
	return ts + suffix
}

// InfoSchemaStr joins the CompactStr with unsigned flag and
// returns a string.
func (ft *FieldType) InfoSchemaStr() string {
	suffix := ""
	if mysql.HasUnsignedFlag(ft.Flag) {
		suffix = " unsigned"
	}
	return ft.CompactStr() + suffix
}

// String joins the information of FieldType and returns a string.
// Note: when flen or decimal is unspecified, this function will use the default value instead of -1.
func (ft *FieldType) String() string {
	strs := []string{ft.CompactStr()}
	if mysql.HasUnsignedFlag(ft.Flag) {
		strs = append(strs, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		strs = append(strs, "ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.Flag) && ft.Tp != mysql.TypeString {
		strs = append(strs, "BINARY")
	}

	if IsTypeChar(ft.Tp) || IsTypeBlob(ft.Tp) {
		if ft.Charset != "" && ft.Charset != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("CHARACTER SET %s", ft.Charset))
		}
		if ft.Collate != "" && ft.Collate != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("COLLATE %s", ft.Collate))
		}
	}

	return strings.Join(strs, " ")
}

// FormatAsCastType is used for write AST back to string.
func (ft *FieldType) FormatAsCastType(w io.Writer) {
	switch ft.Tp {
	case mysql.TypeVarString:
		if ft.Charset == charset.CharsetBin && ft.Collate == charset.CollationBin {
			fmt.Fprintf(w, "BINARY")
		} else {
			fmt.Fprintf(w, "CHAR")
		}
		if ft.Flen != UnspecifiedLength {
			fmt.Fprintf(w, "(%d)", ft.Flen)
		}
		if ft.Flag&mysql.BinaryFlag != 0 {
			fmt.Fprintf(w, " BINARY")
		}
		if ft.Charset != charset.CharsetBin && ft.Charset != charset.CharsetUTF8 {
			fmt.Fprintf(w, " %s", ft.Charset)
		}
	case mysql.TypeDate:
		fmt.Fprintf(w, "DATE")
	case mysql.TypeDatetime:
		fmt.Fprintf(w, "DATETIME")
		if ft.Decimal > 0 {
			fmt.Fprintf(w, "(%d)", ft.Decimal)
		}
	case mysql.TypeNewDecimal:
		fmt.Fprintf(w, "DECIMAL")
		if ft.Flen > 0 && ft.Decimal > 0 {
			fmt.Fprintf(w, "(%d, %d)", ft.Flen, ft.Decimal)
		} else if ft.Flen > 0 {
			fmt.Fprintf(w, "(%d)", ft.Flen)
		}
	case mysql.TypeDuration:
		fmt.Fprintf(w, "TIME")
		if ft.Decimal > 0 {
			fmt.Fprintf(w, "(%d)", ft.Decimal)
		}
	case mysql.TypeLonglong:
		if ft.Flag&mysql.UnsignedFlag != 0 {
			fmt.Fprintf(w, "UNSIGNED")
		} else {
			fmt.Fprintf(w, "SIGNED")
		}
	case mysql.TypeJSON:
		fmt.Fprintf(w, "JSON")
	}
}

// DefaultParamTypeForValue returns the default FieldType for the parameterized value.
func DefaultParamTypeForValue(value interface{}, tp *FieldType) {
	switch value.(type) {
	case nil:
		tp.Tp = mysql.TypeUnspecified
		tp.Flen = UnspecifiedLength
		tp.Decimal = UnspecifiedLength
	default:
		DefaultTypeForValue(value, tp)
	}
}

// DefaultTypeForValue returns the default FieldType for the value.
func DefaultTypeForValue(value interface{}, tp *FieldType) {
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
		tp.Flen = len(strconv.FormatInt(int64(x), 10))
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case int64:
		tp.Tp = mysql.TypeLonglong
		tp.Flen = len(strconv.FormatInt(x, 10))
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case uint64:
		tp.Tp = mysql.TypeLonglong
		tp.Flag |= mysql.UnsignedFlag
		tp.Flen = len(strconv.FormatUint(x, 10))
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case string:
		tp.Tp = mysql.TypeVarString
		// TODO: tp.Flen should be len(x) * 3 (max bytes length of CharsetUTF8)
		tp.Flen = len(x)
		tp.Decimal = UnspecifiedLength
		tp.Charset = mysql.DefaultCharset
		tp.Collate = mysql.DefaultCollationName
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
		tp.Flen = len(x)
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case HexLiteral:
		tp.Tp = mysql.TypeVarString
		tp.Flen = len(x)
		tp.Decimal = 0
		tp.Flag |= mysql.UnsignedFlag
		SetBinChsClnFlag(tp)
	case BinaryLiteral:
		tp.Tp = mysql.TypeBit
		tp.Flen = len(x) * 8
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
		tp.Flag &= ^mysql.BinaryFlag
		tp.Flag |= mysql.UnsignedFlag
	case Time:
		tp.Tp = x.Type
		switch x.Type {
		case mysql.TypeDate:
			tp.Flen = mysql.MaxDateWidth
			tp.Decimal = UnspecifiedLength
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			tp.Flen = mysql.MaxDatetimeWidthNoFsp
			if x.Fsp > DefaultFsp { // consider point('.') and the fractional part.
				tp.Flen += x.Fsp + 1
			}
			tp.Decimal = x.Fsp
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
		tp.Charset = charset.CharsetBin
		tp.Collate = charset.CollationBin
	default:
		tp.Tp = mysql.TypeUnspecified
		tp.Flen = UnspecifiedLength
		tp.Decimal = UnspecifiedLength
	}
}

// DefaultCharsetForType returns the default charset/collation for mysql type.
func DefaultCharsetForType(tp byte) (string, string) {
	switch tp {
	case mysql.TypeVarString, mysql.TypeString, mysql.TypeVarchar:
		// Default charset for string types is utf8.
		return mysql.DefaultCharset, mysql.DefaultCollationName
	}
	return charset.CharsetBin, charset.CollationBin
}

// MergeFieldType merges two MySQL type to a new type.
// This is used in hybrid field type expression.
// For example "select case c when 1 then 2 when 2 then 'tidb' from t;"
// The result field type of the case expression is the merged type of the two when clause.
// See https://github.com/mysql/mysql-server/blob/5.7/sql/field.cc#L1042
func MergeFieldType(a byte, b byte) byte {
	ia := getFieldTypeIndex(a)
	ib := getFieldTypeIndex(b)
	return fieldTypeMergeRules[ia][ib]
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

var fieldTypeMergeRules = [fieldTypeNum][fieldTypeNum]byte{
	/* mysql.TypeDecimal -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeDecimal, mysql.TypeDecimal,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeTiny -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeTiny,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTiny, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeTiny,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeShort -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeShort,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeShort, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeShort,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeLong -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeLong,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeLong, mysql.TypeLong,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLong, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLong,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLong,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeFloat -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeDouble, mysql.TypeFloat,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeFloat, mysql.TypeDouble,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeFloat, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeFloat, mysql.TypeFloat,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeFloat,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeDouble, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeDouble -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDouble, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeDouble,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeDouble, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeNull -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeTiny,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNull, mysql.TypeTimestamp,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLonglong,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeNewDate, mysql.TypeDuration,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeYear,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeBit,
		//mysql.TypeJSON
		mysql.TypeJSON,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeEnum,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeSet, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeGeometry,
	},
	/* mysql.TypeTimestamp -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTimestamp, mysql.TypeTimestamp,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDatetime,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeLonglong -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeLonglong,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeLonglong, mysql.TypeLonglong,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLonglong, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeLong,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeLonglong,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeInt24 -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeInt24,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeInt24, mysql.TypeLong,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeInt24, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeInt24,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal    mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeDate -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDate, mysql.TypeDatetime,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeNewDate, mysql.TypeDatetime,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeTime -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDuration, mysql.TypeDatetime,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDuration,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeDatetime -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeDatetime, mysql.TypeDatetime,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeDatetime, mysql.TypeDatetime,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeYear -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeDecimal, mysql.TypeTiny,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeShort, mysql.TypeLong,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeFloat, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeYear, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLonglong, mysql.TypeInt24,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeYear,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeNewDate -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDate, mysql.TypeDatetime,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeNewDate, mysql.TypeDatetime,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeDatetime, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeNewDate, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeVarchar -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeVarchar, mysql.TypeVarchar,
	},
	/* mysql.TypeBit -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeBit, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeBit,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeJSON -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewFloat     mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeJSON, mysql.TypeVarchar,
		//mysql.TypeLongLONG     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         MYSQL_TYPE_TIME
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     MYSQL_TYPE_YEAR
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeJSON,
		//mysql.TypeNewDecimal   MYSQL_TYPE_ENUM
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeLongBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeLongBlob, mysql.TypeVarchar,
		//mysql.TypeString       MYSQL_TYPE_GEOMETRY
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeNewDecimal -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeDouble, mysql.TypeDouble,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeNewDecimal,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeNewDecimal, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeEnum -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeEnum, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeSet -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeSet, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeVarchar,
	},
	/* mysql.TypeTinyBlob -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeTinyBlob,
		//mysql.TypeJSON
		mysql.TypeLongBlob,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeTinyBlob,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeTinyBlob, mysql.TypeTinyBlob,
	},
	/* mysql.TypeMediumBlob -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeMediumBlob,
		//mysql.TypeJSON
		mysql.TypeLongBlob,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeMediumBlob, mysql.TypeMediumBlob,
	},
	/* mysql.TypeLongBlob -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeLongBlob,
		//mysql.TypeJSON
		mysql.TypeLongBlob,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeLongBlob, mysql.TypeLongBlob,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeLongBlob, mysql.TypeLongBlob,
	},
	/* mysql.TypeBlob -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeBlob,
		//mysql.TypeJSON
		mysql.TypeLongBlob,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeBlob,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeBlob, mysql.TypeBlob,
	},
	/* mysql.TypeVarString -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeVarchar, mysql.TypeVarchar,
	},
	/* mysql.TypeString -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeString, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeString,
		//mysql.TypeJSON
		mysql.TypeString,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeString, mysql.TypeString,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeString, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeString,
	},
	/* mysql.TypeGeometry -> */
	{
		//mysql.TypeDecimal      mysql.TypeTiny
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeShort        mysql.TypeLong
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeFloat        mysql.TypeDouble
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNull         mysql.TypeTimestamp
		mysql.TypeGeometry, mysql.TypeVarchar,
		//mysql.TypeLonglong     mysql.TypeInt24
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDate         mysql.TypeTime
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeDatetime     mysql.TypeYear
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeNewDate      mysql.TypeVarchar
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeBit          <16>-<244>
		mysql.TypeVarchar,
		//mysql.TypeJSON
		mysql.TypeVarchar,
		//mysql.TypeNewDecimal   mysql.TypeEnum
		mysql.TypeVarchar, mysql.TypeVarchar,
		//mysql.TypeSet          mysql.TypeTinyBlob
		mysql.TypeVarchar, mysql.TypeTinyBlob,
		//mysql.TypeMediumBlob  mysql.TypeLongBlob
		mysql.TypeMediumBlob, mysql.TypeLongBlob,
		//mysql.TypeBlob         mysql.TypeVarString
		mysql.TypeBlob, mysql.TypeVarchar,
		//mysql.TypeString       mysql.TypeGeometry
		mysql.TypeString, mysql.TypeGeometry,
	},
}

// SetBinChsClnFlag sets charset, collation as 'binary' and adds binaryFlag to FieldType.
func SetBinChsClnFlag(ft *FieldType) {
	ft.Charset = charset.CharsetBin
	ft.Collate = charset.CollationBin
	ft.Flag |= mysql.BinaryFlag
}

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
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ast "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mathutil"
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
	charset1, collate1 := DefaultCharsetForType(tp)
	flen, decimal := minFlenAndDecimalForType(tp)
	return NewFieldTypeBuilder().
		SetType(tp).
		SetCharset(charset1).
		SetCollate(collate1).
		SetFlen(flen).
		SetDecimal(decimal).
		BuildP()
}

// NewFieldTypeWithCollation returns a FieldType,
// with a type and other information about field type.
func NewFieldTypeWithCollation(tp byte, collation string, length int) *FieldType {
	coll, _ := charset.GetCollationByName(collation)
	return NewFieldTypeBuilder().SetType(tp).SetFlen(length).SetCharset(coll.CharsetName).SetCollate(collation).SetDecimal(UnspecifiedLength).BuildP()
}

// AggFieldType aggregates field types for a multi-argument function like `IF`, `IFNULL`, `COALESCE`
// whose return type is determined by the arguments' FieldTypes.
// Aggregation is performed by MergeFieldType function.
func AggFieldType(tps []*FieldType) *FieldType {
	var currType FieldType
	isMixedSign := false
	for i, t := range tps {
		if i == 0 && currType.GetType() == mysql.TypeUnspecified {
			currType = *t
			continue
		}
		mtp := mergeFieldType(currType.GetType(), t.GetType())
		isMixedSign = isMixedSign || (mysql.HasUnsignedFlag(currType.GetFlag()) != mysql.HasUnsignedFlag(t.GetFlag()))
		currType.SetType(mtp)
		currType.SetFlag(mergeTypeFlag(currType.GetFlag(), t.GetFlag()))
	}
	// integral promotion when tps contains signed and unsigned
	if isMixedSign && IsTypeInteger(currType.GetType()) {
		bumpRange := false // indicate one of tps bump currType range
		for _, t := range tps {
			bumpRange = bumpRange || (mysql.HasUnsignedFlag(t.GetFlag()) && (t.GetType() == currType.GetType() ||
				t.GetType() == mysql.TypeBit))
		}
		if bumpRange {
			switch currType.GetType() {
			case mysql.TypeTiny:
				currType.SetType(mysql.TypeShort)
			case mysql.TypeShort:
				currType.SetType(mysql.TypeInt24)
			case mysql.TypeInt24:
				currType.SetType(mysql.TypeLong)
			case mysql.TypeLong:
				currType.SetType(mysql.TypeLonglong)
			case mysql.TypeLonglong:
				currType.SetType(mysql.TypeNewDecimal)
			}
		}
	}

	if mysql.HasUnsignedFlag(currType.GetFlag()) && !isMixedSign {
		currType.AddFlag(mysql.UnsignedFlag)
	}

	return &currType
}

// TryToFixFlenOfDatetime try to fix flen of Datetime for specific func or other field merge cases
func TryToFixFlenOfDatetime(resultTp *FieldType) {
	if resultTp.GetType() == mysql.TypeDatetime {
		resultTp.SetFlen(mysql.MaxDatetimeWidthNoFsp)
		if resultTp.GetDecimal() > 0 {
			resultTp.SetFlen(resultTp.GetFlen() + resultTp.GetDecimal() + 1)
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
		if ft.GetType() == mysql.TypeNull {
			continue
		}
		et := ft.EvalType()
		rft := ft
		if (IsTypeBlob(ft.GetType()) || IsTypeVarchar(ft.GetType()) || IsTypeChar(ft.GetType())) && mysql.HasBinaryFlag(ft.GetFlag()) {
			gotBinString = true
		}
		if !gotFirst {
			gotFirst = true
			aggregatedEvalType = et
			unsigned = mysql.HasUnsignedFlag(ft.GetFlag())
		} else {
			aggregatedEvalType = mergeEvalType(aggregatedEvalType, et, lft, rft, unsigned, mysql.HasUnsignedFlag(ft.GetFlag()))
			unsigned = unsigned && mysql.HasUnsignedFlag(ft.GetFlag())
		}
		lft = rft
	}
	SetTypeFlag(flag, mysql.UnsignedFlag, unsigned)
	SetTypeFlag(flag, mysql.BinaryFlag, !aggregatedEvalType.IsStringKind() || gotBinString)
	return aggregatedEvalType
}

func mergeEvalType(lhs, rhs EvalType, lft, rft *FieldType, isLHSUnsigned, isRHSUnsigned bool) EvalType {
	if lft.GetType() == mysql.TypeUnspecified || rft.GetType() == mysql.TypeUnspecified {
		if lft.GetType() == rft.GetType() {
			return ETString
		}
		if lft.GetType() == mysql.TypeUnspecified {
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

// InferParamTypeFromDatum is used for plan cache to infer the type of a parameter from its datum.
func InferParamTypeFromDatum(d *Datum, tp *FieldType) {
	InferParamTypeFromUnderlyingValue(d.GetValue(), tp)
	if IsStringKind(d.k) {
		// consider charset and collation here
		c, err := collate.GetCollationByName(d.collation)
		if err != nil || c == nil {
			return // use default charset and collation
		}
		tp.SetCharset(c.CharsetName)
		tp.SetCollate(d.collation)
	}
}

// InferParamTypeFromUnderlyingValue is used for plan cache to infer the type of a parameter from its underlying value.
func InferParamTypeFromUnderlyingValue(value any, tp *FieldType) {
	switch value.(type) {
	case nil:
		// For NULL parameters, use TypeNull to ensure consistent behavior with literal NULL values
		// in control flow functions like CASE WHEN. Previously using TypeVarString caused incorrect
		// type inference in prepared statements (issue #62564).
		tp.SetType(mysql.TypeNull)
		tp.SetFlen(0)
		tp.SetDecimal(0)
		// Set default charset and collation instead of binary charset to avoid breaking JSON functions
		// (see PR #54145 which fixed JSON function argument verification issues).
		tp.SetCharset(mysql.DefaultCharset)
		tp.SetCollate(mysql.DefaultCollationName)
	default:
		DefaultTypeForValue(value, tp, mysql.DefaultCharset, mysql.DefaultCollationName)
		if hasVariantFieldLength(tp) {
			tp.SetFlen(UnspecifiedLength)
		}
		if tp.GetType() == mysql.TypeUnspecified {
			tp.SetType(mysql.TypeVarString)
		}
	}
}

func hasVariantFieldLength(tp *FieldType) bool {
	switch tp.GetType() {
	case mysql.TypeLonglong, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeBlob,
		mysql.TypeBit, mysql.TypeDuration, mysql.TypeEnum, mysql.TypeSet:
		return true
	}
	return false
}

// DefaultTypeForValue returns the default FieldType for the value.
func DefaultTypeForValue(value any, tp *FieldType, char string, collate string) {
	if value != nil {
		tp.AddFlag(mysql.NotNullFlag)
	}
	switch x := value.(type) {
	case nil:
		tp.SetType(mysql.TypeNull)
		tp.SetFlen(0)
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case bool:
		tp.SetType(mysql.TypeLonglong)
		tp.SetFlen(1)
		tp.SetDecimal(0)
		tp.AddFlag(mysql.IsBooleanFlag)
		SetBinChsClnFlag(tp)
	case int:
		tp.SetType(mysql.TypeLonglong)
		tp.SetFlen(mathutil.StrLenOfInt64Fast(int64(x)))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case int64:
		tp.SetType(mysql.TypeLonglong)
		tp.SetFlen(mathutil.StrLenOfInt64Fast(x))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case uint64:
		tp.SetType(mysql.TypeLonglong)
		tp.AddFlag(mysql.UnsignedFlag)
		tp.SetFlen(mathutil.StrLenOfUint64Fast(x))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case string:
		tp.SetType(mysql.TypeVarString)
		// TODO: tp.flen should be len(x) * 3 (max bytes length of CharsetUTF8)
		tp.SetFlen(len(x))
		tp.SetDecimal(UnspecifiedLength)
		tp.SetCharset(char)
		tp.SetCollate(collate)
	case float32:
		tp.SetType(mysql.TypeFloat)
		s := strconv.FormatFloat(float64(x), 'f', -1, 32)
		tp.SetFlen(len(s))
		tp.SetDecimal(UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case float64:
		tp.SetType(mysql.TypeDouble)
		s := strconv.FormatFloat(x, 'f', -1, 64)
		tp.SetFlen(len(s))
		tp.SetDecimal(UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case []byte:
		tp.SetType(mysql.TypeBlob)
		tp.SetFlen(len(x))
		tp.SetDecimal(UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case BitLiteral:
		tp.SetType(mysql.TypeVarString)
		tp.SetFlen(len(x) * 3)
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	case HexLiteral:
		tp.SetType(mysql.TypeVarString)
		tp.SetFlen(len(x) * 3)
		tp.SetDecimal(0)
		tp.AddFlag(mysql.UnsignedFlag)
		SetBinChsClnFlag(tp)
	case BinaryLiteral:
		tp.SetType(mysql.TypeVarString)
		tp.SetFlen(len(x))
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
		tp.DelFlag(mysql.BinaryFlag)
		tp.AddFlag(mysql.UnsignedFlag)
	case Time:
		tp.SetType(x.Type())
		switch x.Type() {
		case mysql.TypeDate:
			tp.SetFlen(mysql.MaxDateWidth)
			tp.SetDecimal(UnspecifiedLength)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			tp.SetFlen(mysql.MaxDatetimeWidthNoFsp)
			if x.Fsp() > DefaultFsp { // consider point('.') and the fractional part.
				tp.SetFlen(tp.GetFlen() + x.Fsp() + 1)
			}
			tp.SetDecimal(x.Fsp())
		}
		SetBinChsClnFlag(tp)
	case Duration:
		tp.SetType(mysql.TypeDuration)
		tp.SetFlen(len(x.String()))
		if x.Fsp > DefaultFsp { // consider point('.') and the fractional part.
			tp.SetFlen(x.Fsp + 1)
		}
		tp.SetDecimal(x.Fsp)
		SetBinChsClnFlag(tp)
	case *MyDecimal:
		tp.SetType(mysql.TypeNewDecimal)
		tp.SetFlenUnderLimit(len(x.ToString()))
		tp.SetDecimalUnderLimit(int(x.digitsFrac))
		// Add the length for `.`.
		tp.SetFlenUnderLimit(tp.GetFlen() + 1)
		SetBinChsClnFlag(tp)
	case Enum:
		tp.SetType(mysql.TypeEnum)
		tp.SetFlen(len(x.Name))
		tp.SetDecimal(UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case Set:
		tp.SetType(mysql.TypeSet)
		tp.SetFlen(len(x.Name))
		tp.SetDecimal(UnspecifiedLength)
		SetBinChsClnFlag(tp)
	case BinaryJSON:
		tp.SetType(mysql.TypeJSON)
		tp.SetFlen(UnspecifiedLength)
		tp.SetDecimal(0)
		tp.SetCharset(charset.CharsetUTF8MB4)
		tp.SetCollate(charset.CollationUTF8MB4)
	case VectorFloat32:
		tp.SetType(mysql.TypeTiDBVectorFloat32)
		tp.SetFlen(UnspecifiedLength)
		tp.SetDecimal(0)
		SetBinChsClnFlag(tp)
	default:
		tp.SetType(mysql.TypeUnspecified)
		tp.SetFlen(UnspecifiedLength)
		tp.SetDecimal(UnspecifiedLength)
		tp.SetCharset(charset.CharsetUTF8MB4)
		tp.SetCollate(charset.CollationUTF8MB4)
	}
}

// minFlenAndDecimalForType returns the minimum flen/decimal that can hold all the data for `tp`.
func minFlenAndDecimalForType(tp byte) (int, int) {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		return mysql.GetDefaultFieldLengthAndDecimal(tp)
	default:
		// todo support non-integer type
		return UnspecifiedLength, UnspecifiedLength
	}
}

// DefaultCharsetForType returns the default charset/collation for mysql type.
func DefaultCharsetForType(tp byte) (defaultCharset string, defaultCollation string) {
	switch tp {
	case mysql.TypeVarString, mysql.TypeString, mysql.TypeVarchar:
		// Default charset for string types is utf8mb4.
		return mysql.DefaultCharset, mysql.DefaultCollationName
	}
	return charset.CharsetBin, charset.CollationBin
}

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
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
)

// UnspecifiedLength is unspecified length.
const (
	UnspecifiedLength = -1
)

// TiDBStrictIntegerDisplayWidth represent whether return warnings when integerType with (length) was parsed.
// The default is `false`, it will be parsed as warning, and the result in show-create-table will ignore the
// display length when it set to `true`. This is for compatibility with MySQL 8.0 in which integer max display
// length is deprecated, referring this issue #6688 for more details.
var (
	TiDBStrictIntegerDisplayWidth bool
)

// FieldType records field type information.
type FieldType struct {
	tp      byte
	flag    uint
	flen    int
	decimal int
	charset string
	collate string
	// elems is the element list for enum and set type.
	elems []string
}

// NewFieldType returns a FieldType,
// with a type and other information about field type.
func NewFieldType(tp byte) *FieldType {
	return &FieldType{
		tp:      tp,
		flen:    UnspecifiedLength,
		decimal: UnspecifiedLength,
	}
}

func (ft *FieldType) GetType() byte {
	return ft.tp
}

func (ft *FieldType) GetFlag() uint {
	return ft.flag
}

func (ft *FieldType) GetFlen() int {
	return ft.flen
}

func (ft *FieldType) GetDecimal() int {
	return ft.decimal
}

func (ft *FieldType) GetCharset() string {
	return ft.charset
}

func (ft *FieldType) GetCollate() string {
	return ft.collate
}

func (ft *FieldType) GetElems() []string {
	return ft.elems
}

func (ft *FieldType) SetType(tp byte) {
	ft.tp = tp
}

func (ft *FieldType) SetFlag(flag uint) {
	ft.flag = flag
}

func (ft *FieldType) AddFlag(flag uint) {
	ft.flag |= flag
}

func (ft *FieldType) AndFlag(flag uint) {
	ft.flag &= flag
}

func (ft *FieldType) ToggleFlag(flag uint) {
	ft.flag ^= flag
}

func (ft *FieldType) DelFlag(flag uint) {
	ft.flag &= ^flag
}

func (ft *FieldType) SetFlen(flen int) {
	ft.flen = flen
}

func (ft *FieldType) SetDecimal(decimal int) {
	ft.decimal = decimal
}

func (ft *FieldType) SetCharset(charset string) {
	ft.charset = charset
}

func (ft *FieldType) SetCollate(collate string) {
	ft.collate = collate
}

func (ft *FieldType) SetElems(elems []string) {
	ft.elems = elems
}

func (ft *FieldType) SetElem(idx int, element string) {
	ft.elems[idx] = element
}

func (ft *FieldType) GetElem(idx int) string {
	return ft.elems[idx]
}

// Clone returns a copy of itself.
func (ft *FieldType) Clone() *FieldType {
	ret := *ft
	return &ret
}

// Equal checks whether two FieldType objects are equal.
func (ft *FieldType) Equal(other *FieldType) bool {
	// We do not need to compare whole `ft.flag == other.flag` when wrapping cast upon an Expression.
	// but need compare unsigned_flag of ft.flag.
	// When tp is float or double with decimal unspecified, do not check whether flen is equal,
	// because flen for them is useless.
	// The decimal field can be ignored if the type is int or string.
	tpEqual := (ft.tp == other.tp) || (ft.tp == mysql.TypeVarchar && other.tp == mysql.TypeVarString) || (ft.tp == mysql.TypeVarString && other.tp == mysql.TypeVarchar)
	flenEqual := ft.flen == other.flen || (ft.EvalType() == ETReal && ft.decimal == UnspecifiedLength)
	ignoreDecimal := ft.EvalType() == ETInt || ft.EvalType() == ETString
	partialEqual := tpEqual &&
		(ignoreDecimal || ft.decimal == other.decimal) &&
		ft.charset == other.charset &&
		ft.collate == other.collate &&
		flenEqual &&
		mysql.HasUnsignedFlag(ft.flag) == mysql.HasUnsignedFlag(other.flag)
	if !partialEqual || len(ft.elems) != len(other.elems) {
		return false
	}
	for i := range ft.elems {
		if ft.elems[i] != other.elems[i] {
			return false
		}
	}
	return true
}

// EvalType gets the type in evaluation.
func (ft *FieldType) EvalType() EvalType {
	switch ft.tp {
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
	case mysql.TypeEnum, mysql.TypeSet:
		if ft.flag&mysql.EnumSetAsIntFlag > 0 {
			return ETInt
		}
	}
	return ETString
}

// Hybrid checks whether a type is a hybrid type, which can represent different types of value in specific context.
func (ft *FieldType) Hybrid() bool {
	return ft.tp == mysql.TypeEnum || ft.tp == mysql.TypeBit || ft.tp == mysql.TypeSet
}

// Init initializes the FieldType data.
func (ft *FieldType) Init(tp byte) {
	ft.tp = tp
	ft.flen = UnspecifiedLength
	ft.decimal = UnspecifiedLength
}

// CompactStr only considers tp/CharsetBin/flen/Deimal.
// This is used for showing column type in infoschema.
func (ft *FieldType) CompactStr() string {
	ts := TypeToStr(ft.tp, ft.charset)
	suffix := ""

	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.tp)
	isDecimalNotDefault := ft.decimal != defaultDecimal && ft.decimal != 0 && ft.decimal != UnspecifiedLength

	// displayFlen and displayDecimal are flen and decimal values with `-1` substituted with default value.
	displayFlen, displayDecimal := ft.flen, ft.decimal
	if displayFlen == UnspecifiedLength {
		displayFlen = defaultFlen
	}
	if displayDecimal == UnspecifiedLength {
		displayDecimal = defaultDecimal
	}

	switch ft.tp {
	case mysql.TypeEnum, mysql.TypeSet:
		// Format is ENUM ('e1', 'e2') or SET ('e1', 'e2')
		es := make([]string, 0, len(ft.elems))
		for _, e := range ft.elems {
			e = format.OutputFormat(e)
			es = append(es, e)
		}
		suffix = fmt.Sprintf("('%s')", strings.Join(es, "','"))
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		if isDecimalNotDefault {
			suffix = fmt.Sprintf("(%d)", displayDecimal)
		}
	case mysql.TypeDouble, mysql.TypeFloat:
		// 1. flen Not Default, decimal Not Default -> Valid
		// 2. flen Not Default, decimal Default (-1) -> Invalid
		// 3. flen Default, decimal Not Default -> Valid
		// 4. flen Default, decimal Default -> Valid (hide)
		if isDecimalNotDefault {
			suffix = fmt.Sprintf("(%d,%d)", displayFlen, displayDecimal)
		}
	case mysql.TypeNewDecimal:
		suffix = fmt.Sprintf("(%d,%d)", displayFlen, displayDecimal)
	case mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		suffix = fmt.Sprintf("(%d)", displayFlen)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		// Referring this issue #6688, the integer max display length is deprecated in MySQL 8.0.
		// Since the length doesn't take any effect in TiDB storage or showing result, we remove it here.
		if !TiDBStrictIntegerDisplayWidth {
			suffix = fmt.Sprintf("(%d)", displayFlen)
		}
	case mysql.TypeYear:
		suffix = fmt.Sprintf("(%d)", ft.flen)
	}
	return ts + suffix
}

// InfoSchemaStr joins the CompactStr with unsigned flag and
// returns a string.
func (ft *FieldType) InfoSchemaStr() string {
	suffix := ""
	if mysql.HasUnsignedFlag(ft.flag) {
		suffix = " unsigned"
	}
	return ft.CompactStr() + suffix
}

// String joins the information of FieldType and returns a string.
// Note: when flen or decimal is unspecified, this function will use the default value instead of -1.
func (ft *FieldType) String() string {
	strs := []string{ft.CompactStr()}
	if mysql.HasUnsignedFlag(ft.flag) {
		strs = append(strs, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.flag) {
		strs = append(strs, "ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.flag) && ft.tp != mysql.TypeString {
		strs = append(strs, "BINARY")
	}

	if IsTypeChar(ft.tp) || IsTypeBlob(ft.tp) {
		if ft.charset != "" && ft.charset != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("CHARACTER SET %s", ft.charset))
		}
		if ft.collate != "" && ft.collate != charset.CharsetBin {
			strs = append(strs, fmt.Sprintf("COLLATE %s", ft.collate))
		}
	}

	return strings.Join(strs, " ")
}

// Restore implements Node interface.
func (ft *FieldType) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(TypeToStr(ft.tp, ft.charset))

	precision := UnspecifiedLength
	scale := UnspecifiedLength

	switch ft.tp {
	case mysql.TypeEnum, mysql.TypeSet:
		ctx.WritePlain("(")
		for i, e := range ft.elems {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteString(e)
		}
		ctx.WritePlain(")")
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		precision = ft.decimal
	case mysql.TypeUnspecified, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		precision = ft.flen
		scale = ft.decimal
	default:
		precision = ft.flen
	}

	if precision != UnspecifiedLength {
		ctx.WritePlainf("(%d", precision)
		if scale != UnspecifiedLength {
			ctx.WritePlainf(",%d", scale)
		}
		ctx.WritePlain(")")
	}

	if mysql.HasUnsignedFlag(ft.flag) {
		ctx.WriteKeyWord(" UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.flag) {
		ctx.WriteKeyWord(" ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.flag) && ft.charset != charset.CharsetBin {
		ctx.WriteKeyWord(" BINARY")
	}

	if IsTypeChar(ft.tp) || IsTypeBlob(ft.tp) {
		if ft.charset != "" && ft.charset != charset.CharsetBin {
			ctx.WriteKeyWord(" CHARACTER SET " + ft.charset)
		}
		if ft.collate != "" && ft.collate != charset.CharsetBin {
			ctx.WriteKeyWord(" COLLATE ")
			ctx.WritePlain(ft.collate)
		}
	}

	return nil
}

// RestoreAsCastType is used for write AST back to string.
func (ft *FieldType) RestoreAsCastType(ctx *format.RestoreCtx, explicitCharset bool) {
	switch ft.tp {
	case mysql.TypeVarString:
		skipWriteBinary := false
		if ft.charset == charset.CharsetBin && ft.collate == charset.CollationBin {
			ctx.WriteKeyWord("BINARY")
			skipWriteBinary = true
		} else {
			ctx.WriteKeyWord("CHAR")
		}
		if ft.flen != UnspecifiedLength {
			ctx.WritePlainf("(%d)", ft.flen)
		}
		if !explicitCharset {
			return
		}
		if !skipWriteBinary && ft.flag&mysql.BinaryFlag != 0 {
			ctx.WriteKeyWord(" BINARY")
		}
		if ft.charset != charset.CharsetBin && ft.charset != mysql.DefaultCharset {
			ctx.WriteKeyWord(" CHARSET ")
			ctx.WriteKeyWord(ft.charset)
		}
	case mysql.TypeDate:
		ctx.WriteKeyWord("DATE")
	case mysql.TypeDatetime:
		ctx.WriteKeyWord("DATETIME")
		if ft.decimal > 0 {
			ctx.WritePlainf("(%d)", ft.decimal)
		}
	case mysql.TypeNewDecimal:
		ctx.WriteKeyWord("DECIMAL")
		if ft.flen > 0 && ft.decimal > 0 {
			ctx.WritePlainf("(%d, %d)", ft.flen, ft.decimal)
		} else if ft.flen > 0 {
			ctx.WritePlainf("(%d)", ft.flen)
		}
	case mysql.TypeDuration:
		ctx.WriteKeyWord("TIME")
		if ft.decimal > 0 {
			ctx.WritePlainf("(%d)", ft.decimal)
		}
	case mysql.TypeLonglong:
		if ft.flag&mysql.UnsignedFlag != 0 {
			ctx.WriteKeyWord("UNSIGNED")
		} else {
			ctx.WriteKeyWord("SIGNED")
		}
	case mysql.TypeJSON:
		ctx.WriteKeyWord("JSON")
	case mysql.TypeDouble:
		ctx.WriteKeyWord("DOUBLE")
	case mysql.TypeFloat:
		ctx.WriteKeyWord("FLOAT")
	case mysql.TypeYear:
		ctx.WriteKeyWord("YEAR")
	}
}

// FormatAsCastType is used for write AST back to string.
func (ft *FieldType) FormatAsCastType(w io.Writer, explicitCharset bool) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	ft.RestoreAsCastType(restoreCtx, explicitCharset)
	fmt.Fprint(w, sb.String())
}

// VarStorageLen indicates this column is a variable length column.
const VarStorageLen = -1

// StorageLength is the length of stored value for the type.
func (ft *FieldType) StorageLength() int {
	switch ft.tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong,
		mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeFloat, mysql.TypeYear, mysql.TypeDuration,
		mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeEnum, mysql.TypeSet,
		mysql.TypeBit:
		// This may not be the accurate length, because we may encode them as varint.
		return 8
	case mysql.TypeNewDecimal:
		precision, frac := ft.flen-ft.decimal, ft.decimal
		return precision/digitsPerWord*wordSize + dig2bytes[precision%digitsPerWord] + frac/digitsPerWord*wordSize + dig2bytes[frac%digitsPerWord]
	default:
		return VarStorageLen
	}
}

// HasCharset indicates if a COLUMN has an associated charset. Returning false here prevents some information
// statements(like `SHOW CREATE TABLE`) from attaching a CHARACTER SET clause to the column.
func HasCharset(ft *FieldType) bool {
	switch ft.tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return !mysql.HasBinaryFlag(ft.flag)
	case mysql.TypeEnum, mysql.TypeSet:
		return true
	}
	return false
}

// for json
type jsonFieldType struct {
	Tp      byte
	Flag    uint
	Flen    int
	Decimal int
	Charset string
	Collate string
	Elems   []string
}

func (ft *FieldType) UnmarshalJSON(data []byte) error {
	var r jsonFieldType
	err := json.Unmarshal(data, &r)
	if err == nil {
		ft.tp = r.Tp
		ft.flag = r.Flag
		ft.flen = r.Flen
		ft.decimal = r.Decimal
		ft.charset = r.Charset
		ft.collate = r.Collate
		ft.elems = r.Elems
	}
	return err
}

func (ft *FieldType) MarshalJSON() ([]byte, error) {
	var r jsonFieldType
	r.Tp = ft.tp
	r.Flag = ft.flag
	r.Flen = ft.flen
	r.Decimal = ft.decimal
	r.Charset = ft.charset
	r.Collate = ft.collate
	r.Elems = ft.elems
	return json.Marshal(r)
}

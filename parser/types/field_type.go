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

func (ft *FieldType) GetTp() byte {
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

func (ft *FieldType) SetTp(tp byte) *FieldType {
	builder := ft.ToBuilder()
	builder.Tp = tp
	return builder.Build()
}

func (ft *FieldType) SetFlag(flag uint) *FieldType {
	builder := ft.ToBuilder()
	builder.Flag = flag
	return builder.Build()
}

func (ft *FieldType) SetFlen(flen int) *FieldType {
	builder := ft.ToBuilder()
	builder.Flen = flen
	return builder.Build()
}

func (ft *FieldType) SetDecimal(decimal int) *FieldType {
	builder := ft.ToBuilder()
	builder.Decimal = decimal
	return builder.Build()
}

func (ft *FieldType) SetCharset(charset string) *FieldType {
	builder := ft.ToBuilder()
	builder.Charset = charset
	return builder.Build()
}

func (ft *FieldType) SetCollate(collate string) *FieldType {
	builder := ft.ToBuilder()
	builder.Collate = collate
	return builder.Build()
}

func (ft *FieldType) SetElems(elms []string) *FieldType {
	builder := ft.ToBuilder()
	builder.Elems = elms
	return builder.Build()
}

func (ft *FieldType) ToBuilder() *FieldTypeBuilder {
	return &FieldTypeBuilder{
		Tp:      ft.tp,
		Flag:    ft.flag,
		Flen:    ft.flen,
		Decimal: ft.decimal,
		Charset: ft.charset,
		Collate: ft.collate,
		Elems:   append([]string(nil), ft.elems...), // copy elems
	}
}

// Equal checks whether two FieldType objects are equal.
func (ft *FieldType) Equal(other *FieldType) bool {
	// We do not need to compare whole `ft.flag == other.flag` when wrapping cast upon an Expression.
	// but need compare unsigned_flag of ft.flag.
	// When tp is float or double with decimal unspecified, do not check whether flen is equal,
	// because flen for them is useless.
	// The decimal field can be ignored if the type is int or string.
	tpEqual := (ft.tp == other.GetTp()) || (ft.tp == mysql.TypeVarchar && other.tp == mysql.TypeVarString) || (ft.tp == mysql.TypeVarString && other.tp == mysql.TypeVarchar)
	flenEqual := ft.flen == other.flen || (ft.EvalType() == ETReal && ft.decimal == UnspecifiedLength)
	ignoreDecimal := ft.EvalType() == ETInt || ft.EvalType() == ETString
	partialEqual := tpEqual &&
		(ignoreDecimal || ft.decimal == other.decimal) &&
		ft.GetCharset() == other.charset &&
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
	switch ft.GetTp() {
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

// CompactStr only considers tp/CharsetBin/flen/Deimal.
// This is used for showing column type in infoschema.
func (ft *FieldType) CompactStr() string {
	ts := TypeToStr(ft.tp, ft.charset)
	suffix := ""

	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.GetTp())
	isDecimalNotDefault := ft.decimal != defaultDecimal && ft.decimal != 0 && ft.decimal != UnspecifiedLength

	// displayFlen and displayDecimal are flen and decimal values with `-1` substituted with default value.
	displayFlen, displayDecimal := ft.flen, ft.decimal
	if displayFlen == UnspecifiedLength {
		displayFlen = defaultFlen
	}
	if displayDecimal == UnspecifiedLength {
		displayDecimal = defaultDecimal
	}

	switch ft.GetTp() {
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
		suffix = fmt.Sprintf("(%d)", ft.GetFlen())
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

	if IsTypeChar(ft.GetTp()) || IsTypeBlob(ft.GetTp()) {
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

	switch ft.GetTp() {
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

	if IsTypeChar(ft.GetTp()) || IsTypeBlob(ft.GetTp()) {
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
	switch ft.GetTp() {
	case mysql.TypeVarString:
		skipWriteBinary := false
		if ft.GetCharset() == charset.CharsetBin && ft.collate == charset.CollationBin {
			ctx.WriteKeyWord("BINARY")
			skipWriteBinary = true
		} else {
			ctx.WriteKeyWord("CHAR")
		}
		if ft.flen != UnspecifiedLength {
			ctx.WritePlainf("(%d)", ft.GetFlen())
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
		if ft.GetFlen() > 0 && ft.decimal > 0 {
			ctx.WritePlainf("(%d, %d)", ft.flen, ft.decimal)
		} else if ft.GetFlen() > 0 {
			ctx.WritePlainf("(%d)", ft.GetFlen())
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

// StorageLength is the length of stored value for the type.
func (ft *FieldType) StorageLength() int {
	switch ft.GetTp() {
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

// FieldTypeBuilder records field type information.
type FieldTypeBuilder struct {
	Tp      byte
	Flag    uint
	Flen    int
	Decimal int
	Charset string
	Collate string
	// Elems is the element list for enum and set type.
	Elems []string
}

// NewFieldTypeBuilder returns a FieldTypeBuilder,
// with a type and other information about field type.
func NewFieldTypeBuilder(tp byte) *FieldTypeBuilder {
	return &FieldTypeBuilder{
		Tp:      tp,
		Flen:    UnspecifiedLength,
		Decimal: UnspecifiedLength,
	}
}

// Build returns a new FieldType
func (ft FieldTypeBuilder) Build() *FieldType {
	return &FieldType{
		tp:      ft.Tp,
		flag:    ft.Flag,
		flen:    ft.Flen,
		decimal: ft.Decimal,
		charset: ft.Charset,
		collate: ft.Collate,
		elems:   ft.Elems,
	}
}

// Clone returns a copy of itself.
func (ft *FieldTypeBuilder) Clone() *FieldTypeBuilder {
	ret := *ft
	return &ret
}

// Equal checks whether two FieldTypeBuilder objects are equal.
func (ft *FieldTypeBuilder) Equal(other *FieldTypeBuilder) bool {
	// We do not need to compare whole `ft.Flag == other.Flag` when wrapping cast upon an Expression.
	// but need compare unsigned_flag of ft.Flag.
	// When Tp is float or double with Decimal unspecified, do not check whether Flen is equal,
	// because Flen for them is useless.
	// The Decimal field can be ignored if the type is int or string.
	tpEqual := (ft.Tp == other.Tp) || (ft.Tp == mysql.TypeVarchar && other.Tp == mysql.TypeVarString) || (ft.Tp == mysql.TypeVarString && other.Tp == mysql.TypeVarchar)
	flenEqual := ft.Flen == other.Flen || (ft.EvalType() == ETReal && ft.Decimal == UnspecifiedLength)
	ignoreDecimal := ft.EvalType() == ETInt || ft.EvalType() == ETString
	partialEqual := tpEqual &&
		(ignoreDecimal || ft.Decimal == other.Decimal) &&
		ft.Charset == other.Charset &&
		ft.Collate == other.Collate &&
		flenEqual &&
		mysql.HasUnsignedFlag(ft.Flag) == mysql.HasUnsignedFlag(other.Flag)
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

// EvalType gets the type in evaluation.
func (ft *FieldTypeBuilder) EvalType() EvalType {
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
	case mysql.TypeEnum, mysql.TypeSet:
		if ft.Flag&mysql.EnumSetAsIntFlag > 0 {
			return ETInt
		}
	}
	return ETString
}

// Hybrid checks whether a type is a hybrid type, which can represent different types of value in specific context.
func (ft *FieldTypeBuilder) Hybrid() bool {
	return ft.Tp == mysql.TypeEnum || ft.Tp == mysql.TypeBit || ft.Tp == mysql.TypeSet
}

// Init initializes the FieldTypeBuilder data.
func (ft *FieldTypeBuilder) Init(tp byte) {
	ft.Tp = tp
	ft.Flen = UnspecifiedLength
	ft.Decimal = UnspecifiedLength
}

// CompactStr only considers Tp/CharsetBin/Flen/Deimal.
// This is used for showing column type in infoschema.
func (ft *FieldTypeBuilder) CompactStr() string {
	ts := TypeToStr(ft.Tp, ft.Charset)
	suffix := ""

	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.Tp)
	isDecimalNotDefault := ft.Decimal != defaultDecimal && ft.Decimal != 0 && ft.Decimal != UnspecifiedLength

	// displayFlen and displayDecimal are flen and decimal values with `-1` substituted with default value.
	displayFlen, displayDecimal := ft.Flen, ft.Decimal
	if displayFlen == UnspecifiedLength {
		displayFlen = defaultFlen
	}
	if displayDecimal == UnspecifiedLength {
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
		suffix = fmt.Sprintf("(%d)", ft.Flen)
	}
	return ts + suffix
}

// InfoSchemaStr joins the CompactStr with unsigned flag and
// returns a string.
func (ft *FieldTypeBuilder) InfoSchemaStr() string {
	suffix := ""
	if mysql.HasUnsignedFlag(ft.Flag) {
		suffix = " unsigned"
	}
	return ft.CompactStr() + suffix
}

// String joins the information of FieldTypeBuilder and returns a string.
// Note: when flen or decimal is unspecified, this function will use the default value instead of -1.
func (ft *FieldTypeBuilder) String() string {
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

// Restore implements Node interface.
func (ft *FieldTypeBuilder) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(TypeToStr(ft.Tp, ft.Charset))

	precision := UnspecifiedLength
	scale := UnspecifiedLength

	switch ft.Tp {
	case mysql.TypeEnum, mysql.TypeSet:
		ctx.WritePlain("(")
		for i, e := range ft.Elems {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteString(e)
		}
		ctx.WritePlain(")")
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		precision = ft.Decimal
	case mysql.TypeUnspecified, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		precision = ft.Flen
		scale = ft.Decimal
	default:
		precision = ft.Flen
	}

	if precision != UnspecifiedLength {
		ctx.WritePlainf("(%d", precision)
		if scale != UnspecifiedLength {
			ctx.WritePlainf(",%d", scale)
		}
		ctx.WritePlain(")")
	}

	if mysql.HasUnsignedFlag(ft.Flag) {
		ctx.WriteKeyWord(" UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		ctx.WriteKeyWord(" ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.Flag) && ft.Charset != charset.CharsetBin {
		ctx.WriteKeyWord(" BINARY")
	}

	if IsTypeChar(ft.Tp) || IsTypeBlob(ft.Tp) {
		if ft.Charset != "" && ft.Charset != charset.CharsetBin {
			ctx.WriteKeyWord(" CHARACTER SET " + ft.Charset)
		}
		if ft.Collate != "" && ft.Collate != charset.CharsetBin {
			ctx.WriteKeyWord(" COLLATE ")
			ctx.WritePlain(ft.Collate)
		}
	}

	return nil
}

// RestoreAsCastType is used for write AST back to string.
func (ft *FieldTypeBuilder) RestoreAsCastType(ctx *format.RestoreCtx, explicitCharset bool) {
	switch ft.Tp {
	case mysql.TypeVarString:
		skipWriteBinary := false
		if ft.Charset == charset.CharsetBin && ft.Collate == charset.CollationBin {
			ctx.WriteKeyWord("BINARY")
			skipWriteBinary = true
		} else {
			ctx.WriteKeyWord("CHAR")
		}
		if ft.Flen != UnspecifiedLength {
			ctx.WritePlainf("(%d)", ft.Flen)
		}
		if !explicitCharset {
			return
		}
		if !skipWriteBinary && ft.Flag&mysql.BinaryFlag != 0 {
			ctx.WriteKeyWord(" BINARY")
		}
		if ft.Charset != charset.CharsetBin && ft.Charset != mysql.DefaultCharset {
			ctx.WriteKeyWord(" CHARSET ")
			ctx.WriteKeyWord(ft.Charset)
		}
	case mysql.TypeDate:
		ctx.WriteKeyWord("DATE")
	case mysql.TypeDatetime:
		ctx.WriteKeyWord("DATETIME")
		if ft.Decimal > 0 {
			ctx.WritePlainf("(%d)", ft.Decimal)
		}
	case mysql.TypeNewDecimal:
		ctx.WriteKeyWord("DECIMAL")
		if ft.Flen > 0 && ft.Decimal > 0 {
			ctx.WritePlainf("(%d, %d)", ft.Flen, ft.Decimal)
		} else if ft.Flen > 0 {
			ctx.WritePlainf("(%d)", ft.Flen)
		}
	case mysql.TypeDuration:
		ctx.WriteKeyWord("TIME")
		if ft.Decimal > 0 {
			ctx.WritePlainf("(%d)", ft.Decimal)
		}
	case mysql.TypeLonglong:
		if ft.Flag&mysql.UnsignedFlag != 0 {
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
func (ft *FieldTypeBuilder) FormatAsCastType(w io.Writer, explicitCharset bool) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	ft.RestoreAsCastType(restoreCtx, explicitCharset)
	fmt.Fprint(w, sb.String())
}

// VarStorageLen indicates this column is a variable length column.
const VarStorageLen = -1

// StorageLength is the length of stored value for the type.
func (ft *FieldTypeBuilder) StorageLength() int {
	switch ft.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong,
		mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeFloat, mysql.TypeYear, mysql.TypeDuration,
		mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeEnum, mysql.TypeSet,
		mysql.TypeBit:
		// This may not be the accurate length, because we may encode them as varint.
		return 8
	case mysql.TypeNewDecimal:
		precision, frac := ft.Flen-ft.Decimal, ft.Decimal
		return precision/digitsPerWord*wordSize + dig2bytes[precision%digitsPerWord] + frac/digitsPerWord*wordSize + dig2bytes[frac%digitsPerWord]
	default:
		return VarStorageLen
	}
}

// HasCharset indicates if a COLUMN has an associated charset. Returning false here prevents some information
// statements(like `SHOW CREATE TABLE`) from attaching a CHARACTER SET clause to the column.
func HasCharset(ft *FieldTypeBuilder) bool {
	switch ft.Tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return !mysql.HasBinaryFlag(ft.Flag)
	case mysql.TypeEnum, mysql.TypeSet:
		return true
	}
	return false
}

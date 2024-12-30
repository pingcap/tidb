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
	"slices"
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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

// IHasher is internal usage represent cascades/base.Hasher
type IHasher interface {
	HashBool(val bool)
	HashInt(val int)
	HashInt64(val int64)
	HashUint64(val uint64)
	HashFloat64(val float64)
	HashRune(val rune)
	HashString(val string)
	HashByte(val byte)
	HashBytes(val []byte)
	Reset()
	Sum64() uint64
}

// FieldType records field type information.
type FieldType struct {
	// tp is type of the field
	tp byte
	// flag represent NotNull, Unsigned, PriKey flags etc.
	flag uint
	// flen represent size of bytes of the field
	flen int
	// decimal represent decimal length of the field
	decimal int
	// charset represent character set
	charset string
	// collate represent collate rules of the charset
	collate string
	// elems is the element list for enum and set type.
	elems            []string
	elemsIsBinaryLit []bool
	array            bool
	// Please keep in mind that jsonFieldType should be updated if you add a new field here.
}

// Hash64 implements the cascades/base.Hasher.<0th> interface.
func (ft *FieldType) Hash64(h IHasher) {
	h.HashByte(ft.tp)
	h.HashUint64(uint64(ft.flag))
	h.HashInt(ft.flen)
	h.HashInt(ft.decimal)
	h.HashString(ft.charset)
	h.HashString(ft.collate)
	h.HashInt(len(ft.elems))
	for _, elem := range ft.elems {
		h.HashString(elem)
	}
	h.HashInt(len(ft.elemsIsBinaryLit))
	for _, elem := range ft.elemsIsBinaryLit {
		h.HashBool(elem)
	}
	h.HashBool(ft.array)
}

// Equals implements the cascades/base.Hasher.<1th> interface.
func (ft *FieldType) Equals(other any) bool {
	ft2, ok := other.(*FieldType)
	if !ok {
		return false
	}
	if ft == nil {
		return ft2 == nil
	}
	if other == nil {
		return false
	}
	ok = ft.tp == ft2.tp &&
		ft.flag == ft2.flag &&
		ft.flen == ft2.flen &&
		ft.decimal == ft2.decimal &&
		ft.charset == ft2.charset &&
		ft.collate == ft2.collate &&
		ft.array == ft2.array
	if !ok {
		return false
	}
	if len(ft.elems) != len(ft2.elems) {
		return false
	}
	for i, one := range ft.elems {
		if one != ft2.elems[i] {
			return false
		}
	}
	if len(ft.elemsIsBinaryLit) != len(ft2.elemsIsBinaryLit) {
		return false
	}
	for i, one := range ft.elemsIsBinaryLit {
		if one != ft2.elemsIsBinaryLit[i] {
			return false
		}
	}
	return true
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

// IsDecimalValid checks whether the decimal is valid.
func (ft *FieldType) IsDecimalValid() bool {
	if ft.GetType() == mysql.TypeNewDecimal && (ft.decimal < 0 || ft.decimal > mysql.MaxDecimalScale || ft.flen <= 0 || ft.flen > mysql.MaxDecimalWidth || ft.flen < ft.decimal) {
		return false
	}
	return true
}

// IsVarLengthType Determine whether the column type is a variable-length type
func (ft *FieldType) IsVarLengthType() bool {
	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeJSON, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeTiDBVectorFloat32:
		return true
	default:
		return false
	}
}

// GetType returns the type of the FieldType.
func (ft *FieldType) GetType() byte {
	if ft.array {
		return mysql.TypeJSON
	}
	return ft.tp
}

// GetFlag returns the flag of the FieldType.
func (ft *FieldType) GetFlag() uint {
	return ft.flag
}

// GetFlen returns the length of the field.
func (ft *FieldType) GetFlen() int {
	return ft.flen
}

// GetDecimal returns the decimal of the FieldType.
func (ft *FieldType) GetDecimal() int {
	return ft.decimal
}

// GetCharset returns the field's charset
func (ft *FieldType) GetCharset() string {
	return ft.charset
}

// GetCollate returns the collation of the field.
func (ft *FieldType) GetCollate() string {
	return ft.collate
}

// GetElems returns the elements of the FieldType.
func (ft *FieldType) GetElems() []string {
	return ft.elems
}

// SetType sets the type of the FieldType.
func (ft *FieldType) SetType(tp byte) {
	ft.tp = tp
	ft.array = false
}

// SetFlag sets the flag of the FieldType.
func (ft *FieldType) SetFlag(flag uint) {
	ft.flag = flag
}

// AddFlag adds a flag to the FieldType.
func (ft *FieldType) AddFlag(flag uint) {
	ft.flag |= flag
}

// AndFlag and the flag of the FieldType.
func (ft *FieldType) AndFlag(flag uint) {
	ft.flag &= flag
}

// ToggleFlag toggle the flag of the FieldType.
func (ft *FieldType) ToggleFlag(flag uint) {
	ft.flag ^= flag
}

// DelFlag delete the flag of the FieldType.
func (ft *FieldType) DelFlag(flag uint) {
	ft.flag &= ^flag
}

// SetFlen sets the length of the field.
func (ft *FieldType) SetFlen(flen int) {
	ft.flen = flen
}

// SetFlenUnderLimit sets the length of the field to the value of the argument
func (ft *FieldType) SetFlenUnderLimit(flen int) {
	if ft.GetType() == mysql.TypeNewDecimal {
		ft.flen = min(flen, mysql.MaxDecimalWidth)
	} else {
		ft.flen = flen
	}
}

// SetDecimal sets the decimal of the FieldType.
func (ft *FieldType) SetDecimal(decimal int) {
	ft.decimal = decimal
}

// SetDecimalUnderLimit sets the decimal of the field to the value of the argument
func (ft *FieldType) SetDecimalUnderLimit(decimal int) {
	if ft.GetType() == mysql.TypeNewDecimal {
		ft.decimal = min(decimal, mysql.MaxDecimalScale)
	} else {
		ft.decimal = decimal
	}
}

// UpdateFlenAndDecimalUnderLimit updates the length and decimal to the value of the argument
func (ft *FieldType) UpdateFlenAndDecimalUnderLimit(old *FieldType, deltaDecimal int, deltaFlen int) {
	if ft.GetType() != mysql.TypeNewDecimal {
		return
	}
	if old.decimal < 0 {
		deltaFlen += mysql.MaxDecimalScale
		ft.decimal = mysql.MaxDecimalScale
	} else {
		ft.SetDecimal(old.decimal + deltaDecimal)
	}
	if old.flen < 0 {
		ft.flen = mysql.MaxDecimalWidth
	} else {
		ft.SetFlenUnderLimit(old.flen + deltaFlen)
	}
}

// SetCharset sets the charset of the FieldType.
func (ft *FieldType) SetCharset(charset string) {
	ft.charset = charset
}

// SetCollate sets the collation of the FieldType.
func (ft *FieldType) SetCollate(collate string) {
	ft.collate = collate
}

// SetElems sets the elements of the FieldType.
func (ft *FieldType) SetElems(elems []string) {
	ft.elems = elems
}

// SetElem sets the element of the FieldType.
func (ft *FieldType) SetElem(idx int, element string) {
	ft.elems[idx] = element
}

// SetArray sets the array field of the FieldType.
func (ft *FieldType) SetArray(array bool) {
	ft.array = array
}

// IsArray return true if the filed type is array.
func (ft *FieldType) IsArray() bool {
	return ft.array
}

// ArrayType return the type of the array.
func (ft *FieldType) ArrayType() *FieldType {
	if !ft.array {
		return ft
	}
	clone := ft.Clone()
	clone.SetArray(false)
	return clone
}

// SetElemWithIsBinaryLit sets the element of the FieldType.
func (ft *FieldType) SetElemWithIsBinaryLit(idx int, element string, isBinaryLit bool) {
	ft.elems[idx] = element
	if isBinaryLit {
		// Create the binary literal flags lazily.
		if ft.elemsIsBinaryLit == nil {
			ft.elemsIsBinaryLit = make([]bool, len(ft.elems))
		}
		ft.elemsIsBinaryLit[idx] = true
	}
}

// GetElem returns the element of the FieldType.
func (ft *FieldType) GetElem(idx int) string {
	return ft.elems[idx]
}

// GetElemIsBinaryLit returns the binary literal flag of the element at index idx.
func (ft *FieldType) GetElemIsBinaryLit(idx int) bool {
	if len(ft.elemsIsBinaryLit) == 0 {
		return false
	}
	return ft.elemsIsBinaryLit[idx]
}

// CleanElemIsBinaryLit cleans the binary literal flag of the element at index idx.
func (ft *FieldType) CleanElemIsBinaryLit() {
	if ft != nil && ft.elemsIsBinaryLit != nil {
		ft.elemsIsBinaryLit = nil
	}
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
	tpEqual := (ft.GetType() == other.GetType()) || (ft.GetType() == mysql.TypeVarchar && other.GetType() == mysql.TypeVarString) || (ft.GetType() == mysql.TypeVarString && other.GetType() == mysql.TypeVarchar)
	flenEqual := ft.flen == other.flen || (ft.EvalType() == ETReal && ft.decimal == UnspecifiedLength) || ft.EvalType() == ETJson
	ignoreDecimal := ft.EvalType() == ETInt || ft.EvalType() == ETString
	partialEqual := tpEqual &&
		(ignoreDecimal || ft.decimal == other.decimal) &&
		ft.charset == other.charset &&
		ft.collate == other.collate &&
		flenEqual &&
		mysql.HasUnsignedFlag(ft.flag) == mysql.HasUnsignedFlag(other.flag)
	if !partialEqual {
		return false
	}
	return slices.Equal(ft.elems, other.elems)
}

// PartialEqual checks whether two FieldType objects are equal.
// If unsafe is true and the objects is string type, PartialEqual will ignore flen.
// See https://github.com/pingcap/tidb/issues/35490#issuecomment-1211658886 for more detail.
func (ft *FieldType) PartialEqual(other *FieldType, unsafe bool) bool {
	if !unsafe || ft.EvalType() != ETString || other.EvalType() != ETString {
		return ft.Equal(other)
	}

	partialEqual := ft.charset == other.charset && ft.collate == other.collate && mysql.HasUnsignedFlag(ft.flag) == mysql.HasUnsignedFlag(other.flag)
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
	switch ft.GetType() {
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
	case mysql.TypeTiDBVectorFloat32:
		return ETVectorFloat32
	case mysql.TypeEnum, mysql.TypeSet:
		if ft.flag&mysql.EnumSetAsIntFlag > 0 {
			return ETInt
		}
	}
	return ETString
}

// Hybrid checks whether a type is a hybrid type, which can represent different types of value in specific context.
func (ft *FieldType) Hybrid() bool {
	return ft.GetType() == mysql.TypeEnum || ft.GetType() == mysql.TypeBit || ft.GetType() == mysql.TypeSet
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
	ts := TypeToStr(ft.GetType(), ft.charset)
	suffix := ""

	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
	isDecimalNotDefault := ft.decimal != defaultDecimal && ft.decimal != 0 && ft.decimal != UnspecifiedLength

	// displayFlen and displayDecimal are flen and decimal values with `-1` substituted with default value.
	displayFlen, displayDecimal := ft.flen, ft.decimal
	if displayFlen == UnspecifiedLength {
		displayFlen = defaultFlen
	}
	if displayDecimal == UnspecifiedLength {
		displayDecimal = defaultDecimal
	}

	switch ft.GetType() {
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
	case mysql.TypeTiny:
		// With display length deprecation active tinyint(1) still has
		// a display length to indicate this might have been a BOOL.
		// Connectors expect this.
		//
		// See also:
		// https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-19.html
		if !TiDBStrictIntegerDisplayWidth || (mysql.HasZerofillFlag(ft.flag) || displayFlen == 1) {
			suffix = fmt.Sprintf("(%d)", displayFlen)
		}
	case mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		// Referring this issue #6688, the integer max display length is deprecated in MySQL 8.0.
		// Since the length doesn't take any effect in TiDB storage or showing result, we remove it here.
		if !TiDBStrictIntegerDisplayWidth || mysql.HasZerofillFlag(ft.flag) {
			suffix = fmt.Sprintf("(%d)", displayFlen)
		}
	case mysql.TypeYear:
		suffix = fmt.Sprintf("(%d)", ft.flen)
	case mysql.TypeTiDBVectorFloat32:
		if ft.flen != UnspecifiedLength {
			suffix = fmt.Sprintf("(%d)", ft.flen)
		}
	case mysql.TypeNull:
		suffix = "(0)"
	}
	return ts + suffix
}

// InfoSchemaStr joins the CompactStr with unsigned flag and
// returns a string.
func (ft *FieldType) InfoSchemaStr() string {
	suffix := ""
	if mysql.HasUnsignedFlag(ft.flag) &&
		ft.GetType() != mysql.TypeBit &&
		ft.GetType() != mysql.TypeYear {
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
	if mysql.HasBinaryFlag(ft.flag) && ft.GetType() != mysql.TypeString {
		strs = append(strs, "BINARY")
	}

	if IsTypeChar(ft.GetType()) || IsTypeBlob(ft.GetType()) {
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
	ctx.WriteKeyWord(TypeToStr(ft.GetType(), ft.charset))

	precision := UnspecifiedLength
	scale := UnspecifiedLength

	switch ft.GetType() {
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

	if IsTypeChar(ft.GetType()) || IsTypeBlob(ft.GetType()) {
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
	case mysql.TypeVarString, mysql.TypeString:
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
			break
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
	case mysql.TypeTiDBVectorFloat32:
		ctx.WriteKeyWord("VECTOR")
	}
	if ft.array {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord("ARRAY")
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
	switch ft.GetType() {
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
	switch ft.GetType() {
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
	Tp               byte
	Flag             uint
	Flen             int
	Decimal          int
	Charset          string
	Collate          string
	Elems            []string
	ElemsIsBinaryLit []bool
	Array            bool
}

// UnmarshalJSON implements the json.Unmarshaler interface.
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
		ft.elemsIsBinaryLit = r.ElemsIsBinaryLit
		ft.array = r.Array
	}
	return err
}

// MarshalJSON marshals the FieldType to JSON.
func (ft *FieldType) MarshalJSON() ([]byte, error) {
	var r jsonFieldType
	r.Tp = ft.tp
	r.Flag = ft.flag
	r.Flen = ft.flen
	r.Decimal = ft.decimal
	r.Charset = ft.charset
	r.Collate = ft.collate
	r.Elems = ft.elems
	r.ElemsIsBinaryLit = ft.elemsIsBinaryLit
	r.Array = ft.array
	return json.Marshal(r)
}

const emptyFieldTypeSize = int64(unsafe.Sizeof(FieldType{}))

// MemoryUsage return the memory usage of FieldType
func (ft *FieldType) MemoryUsage() (sum int64) {
	if ft == nil {
		return
	}
	sum = emptyFieldTypeSize + int64(len(ft.charset)+len(ft.collate)) + int64(cap(ft.elems))*int64(unsafe.Sizeof(*new(string))) +
		int64(cap(ft.elemsIsBinaryLit))*int64(unsafe.Sizeof(*new(bool)))

	for _, s := range ft.elems {
		sum += int64(len(s))
	}
	return
}

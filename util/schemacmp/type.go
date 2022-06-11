// Copyright 2022 PingCAP, Inc.
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

package schemacmp

import (
	"math/bits"
	"strings"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

const (
	flagMaskKeys   = mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag
	flagMaskDefVal = mysql.AutoIncrementFlag | mysql.NoDefaultValueFlag

	notPartOfKeys = ^byte(0)
)

// Please ensure this list is synchronized with the order of Tuple{} in encodeFieldTypeToLattice().
const (
	fieldTypeTupleIndexTp = iota
	fieldTypeTupleIndexFlen
	fieldTypeTupleIndexDec
	fieldTypeTupleIndexFlagSingleton
	fieldTypeTupleIndexFlagNull
	fieldTypeTupleIndexFlagAntiKeys
	fieldTypeTupleIndexFlagDefVal
	fieldTypeTupleIndexCharset
	fieldTypeTupleIndexCollate
	fieldTypeTupleIndexElems

	ErrMsgAutoTypeWithoutKey = "auto type but not defined as a key"
)

func encodeAntiKeys(flag uint) byte {
	// this ensure we get this order:
	//  1. "not part of keys" (flag = 0) is the maximum
	//  2. multiple keys (8) > unique key (4) > primary key (2).
	return ^bits.Reverse8(byte(flag & flagMaskKeys))
}

func decodeAntiKeys(encoded byte) uint {
	return uint(bits.Reverse8(^encoded))
}

// encodeTypeTpAsLattice
func encodeFieldTypeToLattice(ft *types.FieldType) Tuple {
	var flen, dec Lattice
	if ft.GetType() == mysql.TypeNewDecimal {
		flen = Singleton(ft.GetFlen())
		dec = Singleton(ft.GetDecimal())
	} else {
		flen = Int(ft.GetFlen())
		dec = Int(ft.GetDecimal())
	}

	var defVal Lattice
	if mysql.HasAutoIncrementFlag(ft.GetFlag()) || !mysql.HasNoDefaultValueFlag(ft.GetFlag()) {
		defVal = Maybe(Singleton(ft.GetFlag() & flagMaskDefVal))
	} else {
		defVal = Maybe(nil)
	}

	return Tuple{
		FieldTp(ft.GetType()),
		flen,
		dec,

		// TODO: recognize if the remaining flags can be merged or not.
		Singleton(ft.GetFlag() &^ (flagMaskDefVal | mysql.NotNullFlag | flagMaskKeys)),
		Bool(!mysql.HasNotNullFlag(ft.GetFlag())),
		Byte(encodeAntiKeys(ft.GetFlag())),
		defVal,

		Singleton(ft.GetCharset()),
		Singleton(ft.GetCollate()),
		StringList(ft.GetElems()),
	}
}

func decodeFieldTypeFromLattice(tup Tuple) *types.FieldType {
	lst := tup.Unwrap().([]interface{})

	flags := lst[fieldTypeTupleIndexFlagSingleton].(uint)
	flags |= decodeAntiKeys(lst[fieldTypeTupleIndexFlagAntiKeys].(byte))
	if !lst[fieldTypeTupleIndexFlagNull].(bool) {
		flags |= mysql.NotNullFlag
	}
	if x, ok := lst[fieldTypeTupleIndexFlagDefVal].(uint); ok {
		flags |= x
	} else {
		flags |= mysql.NoDefaultValueFlag
	}

	return types.NewFieldTypeBuilder().SetType(lst[fieldTypeTupleIndexTp].(byte)).SetFlen(lst[fieldTypeTupleIndexFlen].(int)).SetDecimal(lst[fieldTypeTupleIndexDec].(int)).SetFlag(flags).SetCharset(lst[fieldTypeTupleIndexCharset].(string)).SetCollate(lst[fieldTypeTupleIndexCollate].(string)).SetElems(lst[fieldTypeTupleIndexElems].([]string)).BuildP()
}

type typ struct{ Tuple }

// Type is to create type.
func Type(ft *types.FieldType) typ {
	return typ{Tuple: encodeFieldTypeToLattice(ft)}
}

func (a typ) hasDefault() bool {
	return a.Tuple[fieldTypeTupleIndexFlagDefVal].Unwrap() != nil
}

// setFlagForMissingColumn adjusts the flags of the type for filling in a
// missing column. Returns whether the column had no default values.
// If the column is AUTO_INCREMENT, returns an incompatible error, because such
// column cannot be part of any keys in the joined table which is invalid.
func (a typ) setFlagForMissingColumn() (hadNoDefault bool) {
	a.Tuple[fieldTypeTupleIndexFlagAntiKeys] = Byte(notPartOfKeys)
	defVal, ok := a.Tuple[fieldTypeTupleIndexFlagDefVal].Unwrap().(uint)
	if !ok || mysql.HasNoDefaultValueFlag(defVal) {
		a.Tuple[fieldTypeTupleIndexFlagDefVal] = Maybe(Singleton(defVal &^ mysql.NoDefaultValueFlag))
		return true
	}
	return false
}

func (a typ) isNotNull() bool {
	return !a.Tuple[fieldTypeTupleIndexFlagNull].Unwrap().(bool)
}

func (a typ) inAutoIncrement() bool {
	defVal, ok := a.Tuple[fieldTypeTupleIndexFlagDefVal].Unwrap().(uint)
	return ok && mysql.HasAutoIncrementFlag(defVal)
}

func (a typ) setAntiKeyFlags(flag uint) {
	a.Tuple[fieldTypeTupleIndexFlagAntiKeys] = Byte(encodeAntiKeys(flag))
}

func (a typ) getStandardDefaultValue() interface{} {
	var tail string
	if dec := a.Tuple[fieldTypeTupleIndexDec].Unwrap().(int); dec > 0 {
		tail = "." + strings.Repeat("0", dec)
	}

	switch a.Tuple[fieldTypeTupleIndexTp].Unwrap().(byte) {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return "0"
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return "0000-00-00 00:00:00" + tail
	case mysql.TypeDate:
		return "0000-00-00"
	case mysql.TypeDuration:
		return "00:00:00" + tail
	case mysql.TypeYear:
		return "0000"
	case mysql.TypeJSON:
		return "null"
	case mysql.TypeEnum:
		return a.Tuple[fieldTypeTupleIndexElems].(StringList)[0]
	case mysql.TypeString:
		// ref https://github.com/pingcap/tidb/blob/66948b2fd9bec8ea11644770a2fa746c7eba1a1f/ddl/ddl_api.go#L3916
		if a.Tuple[fieldTypeTupleIndexCollate].Unwrap().(string) == charset.CollationBin {
			return string(make([]byte, a.Tuple[fieldTypeTupleIndexFlen].Unwrap().(int)))
		}
		return ""
	default:
		return ""
	}
}

func (a typ) clone() typ {
	return typ{Tuple: append(make(Tuple, 0, len(a.Tuple)), a.Tuple...)}
}

func (a typ) Unwrap() interface{} {
	return decodeFieldTypeFromLattice(a.Tuple)
}

func (a typ) Compare(other Lattice) (int, error) {
	if b, ok := other.(typ); ok {
		return a.Tuple.Compare(b.Tuple)
	}
	return 0, typeMismatchError(a, other)
}

func (a typ) Join(other Lattice) (Lattice, error) {
	if b, ok := other.(typ); ok {
		genJoin, err := a.Tuple.Join(b.Tuple)
		if err != nil {
			return nil, err
		}
		join := genJoin.(Tuple)

		// Special check: we can't have an AUTO_INCREMENT column without being a KEY.
		x, ok := join[fieldTypeTupleIndexFlagDefVal].Unwrap().(uint)
		if ok && x&mysql.AutoIncrementFlag != 0 && join[fieldTypeTupleIndexFlagAntiKeys].Unwrap() == notPartOfKeys {
			return nil, &IncompatibleError{Msg: ErrMsgAutoTypeWithoutKey}
		}

		return typ{Tuple: join}, nil
	}
	return nil, typeMismatchError(a, other)
}

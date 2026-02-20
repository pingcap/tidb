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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ast "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// SetBinChsClnFlag sets charset, collation as 'binary' and adds binaryFlag to FieldType.
func SetBinChsClnFlag(ft *FieldType) {
	ft.SetCharset(charset.CharsetBin)
	ft.SetCollate(charset.CollationBin)
	ft.AddFlag(mysql.BinaryFlag)
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
	if origin.GetType() == to.GetType() {
		if origin.GetType() == mysql.TypeEnum || origin.GetType() == mysql.TypeSet {
			typeVar := "set"
			if origin.GetType() == mysql.TypeEnum {
				typeVar = "enum"
			}
			if len(to.GetElems()) < len(origin.GetElems()) {
				msg := fmt.Sprintf("the number of %s column's elements is less than the original: %d", typeVar, len(origin.GetElems()))
				return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
			}
			for index, originElem := range origin.GetElems() {
				toElem := to.GetElems()[index]
				if originElem != toElem {
					msg := fmt.Sprintf("cannot modify %s column value %s to %s", typeVar, originElem, toElem)
					return true, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
				}
			}
		}

		if origin.GetType() == mysql.TypeNewDecimal {
			// Floating-point and fixed-point types also can be UNSIGNED. As with integer types, this attribute prevents
			// negative values from being stored in the column. Unlike the integer types, the upper range of column values
			// remains the same.
			if to.GetFlen() != origin.GetFlen() || to.GetDecimal() != origin.GetDecimal() || mysql.HasUnsignedFlag(to.GetFlag()) != mysql.HasUnsignedFlag(origin.GetFlag()) {
				msg := fmt.Sprintf("decimal change from decimal(%d, %d) to decimal(%d, %d)", origin.GetFlen(), origin.GetDecimal(), to.GetFlen(), to.GetDecimal())
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
	stringToString := IsString(origin.GetType()) && IsString(to.GetType())
	integerToInteger := mysql.IsIntegerType(origin.GetType()) && mysql.IsIntegerType(to.GetType())
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
	toFlen := to.GetFlen()
	originFlen := origin.GetFlen()
	if mysql.IsIntegerType(to.GetType()) && mysql.IsIntegerType(origin.GetType()) {
		// For integers, we should ignore the potential display length represented by flen, using
		// the default flen of the type.
		originFlen, _ = mysql.GetDefaultFieldLengthAndDecimal(origin.GetType())
		toFlen, _ = mysql.GetDefaultFieldLengthAndDecimal(to.GetType())
	}

	if ConvertBetweenCharAndVarchar(origin.GetType(), to.GetType()) {
		return true, "conversion between char and varchar string needs reorganization"
	}

	if toFlen > 0 && toFlen != originFlen {
		if toFlen < originFlen {
			return true, fmt.Sprintf("length %d is less than origin %d", toFlen, originFlen)
		}

		// Due to the behavior of padding \x00 at binary type, we need to reorg when binary length changed
		isBinaryType := func(tp *FieldType) bool { return tp.GetType() == mysql.TypeString && IsBinaryStr(tp) }
		if isBinaryType(origin) && isBinaryType(to) {
			return true, "can't change binary types of different length"
		}
	}
	if to.GetDecimal() > 0 && to.GetDecimal() < origin.GetDecimal() {
		return true, fmt.Sprintf("decimal %d is less than origin %d", to.GetDecimal(), origin.GetDecimal())
	}
	if mysql.HasUnsignedFlag(origin.GetFlag()) != mysql.HasUnsignedFlag(to.GetFlag()) {
		return true, "can't change unsigned integer to signed or vice versa"
	}
	return false, ""
}

func checkTypeChangeSupported(origin *FieldType, to *FieldType) bool {
	if (IsTypeTime(origin.GetType()) || origin.GetType() == mysql.TypeDuration || origin.GetType() == mysql.TypeYear ||
		IsString(origin.GetType()) || origin.GetType() == mysql.TypeJSON) &&
		to.GetType() == mysql.TypeBit {
		// TODO: Currently date/datetime/timestamp/time/year/string/json data type cast to bit are not compatible with mysql, should fix here after compatible.
		return false
	}

	if (IsTypeTime(origin.GetType()) || origin.GetType() == mysql.TypeDuration || origin.GetType() == mysql.TypeYear ||
		origin.GetType() == mysql.TypeNewDecimal || origin.GetType() == mysql.TypeFloat || origin.GetType() == mysql.TypeDouble || origin.GetType() == mysql.TypeJSON || origin.GetType() == mysql.TypeBit) &&
		(to.GetType() == mysql.TypeEnum || to.GetType() == mysql.TypeSet) {
		// TODO: Currently date/datetime/timestamp/time/year/decimal/float/double/json/bit cast to enum/set are not support yet, should fix here after supported.
		return false
	}

	if (origin.GetType() == mysql.TypeEnum || origin.GetType() == mysql.TypeSet || origin.GetType() == mysql.TypeBit ||
		origin.GetType() == mysql.TypeNewDecimal || origin.GetType() == mysql.TypeFloat || origin.GetType() == mysql.TypeDouble) &&
		(IsTypeTime(to.GetType())) {
		// TODO: Currently enum/set/bit/decimal/float/double cast to date/datetime/timestamp type are not support yet, should fix here after supported.
		return false
	}

	if origin.GetType() == mysql.TypeTiDBVectorFloat32 || to.GetType() == mysql.TypeTiDBVectorFloat32 {
		// TODO: Vector type not supported.
		return false
	}

	if (origin.GetType() == mysql.TypeEnum || origin.GetType() == mysql.TypeSet || origin.GetType() == mysql.TypeBit) &&
		to.GetType() == mysql.TypeDuration {
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

// IsVarcharTooBigFieldLength check if the varchar type column exceeds the maximum length limit.
func IsVarcharTooBigFieldLength(colDefTpFlen int, colDefName, setCharset string) error {
	desc, err := charset.GetCharsetInfo(setCharset)
	if err != nil {
		return errors.Trace(err)
	}
	maxFlen := mysql.MaxFieldVarCharLength
	maxFlen /= desc.Maxlen
	if colDefTpFlen != UnspecifiedLength && colDefTpFlen > maxFlen {
		return ErrTooBigFieldLength.GenWithStack("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead", colDefName, maxFlen)
	}
	return nil
}

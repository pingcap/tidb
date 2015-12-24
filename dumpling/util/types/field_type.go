// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
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

// CompactStr only considers Tp/CharsetBin/Flen/Deimal.
// This is used for showing column type in infoschema.
func (ft *FieldType) CompactStr() string {
	ts := TypeToStr(ft.Tp, ft.Charset)
	suffix := ""
	switch ft.Tp {
	case mysql.TypeEnum, mysql.TypeSet:
		// Format is ENUM ('e1', 'e2') or SET ('e1', 'e2')
		es := make([]string, 0, len(ft.Elems))
		for _, e := range ft.Elems {
			e = strings.Replace(e, "'", "''", -1)
			es = append(es, e)
		}
		suffix = fmt.Sprintf("('%s')", strings.Join(es, "','"))
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate:
		if ft.Decimal != UnspecifiedLength && ft.Decimal != 0 {
			suffix = fmt.Sprintf("(%d)", ft.Decimal)
		}
	default:
		if ft.Flen != UnspecifiedLength {
			if ft.Decimal == UnspecifiedLength {
				if ft.Tp != mysql.TypeFloat && ft.Tp != mysql.TypeDouble {
					suffix = fmt.Sprintf("(%d)", ft.Flen)
				}
			} else {
				suffix = fmt.Sprintf("(%d,%d)", ft.Flen, ft.Decimal)
			}
		} else if ft.Decimal != UnspecifiedLength {
			suffix = fmt.Sprintf("(%d)", ft.Decimal)
		}
	}
	return ts + suffix
}

// String joins the information of FieldType and
// returns a string.
func (ft *FieldType) String() string {
	strs := []string{ft.CompactStr()}
	if mysql.HasUnsignedFlag(ft.Flag) {
		strs = append(strs, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		strs = append(strs, "ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.Flag) {
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

// DataItem is wrapped data with type info.
type DataItem struct {
	Type *FieldType
	Data interface{}
}

// String implements Stringer interface.
func (di *DataItem) String() string {
	return fmt.Sprintf("%s", di.Data)
}

// RawData returns the raw data for DataItem.
func RawData(d interface{}) interface{} {
	v, ok := d.(*DataItem)
	if ok {
		return v.Data
	}
	return d
}

// IsNil checks if the raw data is nil.
func IsNil(d interface{}) bool {
	return RawData(d) == nil
}

// IsDataItem checks if the interface is a DataItem.
func IsDataItem(d interface{}) bool {
	_, ok := d.(*DataItem)
	return ok
}

// DefaultTypeForValue returns the default FieldType for the value.
func DefaultTypeForValue(value interface{}) *FieldType {
	switch x := value.(type) {
	case nil:
		return NewFieldType(mysql.TypeNull)
	case bool, int64, int:
		return NewFieldType(mysql.TypeLonglong)
	case uint64:
		tp := NewFieldType(mysql.TypeLonglong)
		tp.Flag |= mysql.UnsignedFlag
		return tp
	case string:
		tp := NewFieldType(mysql.TypeVarchar)
		tp.Charset = mysql.DefaultCharset
		tp.Collate = mysql.DefaultCollationName
		return tp
	case float64:
		return NewFieldType(mysql.TypeNewDecimal)
	case []byte:
		tp := NewFieldType(mysql.TypeBlob)
		tp.Charset = charset.CharsetBin
		tp.Collate = charset.CharsetBin
		return tp
	case mysql.Bit:
		return NewFieldType(mysql.TypeBit)
	case mysql.Hex:
		tp := NewFieldType(mysql.TypeVarchar)
		tp.Charset = charset.CharsetBin
		tp.Collate = charset.CharsetBin
		return tp
	case mysql.Time:
		return NewFieldType(x.Type)
	case mysql.Duration:
		return NewFieldType(mysql.TypeDuration)
	case mysql.Decimal:
		return NewFieldType(mysql.TypeNewDecimal)
	case mysql.Enum:
		return NewFieldType(mysql.TypeEnum)
	case mysql.Set:
		return NewFieldType(mysql.TypeSet)
	case *DataItem:
		return x.Type
	}
	log.Errorf("Unknown value type %T for default field type.", value)
	return nil
}

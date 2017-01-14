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
	"io"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
)

// IsTypeBlob returns a boolean indicating whether the tp is a blob type.
func IsTypeBlob(tp byte) bool {
	switch tp {
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return true
	default:
		return false
	}
}

// IsTypeChar returns a boolean indicating
// whether the tp is the char type like a string type or a varchar type.
func IsTypeChar(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeVarchar:
		return true
	default:
		return false
	}
}

// IsTypePrefixable returns a boolean indicating
// whether an index on a column with the tp can be defined with a prefix.
func IsTypePrefixable(tp byte) bool {
	return IsTypeBlob(tp) || IsTypeChar(tp)
}

// IsTypeFractionable returns a boolean indicating
// whether the tp can has time fraction.
func IsTypeFractionable(tp byte) bool {
	return tp == mysql.TypeDatetime || tp == mysql.TypeDuration || tp == mysql.TypeTimestamp
}

var type2Str = map[byte]string{
	mysql.TypeBit:        "bit",
	mysql.TypeBlob:       "text",
	mysql.TypeDate:       "date",
	mysql.TypeDatetime:   "datetime",
	mysql.TypeDecimal:    "decimal",
	mysql.TypeNewDecimal: "decimal",
	mysql.TypeDouble:     "double",
	mysql.TypeEnum:       "enum",
	mysql.TypeFloat:      "float",
	mysql.TypeGeometry:   "geometry",
	mysql.TypeInt24:      "mediumint",
	mysql.TypeLong:       "int",
	mysql.TypeLonglong:   "bigint",
	mysql.TypeLongBlob:   "longtext",
	mysql.TypeMediumBlob: "mediumtext",
	mysql.TypeNull:       "null",
	mysql.TypeSet:        "set",
	mysql.TypeShort:      "smallint",
	mysql.TypeString:     "char",
	mysql.TypeDuration:   "time",
	mysql.TypeTimestamp:  "timestamp",
	mysql.TypeTiny:       "tinyint",
	mysql.TypeTinyBlob:   "tinytext",
	mysql.TypeVarchar:    "varchar",
	mysql.TypeVarString:  "var_string",
	mysql.TypeYear:       "year",
}

// TypeStr converts tp to a string.
func TypeStr(tp byte) (r string) {
	return type2Str[tp]
}

// TypeToStr converts a field to a string.
// It is used for converting Text to Blob,
// or converting Char to Binary.
// Args:
//	tp: type enum
//	cs: charset
func TypeToStr(tp byte, cs string) (r string) {
	ts := type2Str[tp]
	if cs != charset.CharsetBin {
		return ts
	}
	if IsTypeBlob(tp) {
		ts = strings.Replace(ts, "text", "blob", 1)
	} else if IsTypeChar(tp) {
		ts = strings.Replace(ts, "char", "binary", 1)
	}
	return ts
}

// EOFAsNil filtrates errors,
// If err is equal to io.EOF returns nil.
func EOFAsNil(err error) error {
	if terror.ErrorEqual(err, io.EOF) {
		return nil
	}
	return errors.Trace(err)
}

// InvOp2 returns an invalid operation error.
func InvOp2(x, y interface{}, o opcode.Op) (interface{}, error) {
	return nil, errors.Errorf("Invalid operation: %v %v %v (mismatched types %T and %T)", x, o, y, x, y)
}

// Overflow returns an overflowed error.
func overflow(v interface{}, tp byte) error {
	return errors.Errorf("constant %v overflows %s", v, TypeStr(tp))
}

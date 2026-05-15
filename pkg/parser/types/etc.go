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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
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
	return tp == mysql.TypeString || tp == mysql.TypeVarchar
}

// IsTypeVector returns whether tp is a vector type.
func IsTypeVector(tp byte) bool {
	return tp == mysql.TypeTiDBVectorFloat32
}

var type2Str = map[byte]string{
	mysql.TypeBit:               "bit",
	mysql.TypeBlob:              "text",
	mysql.TypeDate:              "date",
	mysql.TypeDatetime:          "datetime",
	mysql.TypeUnspecified:       "unspecified",
	mysql.TypeNewDecimal:        "decimal",
	mysql.TypeDouble:            "double",
	mysql.TypeEnum:              "enum",
	mysql.TypeFloat:             "float",
	mysql.TypeGeometry:          "geometry",
	mysql.TypeTiDBVectorFloat32: "vector",
	mysql.TypeInt24:             "mediumint",
	mysql.TypeJSON:              "json",
	mysql.TypeLong:              "int",
	mysql.TypeLonglong:          "bigint",
	mysql.TypeLongBlob:          "longtext",
	mysql.TypeMediumBlob:        "mediumtext",
	mysql.TypeNull:              "null",
	mysql.TypeSet:               "set",
	mysql.TypeShort:             "smallint",
	mysql.TypeString:            "char",
	mysql.TypeDuration:          "time",
	mysql.TypeTimestamp:         "timestamp",
	mysql.TypeTiny:              "tinyint",
	mysql.TypeTinyBlob:          "tinytext",
	mysql.TypeVarchar:           "varchar",
	mysql.TypeVarString:         "var_string",
	mysql.TypeYear:              "year",
}

var str2Type = map[string]byte{
	"bit":         mysql.TypeBit,
	"text":        mysql.TypeBlob,
	"date":        mysql.TypeDate,
	"datetime":    mysql.TypeDatetime,
	"unspecified": mysql.TypeUnspecified,
	"decimal":     mysql.TypeNewDecimal,
	"double":      mysql.TypeDouble,
	"enum":        mysql.TypeEnum,
	"float":       mysql.TypeFloat,
	"geometry":    mysql.TypeGeometry,
	"vector":      mysql.TypeTiDBVectorFloat32,
	"mediumint":   mysql.TypeInt24,
	"json":        mysql.TypeJSON,
	"int":         mysql.TypeLong,
	"bigint":      mysql.TypeLonglong,
	"longtext":    mysql.TypeLongBlob,
	"mediumtext":  mysql.TypeMediumBlob,
	"null":        mysql.TypeNull,
	"set":         mysql.TypeSet,
	"smallint":    mysql.TypeShort,
	"char":        mysql.TypeString,
	"time":        mysql.TypeDuration,
	"timestamp":   mysql.TypeTimestamp,
	"tinyint":     mysql.TypeTiny,
	"tinytext":    mysql.TypeTinyBlob,
	"varchar":     mysql.TypeVarchar,
	"var_string":  mysql.TypeVarString,
	"year":        mysql.TypeYear,
}

// TypeStr converts tp to a string.
func TypeStr(tp byte) (r string) {
	return type2Str[tp]
}

// TypeToStr converts a field to a string.
// It is used for converting Text to Blob,
// or converting Char to Binary.
// Args:
//
//	tp: type enum
//	cs: charset
func TypeToStr(tp byte, cs string) (r string) {
	ts := type2Str[tp]
	if cs != "binary" {
		return ts
	}
	if IsTypeBlob(tp) {
		ts = strings.Replace(ts, "text", "blob", 1)
	} else if IsTypeChar(tp) {
		ts = strings.Replace(ts, "char", "binary", 1)
	} else if tp == mysql.TypeNull {
		ts = "binary"
	}
	return ts
}

// StrToType convert a string to type enum.
// Args:
//
//	ts: type string
func StrToType(ts string) (tp byte) {
	ts = strings.Replace(ts, "blob", "text", 1)
	ts = strings.Replace(ts, "binary", "char", 1)
	if tp, ok := str2Type[ts]; ok {
		return tp
	}

	return mysql.TypeUnspecified
}

var (
	dig2bytes = [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
)

// constant values.
const (
	digitsPerWord = 9 // A word holds 9 digits.
	wordSize      = 4 // A word is 4 bytes int32.
)

var (
	// ErrInvalidDefault is returned when meet a invalid default value.
	ErrInvalidDefault = terror.ClassTypes.NewStd(mysql.ErrInvalidDefault)
	// ErrDataOutOfRange is returned when meet a value out of range.
	ErrDataOutOfRange = terror.ClassTypes.NewStd(mysql.ErrDataOutOfRange)
	// ErrTruncatedWrongValue is returned when meet a value bigger than
	// 99999999999999999999999999999999999999999999999999999999999999999 during parsing.
	ErrTruncatedWrongValue = terror.ClassTypes.NewStd(mysql.ErrTruncatedWrongValue)
	// ErrIllegalValueForType is returned when strconv.ParseFloat meet strconv.ErrRange during parsing.
	ErrIllegalValueForType = terror.ClassTypes.NewStd(mysql.ErrIllegalValueForType)
)

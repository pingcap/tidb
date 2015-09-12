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
	"io"
	"reflect"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/errors2"
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

var type2Str = map[byte]string{
	mysql.TypeBit:        "BIT",
	mysql.TypeBlob:       "TEXT",
	mysql.TypeDate:       "DATE",
	mysql.TypeDatetime:   "DATETIME",
	mysql.TypeDecimal:    "DECIMAL",
	mysql.TypeNewDecimal: "DECIMAL",
	mysql.TypeDouble:     "DOUBLE",
	mysql.TypeEnum:       "ENUM",
	mysql.TypeFloat:      "FLOAT",
	mysql.TypeGeometry:   "GEOMETRY",
	mysql.TypeInt24:      "MEDIUMINT",
	mysql.TypeLong:       "INT",
	mysql.TypeLonglong:   "BIGINT",
	mysql.TypeLongBlob:   "LONGTEXT",
	mysql.TypeMediumBlob: "MEDIUMTEXT",
	mysql.TypeNull:       "NULL",
	mysql.TypeSet:        "SET",
	mysql.TypeShort:      "SMALLINT",
	mysql.TypeString:     "CHAR",
	mysql.TypeDuration:   "TIME",
	mysql.TypeTimestamp:  "TIMESTAMP",
	mysql.TypeTiny:       "TINYINT",
	mysql.TypeTinyBlob:   "TINYTEXT",
	mysql.TypeVarchar:    "VARCHAR",
	mysql.TypeVarString:  "VAR_STRING",
	mysql.TypeYear:       "YEAR",
}

// TypeStr converts tp to a string.
func TypeStr(tp byte) (r string) {
	return type2Str[tp]
}

// TypeToStr converts tp to a string with an extra binary.
func TypeToStr(tp byte, binary bool) string {
	switch tp {
	case mysql.TypeBlob:
		if binary {
			return "text"
		}
		return "blob"
	case mysql.TypeLongBlob:
		if binary {
			return "longtext"
		}
		return "longblob"
	case mysql.TypeTinyBlob:
		if binary {
			return "tinytext"
		}
		return "tinyblob"
	case mysql.TypeMediumBlob:
		if binary {
			return "mediumtext"
		}
		return "mediumblob"
	case mysql.TypeVarchar:
		if binary {
			return "varbinary"
		}
		return "varchar"
	case mysql.TypeString:
		if binary {
			return "binary"
		}
		return "char"
	case mysql.TypeTiny:
		return "tinyint"
	case mysql.TypeShort:
		return "smallint"
	case mysql.TypeInt24:
		return "mediumint"
	case mysql.TypeLong:
		return "int"
	case mysql.TypeLonglong:
		return "bigint"
	case mysql.TypeFloat:
		return "float"
	case mysql.TypeDouble:
		return "double"
	case mysql.TypeDecimal, mysql.TypeNewDecimal:
		return "decimal"
	case mysql.TypeYear:
		return "year"
	case mysql.TypeDuration:
		return "time"
	case mysql.TypeDatetime:
		return "datetime"
	case mysql.TypeDate:
		return "date"
	case mysql.TypeTimestamp:
		return "timestamp"
	case mysql.TypeBit:
		return "bit"
	default:
		log.Errorf("unkown type %d, binary %v", tp, binary)
	}
	return ""
}

// FieldTypeToStr converts a field to a string.
// It is used for converting Text to Blob,
// or converting Char to Binary.
// Args:
//	tp: type enum
//	cs: charset
func FieldTypeToStr(tp byte, cs string) (r string) {
	ts := type2Str[tp]
	if cs != charset.CharsetBin {
		return ts
	}
	if IsTypeBlob(tp) {
		ts = strings.Replace(ts, "TEXT", "BLOB", 1)
	} else if IsTypeChar(tp) {
		ts = strings.Replace(ts, "CHAR", "BINARY", 1)
	}
	return ts
}

// EOFAsNil filtrates errors,
// If err is equal to io.EOF returns nil.
func EOFAsNil(err error) error {
	if errors2.ErrorEqual(err, io.EOF) {
		return nil
	}
	return err
}

// InvOp2 returns an invalid operation error.
func InvOp2(x, y interface{}, o opcode.Op) (interface{}, error) {
	return nil, errors.Errorf("Invalid operation: %v %v %v (mismatched types %T and %T)", x, o, y, x, y)
}

// UndOp returns an undefined error.
func UndOp(x interface{}, o opcode.Op) (interface{}, error) {
	return nil, errors.Errorf("Invalid operation: %v%v (operator %v not defined on %T)", o, x, o, x)
}

// Overflow returns an overflowed error.
func overflow(v interface{}, tp byte) error {
	return errors.Errorf("constant %v overflows %s", v, TypeStr(tp))
}

func compareUint64With(x uint64, b interface{}) int {
	switch y := b.(type) {
	case nil:
		return 1
	case uint8:
		return CompareUint64(uint64(x), uint64(y))
	case uint16:
		return CompareUint64(uint64(x), uint64(y))
	case uint32:
		return CompareUint64(uint64(x), uint64(y))
	case uint64:
		return CompareUint64(uint64(x), uint64(y))
	case uint:
		return CompareUint64(uint64(x), uint64(y))
	default:
		panic("should never happen")
	}
}

func compareInt64With(x int64, b interface{}) int {
	switch y := b.(type) {
	case nil:
		return 1
	case int8:
		return CompareInt64(int64(x), int64(y))
	case int16:
		return CompareInt64(int64(x), int64(y))
	case int32:
		return CompareInt64(int64(x), int64(y))
	case int64:
		return CompareInt64(int64(x), int64(y))
	case int:
		return CompareInt64(int64(x), int64(y))
	default:
		panic("should never happen")
	}
}

func compareFloat64With(x float64, b interface{}) int {
	switch y := b.(type) {
	case nil:
		return 1
	case float32:
		return CompareFloat64(float64(x), float64(y))
	case float64:
		return CompareFloat64(float64(x), float64(y))
	default:
		panic("should never happen")
	}
}

// TODO: collate should return errors from Compare.
func collate(x, y []interface{}) (r int) {
	nx, ny := len(x), len(y)

	switch {
	case nx == 0 && ny != 0:
		return -1
	case nx == 0 && ny == 0:
		return 0
	case nx != 0 && ny == 0:
		return 1
	}

	r = 1
	if nx > ny {
		x, y, r = y, x, -r
	}

	for i, xi := range x {
		// TODO: we may remove collate later, so here just panic error.
		c, err := Compare(xi, y[i])
		if err != nil {
			panic(fmt.Sprintf("should never happend %v", err))
		}

		if c != 0 {
			return c * r
		}
	}

	if nx == ny {
		return 0
	}

	return -r
}

// Collators maps a boolean value to a collated function.
var Collators = map[bool]func(a, b []interface{}) int{false: collateDesc, true: collate}

func collateDesc(a, b []interface{}) int {
	return -collate(a, b)
}

// IsOrderedType returns a boolean
// whether the type of y can be used by order by.
func IsOrderedType(v interface{}) (y interface{}, r bool, err error) {
	switch x := v.(type) {
	case float32, float64,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		string, mysql.Decimal:
		return v, true, nil
	case mysql.Time, mysql.Duration:
		return x, true, nil
	}

	return v, false, nil
}

// Clone copies a interface to another interface.
// It does a deep copy.
func Clone(from interface{}) (interface{}, error) {
	if from == nil {
		return nil, nil
	}
	switch x := from.(type) {
	case uint8, uint16, uint32, uint64, float32, float64,
		int16, int8, bool, string, int, int64, int32:
		return x, nil
	case []byte:
		target := make([]byte, len(from.([]byte)))
		copy(target, from.([]byte))
		return target, nil
	case []interface{}:
		var r []interface{}
		for _, v := range from.([]interface{}) {
			vv, err := Clone(v)
			if err != nil {
				return nil, err
			}
			r = append(r, vv)
		}
		return r, nil
	case mysql.Time:
		return x, nil
	case mysql.Duration:
		return x, nil
	case mysql.Decimal:
		return x, nil
	default:
		log.Error(reflect.TypeOf(from))
		return nil, errors.Errorf("Clone invalid type %T", from)
	}
}

func convergeType(a interface{}, hasDecimal, hasFloat *bool) (x interface{}) {
	x = a
	switch v := a.(type) {
	case bool:
		// treat bool as 1 and 0
		if v {
			x = int64(1)
		} else {
			x = int64(0)
		}
	case int:
		x = int64(v)
	case int8:
		x = int64(v)
	case int16:
		x = int64(v)
	case int32:
		x = int64(v)
	case int64:
		x = int64(v)
	case uint:
		x = uint64(v)
	case uint8:
		x = uint64(v)
	case uint16:
		x = uint64(v)
	case uint32:
		x = uint64(v)
	case uint64:
		x = uint64(v)
	case float32:
		x = float64(v)
		*hasFloat = true
	case float64:
		x = float64(v)
		*hasFloat = true
	case mysql.Decimal:
		x = v
		*hasDecimal = true
	}
	return
}

// Coerce changes type.
// If a or b is Decimal, changes the both to Decimal.
// If a or b is Float, changes the both to Float.
func Coerce(a, b interface{}) (x, y interface{}) {
	var hasDecimal bool
	var hasFloat bool
	x = convergeType(a, &hasDecimal, &hasFloat)
	y = convergeType(b, &hasDecimal, &hasFloat)
	if hasDecimal {
		d, err := mysql.ConvertToDecimal(x)
		if err == nil {
			x = d
		}
		d, err = mysql.ConvertToDecimal(y)
		if err == nil {
			y = d
		}
	} else if hasFloat {
		switch v := x.(type) {
		case int64:
			x = float64(v)
		case uint64:
			x = float64(v)
		}
		switch v := y.(type) {
		case int64:
			y = float64(v)
		case uint64:
			y = float64(v)
		}
	}
	return
}

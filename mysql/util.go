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

package mysql

var defaultLengthAndDecimal = map[byte]struct {
	length  int
	decimal int
}{
	TypeBit:        {1, 0},
	TypeTiny:       {4, 0},
	TypeShort:      {6, 0},
	TypeInt24:      {9, 0},
	TypeLong:       {11, 0},
	TypeLonglong:   {20, 0},
	TypeDouble:     {22, NotFixedDec},
	TypeFloat:      {12, NotFixedDec},
	TypeNewDecimal: {11, 0},
	TypeDate:       {10, 0},
	TypeTimestamp:  {19, 0},
	TypeDatetime:   {19, 0},
	TypeYear:       {4, 0},
	TypeString:     {1, 0},
	TypeVarchar:    {5, 0},
	TypeVarString:  {5, 0},
	TypeTinyBlob:   {255, 0},
	TypeBlob:       {65535, 0},
	TypeMediumBlob: {16777215, 0},
	TypeLongBlob:   {4294967295, 0},
	TypeJSON:       {4294967295, 0},
	TypeNull:       {0, 0},
}

// GetDefaultFieldLengthAndDecimal returns the default display length (flen) and decimal length for column.
// Call this when no Flen assigned in ddl.
// or column value is calculated from an expression.
// For example: "select count(*) from t;", the column type is int64 and Flen in ResultField will be 21.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func GetDefaultFieldLengthAndDecimal(tp byte) (flen int, decimal int) {
	val, ok := defaultLengthAndDecimal[tp]
	if ok {
		return val.length, val.decimal
	}
	return -1, 0
}

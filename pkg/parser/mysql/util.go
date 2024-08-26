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

type lengthAndDecimal struct {
	length  int64
	decimal int
}

// defaultLengthAndDecimal provides default flen and decimal for fields
// from CREATE TABLE when they are unspecified.
var defaultLengthAndDecimal = map[byte]lengthAndDecimal{
	TypeBit:        {1, 0},
	TypeTiny:       {4, 0},
	TypeShort:      {6, 0},
	TypeInt24:      {9, 0},
	TypeLong:       {11, 0},
	TypeLonglong:   {20, 0},
	TypeDouble:     {22, -1},
	TypeFloat:      {12, -1},
	TypeNewDecimal: {10, 0},
	TypeDuration:   {10, 0},
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
	TypeSet:        {-1, 0},
	TypeEnum:       {-1, 0},
}

// IsIntegerType indicate whether tp is an integer type.
func IsIntegerType(tp byte) bool {
	switch tp {
	case TypeTiny, TypeShort, TypeInt24, TypeLong, TypeLonglong:
		return true
	}
	return false
}

// GetDefaultFieldLengthAndDecimal returns the default display length (flen) and decimal length for column.
// Call this when no flen assigned in ddl.
// or column value is calculated from an expression.
// For example: "select count(*) from t;", the column type is int64 and flen in ResultField will be 21.
// See https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
func GetDefaultFieldLengthAndDecimal(tp byte) (flen, decimal int) {
	val, ok := defaultLengthAndDecimal[tp]
	if ok {
		return int(val.length), val.decimal
	}
	return -1, -1
}

// defaultLengthAndDecimal provides default flen and decimal for fields
// from CAST when they are unspecified.
var defaultLengthAndDecimalForCast = map[byte]lengthAndDecimal{
	TypeString:     {0, -1}, // flen & decimal differs.
	TypeDate:       {10, 0},
	TypeDatetime:   {19, 0},
	TypeNewDecimal: {10, 0},
	TypeDuration:   {10, 0},
	TypeLonglong:   {22, 0},
	TypeDouble:     {22, -1},
	TypeFloat:      {12, -1},
	TypeJSON:       {4194304, 0}, // flen differs.
}

// GetDefaultFieldLengthAndDecimalForCast returns the default display length (flen) and decimal length for casted column
// when flen or decimal is not specified.
func GetDefaultFieldLengthAndDecimalForCast(tp byte) (flen, decimal int) {
	val, ok := defaultLengthAndDecimalForCast[tp]
	if ok {
		return int(val.length), val.decimal
	}
	return -1, -1
}

// IsAuthPluginClearText is used to indicated that the plugin need clear-text password.
func IsAuthPluginClearText(authPlugin string) bool {
	return authPlugin == AuthNativePassword ||
		authPlugin == AuthTiDBSM3Password ||
		authPlugin == AuthCachingSha2Password
}

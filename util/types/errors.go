// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

var (
	// ErrDataTooLong is returned when converts a string value that is longer than field type length.
	ErrDataTooLong = terror.ClassTypes.New(codeDataTooLong, "Data Too Long")
	// ErrIllegalValueForType is returned when value of type is illegal.
	ErrIllegalValueForType = terror.ClassTypes.New(codeIllegalValueForType, mysql.MySQLErrName[mysql.ErrIllegalValueForType])
	// ErrTruncated is returned when data has been truncated during conversion.
	ErrTruncated = terror.ClassTypes.New(codeTruncated, "Data Truncated")
	// ErrTruncatedWrongVal is returned when data has been truncated during conversion.
	ErrTruncatedWrongVal = terror.ClassTypes.New(codeTruncatedWrongValue, msgTruncatedWrongVal)
	// ErrOverflow is returned when data is out of range for a field type.
	ErrOverflow = terror.ClassTypes.New(codeOverflow, msgOverflow)
	// ErrDivByZero is return when do division by 0.
	ErrDivByZero = terror.ClassTypes.New(codeDivByZero, "Division by 0")
	// ErrTooBigDisplayWidth is return when display width out of range for column.
	ErrTooBigDisplayWidth = terror.ClassTypes.New(codeTooBigDisplayWidth, "Too Big Display width")
	// ErrTooBigFieldLength is return when column length too big for column.
	ErrTooBigFieldLength = terror.ClassTypes.New(codeTooBigFieldLength, "Too Big Field length")
	// ErrTooBigSet is return when too many strings for column.
	ErrTooBigSet = terror.ClassTypes.New(codeTooBigSet, "Too Big Set")
	// ErrWrongFieldSpec is return when incorrect column specifier for column.
	ErrWrongFieldSpec = terror.ClassTypes.New(codeWrongFieldSpec, "Wrong Field Spec")
	// ErrBadNumber is return when parsing an invalid binary decimal number.
	ErrBadNumber = terror.ClassTypes.New(codeBadNumber, "Bad Number")
	// ErrCastAsSignedOverflow is returned when positive out-of-range integer, and convert to it's negative complement.
	ErrCastAsSignedOverflow = terror.ClassTypes.New(codeUnknown, msgCastAsSignedOverflow)
	// ErrCastNegIntAsUnsigned is returned when a negative integer be casted to an unsigned int.
	ErrCastNegIntAsUnsigned = terror.ClassTypes.New(codeUnknown, msgCastNegIntAsUnsigned)
	// ErrInvalidDefault is returned when meet a invalid default value.
	ErrInvalidDefault = terror.ClassTypes.New(codeInvalidDefault, "Invalid default value for '%s'")
	// ErrMBiggerThanD is returned when precision less than the scale.
	ErrMBiggerThanD = terror.ClassTypes.New(codeMBiggerThanD, mysql.MySQLErrName[mysql.ErrMBiggerThanD])
)

const (
	codeBadNumber terror.ErrCode = 1

	codeDataTooLong         terror.ErrCode = terror.ErrCode(mysql.ErrDataTooLong)
	codeIllegalValueForType terror.ErrCode = terror.ErrCode(mysql.ErrIllegalValueForType)
	codeTruncated           terror.ErrCode = terror.ErrCode(mysql.WarnDataTruncated)
	codeOverflow            terror.ErrCode = terror.ErrCode(mysql.ErrDataOutOfRange)
	codeDivByZero           terror.ErrCode = terror.ErrCode(mysql.ErrDivisionByZero)
	codeTooBigDisplayWidth  terror.ErrCode = terror.ErrCode(mysql.ErrTooBigDisplaywidth)
	codeTooBigFieldLength   terror.ErrCode = terror.ErrCode(mysql.ErrTooBigFieldlength)
	codeTooBigSet           terror.ErrCode = terror.ErrCode(mysql.ErrTooBigSet)
	codeWrongFieldSpec      terror.ErrCode = terror.ErrCode(mysql.ErrWrongFieldSpec)
	codeTruncatedWrongValue terror.ErrCode = terror.ErrCode(mysql.ErrTruncatedWrongValue)
	codeUnknown             terror.ErrCode = terror.ErrCode(mysql.ErrUnknown)
	codeInvalidDefault      terror.ErrCode = terror.ErrCode(mysql.ErrInvalidDefault)
	codeMBiggerThanD        terror.ErrCode = terror.ErrCode(mysql.ErrMBiggerThanD)
)

var (
	msgOverflow             = mysql.MySQLErrName[mysql.ErrDataOutOfRange]
	msgTruncatedWrongVal    = mysql.MySQLErrName[mysql.ErrTruncatedWrongValue]
	msgCastAsSignedOverflow = "Cast to signed converted positive out-of-range integer to it's negative complement"
	msgCastNegIntAsUnsigned = "Cast to unsigned converted negative integer to it's positive complement"
)

func init() {
	typesMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDataTooLong:         mysql.ErrDataTooLong,
		codeIllegalValueForType: mysql.ErrIllegalValueForType,
		codeTruncated:           mysql.WarnDataTruncated,
		codeOverflow:            mysql.ErrDataOutOfRange,
		codeDivByZero:           mysql.ErrDivisionByZero,
		codeTooBigDisplayWidth:  mysql.ErrTooBigDisplaywidth,
		codeTooBigFieldLength:   mysql.ErrTooBigFieldlength,
		codeTooBigSet:           mysql.ErrTooBigSet,
		codeWrongFieldSpec:      mysql.ErrWrongFieldSpec,
		codeTruncatedWrongValue: mysql.ErrTruncatedWrongValue,
		codeUnknown:             mysql.ErrUnknown,
		codeInvalidDefault:      mysql.ErrInvalidDefault,
		codeMBiggerThanD:        mysql.ErrMBiggerThanD,
	}
	terror.ErrClassToMySQLCodes[terror.ClassTypes] = typesMySQLErrCodes
}

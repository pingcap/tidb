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
	"github.com/pingcap/parser/terror"
	parser_types "github.com/pingcap/parser/types"
	mysql "github.com/pingcap/tidb/errno"
)

// const strings for ErrWrongValue
const (
	DateTimeStr = "datetime"
	TimeStr     = "time"
)

var (
	// ErrInvalidDefault is returned when meet a invalid default value.
	ErrInvalidDefault = parser_types.ErrInvalidDefault
	// ErrDataTooLong is returned when converts a string value that is longer than field type length.
	ErrDataTooLong = terror.ClassTypes.New(mysql.ErrDataTooLong, mysql.MySQLErrName[mysql.ErrDataTooLong])
	// ErrIllegalValueForType is returned when value of type is illegal.
	ErrIllegalValueForType = terror.ClassTypes.New(mysql.ErrIllegalValueForType, mysql.MySQLErrName[mysql.ErrIllegalValueForType])
	// ErrTruncated is returned when data has been truncated during conversion.
	ErrTruncated = terror.ClassTypes.New(mysql.WarnDataTruncated, mysql.MySQLErrName[mysql.WarnDataTruncated])
	// ErrOverflow is returned when data is out of range for a field type.
	ErrOverflow = terror.ClassTypes.New(mysql.ErrDataOutOfRange, mysql.MySQLErrName[mysql.ErrDataOutOfRange])
	// ErrDivByZero is return when do division by 0.
	ErrDivByZero = terror.ClassTypes.New(mysql.ErrDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])
	// ErrTooBigDisplayWidth is return when display width out of range for column.
	ErrTooBigDisplayWidth = terror.ClassTypes.New(mysql.ErrTooBigDisplaywidth, mysql.MySQLErrName[mysql.ErrTooBigDisplaywidth])
	// ErrTooBigFieldLength is return when column length too big for column.
	ErrTooBigFieldLength = terror.ClassTypes.New(mysql.ErrTooBigFieldlength, mysql.MySQLErrName[mysql.ErrTooBigFieldlength])
	// ErrTooBigSet is returned when too many strings for column.
	ErrTooBigSet = terror.ClassTypes.New(mysql.ErrTooBigSet, mysql.MySQLErrName[mysql.ErrTooBigSet])
	// ErrTooBigScale is returned when type DECIMAL/NUMERIC scale is bigger than mysql.MaxDecimalScale.
	ErrTooBigScale = terror.ClassTypes.New(mysql.ErrTooBigScale, mysql.MySQLErrName[mysql.ErrTooBigScale])
	// ErrTooBigPrecision is returned when type DECIMAL/NUMERIC precision is bigger than mysql.MaxDecimalWidth
	ErrTooBigPrecision = terror.ClassTypes.New(mysql.ErrTooBigPrecision, mysql.MySQLErrName[mysql.ErrTooBigPrecision])
	// ErrBadNumber is return when parsing an invalid binary decimal number.
	ErrBadNumber = terror.ClassTypes.New(mysql.ErrBadNumber, mysql.MySQLErrName[mysql.ErrBadNumber])
	// ErrInvalidFieldSize is returned when the precision of a column is out of range.
	ErrInvalidFieldSize = terror.ClassTypes.New(mysql.ErrInvalidFieldSize, mysql.MySQLErrName[mysql.ErrInvalidFieldSize])
	// ErrMBiggerThanD is returned when precision less than the scale.
	ErrMBiggerThanD = terror.ClassTypes.New(mysql.ErrMBiggerThanD, mysql.MySQLErrName[mysql.ErrMBiggerThanD])
	// ErrWarnDataOutOfRange is returned when the value in a numeric column that is outside the permissible range of the column data type.
	// See https://dev.mysql.com/doc/refman/5.5/en/out-of-range-and-overflow.html for details
	ErrWarnDataOutOfRange = terror.ClassTypes.New(mysql.ErrWarnDataOutOfRange, mysql.MySQLErrName[mysql.ErrWarnDataOutOfRange])
	// ErrDuplicatedValueInType is returned when enum column has duplicated value.
	ErrDuplicatedValueInType = terror.ClassTypes.New(mysql.ErrDuplicatedValueInType, mysql.MySQLErrName[mysql.ErrDuplicatedValueInType])
	// ErrDatetimeFunctionOverflow is returned when the calculation in datetime function cause overflow.
	ErrDatetimeFunctionOverflow = terror.ClassTypes.New(mysql.ErrDatetimeFunctionOverflow, mysql.MySQLErrName[mysql.ErrDatetimeFunctionOverflow])
	// ErrCastAsSignedOverflow is returned when positive out-of-range integer, and convert to it's negative complement.
	ErrCastAsSignedOverflow = terror.ClassTypes.New(mysql.ErrCastAsSignedOverflow, mysql.MySQLErrName[mysql.ErrCastAsSignedOverflow])
	// ErrCastNegIntAsUnsigned is returned when a negative integer be casted to an unsigned int.
	ErrCastNegIntAsUnsigned = terror.ClassTypes.New(mysql.ErrCastNegIntAsUnsigned, mysql.MySQLErrName[mysql.ErrCastNegIntAsUnsigned])
	// ErrInvalidYearFormat is returned when the input is not a valid year format.
	ErrInvalidYearFormat = terror.ClassTypes.New(mysql.ErrInvalidYearFormat, mysql.MySQLErrName[mysql.ErrInvalidYearFormat])
	// ErrInvalidYear is returned when the input value is not a valid year.
	ErrInvalidYear = terror.ClassTypes.New(mysql.ErrInvalidYear, mysql.MySQLErrName[mysql.ErrInvalidYear])
	// ErrTruncatedWrongVal is returned when data has been truncated during conversion.
	ErrTruncatedWrongVal = terror.ClassTypes.New(mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrTruncatedWrongValue])
	// ErrInvalidWeekModeFormat is returned when the week mode is wrong.
	ErrInvalidWeekModeFormat = terror.ClassTypes.New(mysql.ErrInvalidWeekModeFormat, mysql.MySQLErrName[mysql.ErrInvalidWeekModeFormat])
	// ErrWrongValue is returned when the input value is in wrong format.
	ErrWrongValue = terror.ClassTypes.New(mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrWrongValue])
)

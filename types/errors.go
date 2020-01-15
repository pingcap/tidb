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
	pterror "github.com/pingcap/parser/terror"
	parser_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
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
	ErrDataTooLong = terror.New(pterror.ClassTypes, mysql.ErrDataTooLong, mysql.MySQLErrName[mysql.ErrDataTooLong])
	// ErrIllegalValueForType is returned when value of type is illegal.
	ErrIllegalValueForType = terror.New(pterror.ClassTypes, mysql.ErrIllegalValueForType, mysql.MySQLErrName[mysql.ErrIllegalValueForType])
	// ErrTruncated is returned when data has been truncated during conversion.
	ErrTruncated = terror.New(pterror.ClassTypes, mysql.WarnDataTruncated, mysql.MySQLErrName[mysql.WarnDataTruncated])
	// ErrOverflow is returned when data is out of range for a field type.
	ErrOverflow = terror.New(pterror.ClassTypes, mysql.ErrDataOutOfRange, mysql.MySQLErrName[mysql.ErrDataOutOfRange])
	// ErrDivByZero is return when do division by 0.
	ErrDivByZero = terror.New(pterror.ClassTypes, mysql.ErrDivisionByZero, mysql.MySQLErrName[mysql.ErrDivisionByZero])
	// ErrTooBigDisplayWidth is return when display width out of range for column.
	ErrTooBigDisplayWidth = terror.New(pterror.ClassTypes, mysql.ErrTooBigDisplaywidth, mysql.MySQLErrName[mysql.ErrTooBigDisplaywidth])
	// ErrTooBigFieldLength is return when column length too big for column.
	ErrTooBigFieldLength = terror.New(pterror.ClassTypes, mysql.ErrTooBigFieldlength, mysql.MySQLErrName[mysql.ErrTooBigFieldlength])
	// ErrTooBigSet is returned when too many strings for column.
	ErrTooBigSet = terror.New(pterror.ClassTypes, mysql.ErrTooBigSet, mysql.MySQLErrName[mysql.ErrTooBigSet])
	// ErrTooBigScale is returned when type DECIMAL/NUMERIC scale is bigger than mysql.MaxDecimalScale.
	ErrTooBigScale = terror.New(pterror.ClassTypes, mysql.ErrTooBigScale, mysql.MySQLErrName[mysql.ErrTooBigScale])
	// ErrTooBigPrecision is returned when type DECIMAL/NUMERIC precision is bigger than mysql.MaxDecimalWidth
	ErrTooBigPrecision = terror.New(pterror.ClassTypes, mysql.ErrTooBigPrecision, mysql.MySQLErrName[mysql.ErrTooBigPrecision])
	// ErrBadNumber is return when parsing an invalid binary decimal number.
	ErrBadNumber = terror.New(pterror.ClassTypes, mysql.ErrBadNumber, mysql.MySQLErrName[mysql.ErrBadNumber])
	// ErrInvalidFieldSize is returned when the precision of a column is out of range.
	ErrInvalidFieldSize = terror.New(pterror.ClassTypes, mysql.ErrInvalidFieldSize, mysql.MySQLErrName[mysql.ErrInvalidFieldSize])
	// ErrMBiggerThanD is returned when precision less than the scale.
	ErrMBiggerThanD = terror.New(pterror.ClassTypes, mysql.ErrMBiggerThanD, mysql.MySQLErrName[mysql.ErrMBiggerThanD])
	// ErrWarnDataOutOfRange is returned when the value in a numeric column that is outside the permissible range of the column data type.
	// See https://dev.mysql.com/doc/refman/5.5/en/out-of-range-and-overflow.html for details
	ErrWarnDataOutOfRange = terror.New(pterror.ClassTypes, mysql.ErrWarnDataOutOfRange, mysql.MySQLErrName[mysql.ErrWarnDataOutOfRange])
	// ErrDuplicatedValueInType is returned when enum column has duplicated value.
	ErrDuplicatedValueInType = terror.New(pterror.ClassTypes, mysql.ErrDuplicatedValueInType, mysql.MySQLErrName[mysql.ErrDuplicatedValueInType])
	// ErrDatetimeFunctionOverflow is returned when the calculation in datetime function cause overflow.
	ErrDatetimeFunctionOverflow = terror.New(pterror.ClassTypes, mysql.ErrDatetimeFunctionOverflow, mysql.MySQLErrName[mysql.ErrDatetimeFunctionOverflow])
	// ErrCastAsSignedOverflow is returned when positive out-of-range integer, and convert to it's negative complement.
	ErrCastAsSignedOverflow = terror.New(pterror.ClassTypes, mysql.ErrCastAsSignedOverflow, mysql.MySQLErrName[mysql.ErrCastAsSignedOverflow])
	// ErrCastNegIntAsUnsigned is returned when a negative integer be casted to an unsigned int.
	ErrCastNegIntAsUnsigned = terror.New(pterror.ClassTypes, mysql.ErrCastNegIntAsUnsigned, mysql.MySQLErrName[mysql.ErrCastNegIntAsUnsigned])
	// ErrInvalidYearFormat is returned when the input is not a valid year format.
	ErrInvalidYearFormat = terror.New(pterror.ClassTypes, mysql.ErrInvalidYearFormat, mysql.MySQLErrName[mysql.ErrInvalidYearFormat])
	// ErrInvalidYear is returned when the input value is not a valid year.
	ErrInvalidYear = terror.New(pterror.ClassTypes, mysql.ErrInvalidYear, mysql.MySQLErrName[mysql.ErrInvalidYear])
	// ErrTruncatedWrongVal is returned when data has been truncated during conversion.
	ErrTruncatedWrongVal = terror.New(pterror.ClassTypes, mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrTruncatedWrongValue])
	// ErrInvalidWeekModeFormat is returned when the week mode is wrong.
	ErrInvalidWeekModeFormat = terror.New(pterror.ClassTypes, mysql.ErrInvalidWeekModeFormat, mysql.MySQLErrName[mysql.ErrInvalidWeekModeFormat])
	// ErrWrongValue is returned when the input value is in wrong format.
	ErrWrongValue = terror.New(pterror.ClassTypes, mysql.ErrWrongValue, mysql.MySQLErrName[mysql.ErrWrongValue])
)

func init() {
	typesMySQLErrCodes := map[pterror.ErrCode]uint16{
		mysql.ErrInvalidDefault:           mysql.ErrInvalidDefault,
		mysql.ErrDataTooLong:              mysql.ErrDataTooLong,
		mysql.ErrIllegalValueForType:      mysql.ErrIllegalValueForType,
		mysql.WarnDataTruncated:           mysql.WarnDataTruncated,
		mysql.ErrDataOutOfRange:           mysql.ErrDataOutOfRange,
		mysql.ErrDivisionByZero:           mysql.ErrDivisionByZero,
		mysql.ErrTooBigDisplaywidth:       mysql.ErrTooBigDisplaywidth,
		mysql.ErrTooBigFieldlength:        mysql.ErrTooBigFieldlength,
		mysql.ErrTooBigSet:                mysql.ErrTooBigSet,
		mysql.ErrTooBigScale:              mysql.ErrTooBigScale,
		mysql.ErrTooBigPrecision:          mysql.ErrTooBigPrecision,
		mysql.ErrBadNumber:                mysql.ErrBadNumber,
		mysql.ErrInvalidFieldSize:         mysql.ErrInvalidFieldSize,
		mysql.ErrMBiggerThanD:             mysql.ErrMBiggerThanD,
		mysql.ErrWarnDataOutOfRange:       mysql.ErrWarnDataOutOfRange,
		mysql.ErrDuplicatedValueInType:    mysql.ErrDuplicatedValueInType,
		mysql.ErrDatetimeFunctionOverflow: mysql.ErrDatetimeFunctionOverflow,
		mysql.ErrCastAsSignedOverflow:     mysql.ErrCastAsSignedOverflow,
		mysql.ErrCastNegIntAsUnsigned:     mysql.ErrCastNegIntAsUnsigned,
		mysql.ErrInvalidYearFormat:        mysql.ErrInvalidYearFormat,
		mysql.ErrInvalidYear:              mysql.ErrInvalidYear,
		mysql.ErrTruncatedWrongValue:      mysql.ErrTruncatedWrongValue,
		mysql.ErrInvalidTimeFormat:        mysql.ErrInvalidTimeFormat,
		mysql.ErrInvalidWeekModeFormat:    mysql.ErrInvalidWeekModeFormat,
		mysql.ErrWrongValue:               mysql.ErrWrongValue,
	}
	for cls, code := range pterror.ErrClassToMySQLCodes[pterror.ClassTypes] {
		if _, ok := typesMySQLErrCodes[cls]; !ok {
			typesMySQLErrCodes[cls] = code
		}
	}
	terror.ErrClassToMySQLCodes[pterror.ClassTypes] = typesMySQLErrCodes

	for cls, code := range pterror.ErrClassToMySQLCodes[pterror.ClassParser] {
		if _, ok := typesMySQLErrCodes[cls]; !ok {
			typesMySQLErrCodes[cls] = code
		}
	}
	terror.ErrClassToMySQLCodes[pterror.ClassParser] = typesMySQLErrCodes
}

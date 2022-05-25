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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	mysql "github.com/pingcap/tidb/errno"
	parser_types "github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/util/dbterror"
)

// const strings for ErrWrongValue
const (
	DateTimeStr  = "datetime"
	DateStr      = "date"
	TimeStr      = "time"
	TimestampStr = "timestamp"
)

var (
	// ErrInvalidDefault is returned when meet a invalid default value.
	ErrInvalidDefault = parser_types.ErrInvalidDefault
	// ErrDataTooLong is returned when converts a string value that is longer than field type length.
	ErrDataTooLong = dbterror.ClassTypes.NewStd(mysql.ErrDataTooLong)
	// ErrIllegalValueForType is returned when value of type is illegal.
	ErrIllegalValueForType = dbterror.ClassTypes.NewStd(mysql.ErrIllegalValueForType)
	// ErrTruncated is returned when data has been truncated during conversion.
	ErrTruncated = dbterror.ClassTypes.NewStd(mysql.WarnDataTruncated)
	// ErrOverflow is returned when data is out of range for a field type.
	ErrOverflow = dbterror.ClassTypes.NewStd(mysql.ErrDataOutOfRange)
	// ErrDivByZero is return when do division by 0.
	ErrDivByZero = dbterror.ClassTypes.NewStd(mysql.ErrDivisionByZero)
	// ErrTooBigDisplayWidth is return when display width out of range for column.
	ErrTooBigDisplayWidth = dbterror.ClassTypes.NewStd(mysql.ErrTooBigDisplaywidth)
	// ErrTooBigFieldLength is return when column length too big for column.
	ErrTooBigFieldLength = dbterror.ClassTypes.NewStd(mysql.ErrTooBigFieldlength)
	// ErrTooBigSet is returned when too many strings for column.
	ErrTooBigSet = dbterror.ClassTypes.NewStd(mysql.ErrTooBigSet)
	// ErrTooBigScale is returned when type DECIMAL/NUMERIC scale is bigger than mysql.MaxDecimalScale.
	ErrTooBigScale = dbterror.ClassTypes.NewStd(mysql.ErrTooBigScale)
	// ErrTooBigPrecision is returned when type DECIMAL/NUMERIC precision is bigger than mysql.MaxDecimalWidth
	ErrTooBigPrecision = dbterror.ClassTypes.NewStd(mysql.ErrTooBigPrecision)
	// ErrBadNumber is return when parsing an invalid binary decimal number.
	ErrBadNumber = dbterror.ClassTypes.NewStd(mysql.ErrBadNumber)
	// ErrInvalidFieldSize is returned when the precision of a column is out of range.
	ErrInvalidFieldSize = dbterror.ClassTypes.NewStd(mysql.ErrInvalidFieldSize)
	// ErrMBiggerThanD is returned when precision less than the scale.
	ErrMBiggerThanD = dbterror.ClassTypes.NewStd(mysql.ErrMBiggerThanD)
	// ErrWarnDataOutOfRange is returned when the value in a numeric column that is outside the permissible range of the column data type.
	// See https://dev.mysql.com/doc/refman/5.5/en/out-of-range-and-overflow.html for details
	ErrWarnDataOutOfRange = dbterror.ClassTypes.NewStd(mysql.ErrWarnDataOutOfRange)
	// ErrDuplicatedValueInType is returned when enum column has duplicated value.
	ErrDuplicatedValueInType = dbterror.ClassTypes.NewStd(mysql.ErrDuplicatedValueInType)
	// ErrDatetimeFunctionOverflow is returned when the calculation in datetime function cause overflow.
	ErrDatetimeFunctionOverflow = dbterror.ClassTypes.NewStd(mysql.ErrDatetimeFunctionOverflow)
	// ErrCastAsSignedOverflow is returned when positive out-of-range integer, and convert to it's negative complement.
	ErrCastAsSignedOverflow = dbterror.ClassTypes.NewStd(mysql.ErrCastAsSignedOverflow)
	// ErrCastNegIntAsUnsigned is returned when a negative integer be casted to an unsigned int.
	ErrCastNegIntAsUnsigned = dbterror.ClassTypes.NewStd(mysql.ErrCastNegIntAsUnsigned)
	// ErrInvalidYearFormat is returned when the input is not a valid year format.
	ErrInvalidYearFormat = dbterror.ClassTypes.NewStd(mysql.ErrInvalidYearFormat)
	// ErrInvalidYear is returned when the input value is not a valid year.
	ErrInvalidYear = dbterror.ClassTypes.NewStd(mysql.ErrInvalidYear)
	// ErrTruncatedWrongVal is returned when data has been truncated during conversion.
	ErrTruncatedWrongVal = dbterror.ClassTypes.NewStd(mysql.ErrTruncatedWrongValue)
	// ErrInvalidWeekModeFormat is returned when the week mode is wrong.
	ErrInvalidWeekModeFormat = dbterror.ClassTypes.NewStd(mysql.ErrInvalidWeekModeFormat)
	// ErrWrongFieldSpec is returned when the column specifier incorrect.
	ErrWrongFieldSpec = dbterror.ClassTypes.NewStd(mysql.ErrWrongFieldSpec)
	// ErrSyntax is returned when the syntax is not allowed.
	ErrSyntax = dbterror.ClassTypes.NewStdErr(mysql.ErrParse, mysql.MySQLErrName[mysql.ErrSyntax])
	// ErrWrongValue is returned when the input value is in wrong format.
	ErrWrongValue = dbterror.ClassTypes.NewStdErr(mysql.ErrTruncatedWrongValue, mysql.MySQLErrName[mysql.ErrWrongValue])
	// ErrWrongValueForType is returned when the input value is in wrong format for function.
	ErrWrongValueForType = dbterror.ClassTypes.NewStdErr(mysql.ErrWrongValueForType, mysql.MySQLErrName[mysql.ErrWrongValueForType])
	// ErrPartitionStatsMissing is returned when the partition-level stats is missing and the build global-level stats fails.
	// Put this error here is to prevent `import cycle not allowed`.
	ErrPartitionStatsMissing = dbterror.ClassTypes.NewStd(mysql.ErrPartitionStatsMissing)
	// ErrPartitionColumnStatsMissing is returned when the partition-level column stats is missing and the build global-level stats fails.
	// Put this error here is to prevent `import cycle not allowed`.
	ErrPartitionColumnStatsMissing = dbterror.ClassTypes.NewStd(mysql.ErrPartitionColumnStatsMissing)
)

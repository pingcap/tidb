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
	// ErrTruncated is returned when data has been truncated during conversion.
	ErrTruncated = terror.ClassTypes.New(codeTruncated, "Data Truncated")
	// ErrTruncatedWrongVal is returned when data has been truncated during conversion.
	ErrTruncatedWrongVal = terror.ClassTypes.New(codeTruncatedWrongValue, msgTruncatedWrongVal)
	// ErrOverflow is returned when data is out of range for a field type.
	ErrOverflow = terror.ClassTypes.New(codeOverflow, msgOverflow)
	// ErrDivByZero is return when do division by 0.
	ErrDivByZero = terror.ClassTypes.New(codeDivByZero, "Division by 0")
	// ErrBadNumber is return when parsing an invalid binary decimal number.
	ErrBadNumber = terror.ClassTypes.New(codeBadNumber, "Bad Number")
	// ErrCastSignedOverflow is returned when positive out-of-range integer, and convert to it's negative complement
	ErrCastSignedOverflow = terror.ClassTypes.New(codeCastSignedOverflow, msgCastSignedOverflow)
)

const (
	codeBadNumber terror.ErrCode = 1

	codeDataTooLong         terror.ErrCode = terror.ErrCode(mysql.ErrDataTooLong)
	codeTruncated           terror.ErrCode = terror.ErrCode(mysql.WarnDataTruncated)
	codeOverflow            terror.ErrCode = terror.ErrCode(mysql.ErrDataOutOfRange)
	codeDivByZero           terror.ErrCode = terror.ErrCode(mysql.ErrDivisionByZero)
	codeTruncatedWrongValue terror.ErrCode = terror.ErrCode(mysql.ErrTruncatedWrongValue)

	// warning caused by `select cast('18446744073709551614' as signed);`
	codeCastSignedOverflow terror.ErrCode = terror.ErrCode(mysql.ErrUnknown)
)

var (
	msgOverflow           = mysql.MySQLErrName[mysql.ErrDataOutOfRange]
	msgTruncatedWrongVal  = mysql.MySQLErrName[mysql.ErrTruncatedWrongValue]
	msgCastSignedOverflow = "Cast to signed converted positive out-of-range integer to it's negative complement"
)

func init() {
	typesMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDataTooLong:         mysql.ErrDataTooLong,
		codeTruncated:           mysql.WarnDataTruncated,
		codeOverflow:            mysql.ErrDataOutOfRange,
		codeDivByZero:           mysql.ErrDivisionByZero,
		codeTruncatedWrongValue: mysql.ErrTruncatedWrongValue,
		codeCastSignedOverflow:  mysql.ErrUnknown,
	}
	terror.ErrClassToMySQLCodes[terror.ClassTypes] = typesMySQLErrCodes
}

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

import (
	"fmt"

	"github.com/pingcap/errors"
)

// Portable analogs of some common call errors.
var (
	ErrBadConn       = errors.New("connection was bad")
	ErrMalformPacket = errors.New("malform packet error")
)

// SQLError records an error information, from executing SQL.
type SQLError struct {
	Code    uint16
	Message string
	State   string
}

// Error prints errors, with a formatted string.
func (e *SQLError) Error() string {
	return fmt.Sprintf("ERROR %d (%s): %s", e.Code, e.State, e.Message)
}

// NewErr generates a SQL error, with an error code and default format specifier defined in MySQLErrName.
func NewErr(errCode uint16, args ...interface{}) *SQLError {
	e := &SQLError{Code: errCode}

	if s, ok := MySQLState[errCode]; ok {
		e.State = s
	} else {
		e.State = DefaultMySQLState
	}

	if sqlErr, ok := MySQLErrName[errCode]; ok {
		errors.RedactErrorArg(args, sqlErr.RedactArgPos)
		e.Message = fmt.Sprintf(sqlErr.Raw, args...)
	} else {
		e.Message = fmt.Sprint(args...)
	}

	return e
}

// NewErrf creates a SQL error, with an error code and a format specifier.
func NewErrf(errCode uint16, format string, redactArgPos []int, args ...interface{}) *SQLError {
	e := &SQLError{Code: errCode}

	if s, ok := MySQLState[errCode]; ok {
		e.State = s
	} else {
		e.State = DefaultMySQLState
	}

	errors.RedactErrorArg(args, redactArgPos)
	e.Message = fmt.Sprintf(format, args...)

	return e
}

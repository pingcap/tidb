// Copyright 2020 PingCAP, Inc.
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

package redact

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
)

type redactError struct {
	err             *terror.Error
	redactPositions []int
}

// GenWithStackByArgs generates a new *Error with the same class and code, and new arguments.
func (e *redactError) GenWithStackByArgs(args ...interface{}) error {
	redactErrorArg(args, e.redactPositions)
	return e.err.GenWithStackByArgs(args...)
}

// FastGen generates a new *Error with the same class and code, and a new arguments.
func (e *redactError) FastGenByArgs(args ...interface{}) error {
	redactErrorArg(args, e.redactPositions)
	return e.err.GenWithStackByArgs(args...)
}

// Error implements the error interface.
func (e *redactError) Error() string {
	return e.err.Error()
}

// Equal checks if err is equal to e.
func (e *redactError) Equal(err error) bool {
	if redactErr, ok := err.(*redactError); ok {
		return e.err.Equal(redactErr.err)
	}
	return e.err.Equal(err)
}

// Equal returns terror Code.
func (e *redactError) Code() errors.ErrCode {
	return e.err.Code()
}

// Equal returns terror RFCCode.
func (e *redactError) RFCCode() errors.RFCErrorCode {
	return e.err.RFCCode()
}

func redactErrorArg(args []interface{}, position []int) {
	if config.RedactLogEnabled() {
		for _, pos := range position {
			if len(args) > pos {
				args[pos] = "?"
			}
		}
	}
}

// NewRedactError returns a new redact error.
func NewRedactError(err *terror.Error, redactPositions ...int) *redactError {
	return &redactError{err, redactPositions}
}

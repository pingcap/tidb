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

package terror

import (
	"fmt"
	"github.com/juju/errors"
	"strconv"
)

// ErrClass represents a class of errors.
type ErrClass int

// ErrCode represents a specific error type in a error class.
// Same error code can be used in different error classes.
type ErrCode int

// Error classes
const (
	Parser ErrClass = iota + 1
	Optimizer
	KV
	Server
	// Add more as needed.
)

// String implements fmt.Stringer interface.
func (ec ErrClass) String() string {
	switch ec {
	case Parser:
		return "parser"
	case Optimizer:
		return "optimizer"
	case KV:
		return "kv"
	case Server:
		return "server"
	}
	return strconv.Itoa(int(ec))
}

// Equal returns true if err is *Error with the same clase and code.
func (ec ErrClass) Equal(err error, code ErrCode) bool {
	e := errors.Cause(err)
	if e == nil {
		return false
	}
	if te, ok := e.(*Error); ok {
		return te.Class == ec && te.Code == code
	}
	return false
}

// NotEqual returns true if err is not *Error with the same class
// and the same code.
func (ec ErrClass) NotEqual(err error, code ErrCode) bool {
	return !ec.Equal(err, code)
}

// EqualClass returns true if err is *Error with the same class.
func (ec ErrClass) EqualClass(err error) bool {
	e := errors.Cause(err)
	if e == nil {
		return false
	}
	if te, ok := e.(*Error); ok {
		return te.Class == ec
	}
	return false
}

// NotEqualClass returns true if err is not *Error with the same class.
func (ec ErrClass) NotEqualClass(err error) bool {
	return !ec.EqualClass(err)
}

// New creates an *Error with an error code, message format and arguments.
func (ec ErrClass) New(code ErrCode, message string, args ...interface{}) *Error {
	if len(args) != 0 {
		message = fmt.Sprintf(message, args...)
	}
	return &Error{
		Class:   ec,
		Code:    code,
		Message: message,
	}
}

// Error implements error interface and adds integer Class and Code, so
// errors with different message can be compared.
type Error struct {
	Class   ErrClass
	Code    ErrCode
	Message string
}

// Error implements error interface.
func (te *Error) Error() string {
	return fmt.Sprintf("[%s:%d]%s", te.Class, te.Code, te.Message)
}

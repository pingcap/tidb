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
	"strconv"

	"github.com/juju/errors"
)

// ErrCode represents a specific error type in a error class.
// Same error code can be used in different error classes.
type ErrCode int

// Schema error codes
const (
	DatabaseNotExists ErrCode = iota + 1
)

// Executor error codes
const (
	CommitNotInTransaction ErrCode = iota + 1
	RollbackNotInTransaction
)

// KV error codes
const (
	IncompatibleDBFormat ErrCode = iota + 1
	NoDataForHandle
)

// ErrClass represents a class of errors.
type ErrClass int

// Error classes
const (
	Parser ErrClass = iota + 1
	Schema
	Optimizer
	Executor
	KV
	Server
	// Add more as needed.
)

// String implements fmt.Stringer interface.
func (ec ErrClass) String() string {
	switch ec {
	case Parser:
		return "parser"
	case Schema:
		return "schema"
	case Optimizer:
		return "optimizer"
	case Executor:
		return "executor"
	case KV:
		return "kv"
	case Server:
		return "server"
	}
	return strconv.Itoa(int(ec))
}

// Equal returns true if err is *Error with the same class and code.
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

// ErrorEqual returns a boolean indicating whether err1 is equal to err2.
func ErrorEqual(err1, err2 error) bool {
	e1 := errors.Cause(err1)
	e2 := errors.Cause(err2)

	if e1 == e2 {
		return true
	}

	if e1 == nil || e2 == nil {
		return e1 == e2
	}

	te1, ok1 := e1.(*Error)
	te2, ok2 := e2.(*Error)
	if ok1 && ok2 {
		return te1.Class == te2.Class && te1.Code == te2.Code
	}

	return e1.Error() == e2.Error()
}

// ErrorNotEqual returns a boolean indicating whether err1 isn't equal to err2.
func ErrorNotEqual(err1, err2 error) bool {
	return !ErrorEqual(err1, err2)
}

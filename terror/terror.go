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

// Common base error instances.
var (
	DatabaseNotExists = ClassSchema.New(CodeDatabaseNotExists, "database not exists")
	TableNotExists    = ClassSchema.New(CodeTableNotExists, "table not exists")

	CommitNotInTransaction   = ClassExecutor.New(CodeCommitNotInTransaction, "commit not in transaction")
	RollbackNotInTransaction = ClassExecutor.New(CodeRollbackNotInTransaction, "rollback not in transaction")
)

// ErrCode represents a specific error type in a error class.
// Same error code can be used in different error classes.
type ErrCode int

// Schema error codes.
const (
	CodeDatabaseNotExists ErrCode = iota + 1
	CodeTableNotExists
)

// Executor error codes.
const (
	CodeCommitNotInTransaction ErrCode = iota + 1
	CodeRollbackNotInTransaction
)

// KV error codes.
const (
	CodeIncompatibleDBFormat ErrCode = iota + 1
	CodeNoDataForHandle
)

// ErrClass represents a class of errors.
type ErrClass int

// Error classes.
const (
	ClassParser ErrClass = iota + 1
	ClassSchema
	ClassOptimizer
	ClassExecutor
	ClassKV
	ClassServer
	// Add more as needed.
)

// String implements fmt.Stringer interface.
func (ec ErrClass) String() string {
	switch ec {
	case ClassParser:
		return "parser"
	case ClassSchema:
		return "schema"
	case ClassOptimizer:
		return "optimizer"
	case ClassExecutor:
		return "executor"
	case ClassKV:
		return "kv"
	case ClassServer:
		return "server"
	}
	return strconv.Itoa(int(ec))
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

// New creates an *Error with an error code and an error message.
// Usually used to create base *Error.
func (ec ErrClass) New(code ErrCode, message string) *Error {
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
func (e *Error) Error() string {
	return fmt.Sprintf("[%s:%d]%s", e.Class, e.Code, e.Message)
}

// Gen generates a new *Error with the same class and code, and a new formatted message.
func (e *Error) Gen(format string, args ...interface{}) *Error {
	err := *e
	err.Message = fmt.Sprintf(format, args...)
	return &err
}

// Equal checks if err is equal to e.
func (e *Error) Equal(err error) bool {
	originErr := errors.Cause(err)
	if originErr == nil {
		return false
	}
	inErr, ok := originErr.(*Error)
	return ok && e.Class == inErr.Class && e.Code == inErr.Code
}

// NotEqual checks if err is not equal to e.
func (e *Error) NotEqual(err error) bool {
	return !e.Equal(err)
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

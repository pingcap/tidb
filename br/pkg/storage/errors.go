// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"fmt"

	"github.com/pingcap/errors"
	brerrors "github.com/pingcap/tidb/br/pkg/errors"
)

// Error in storage package provides both a simple standard error with given
// message and a BR specified error class that can be converted to BR's error.
type Error struct {
	error
	brErrorClass *errors.Error
}

//// NewError creates a new Error with given error and BR error class.
//func NewError(brErr *errors.Error, err error) *Error {
//	return &Error{
//		error:        err,
//		brErrorClass: brErr,
//	}
//}

// NewErrorWithMessage creates a new Error with given text message and BR error class.
func NewErrorWithMessage(brErr *errors.Error, format string, args ...interface{}) *Error {
	return &Error{
		error:        fmt.Errorf(format, args...),
		brErrorClass: brErr,
	}
}

// NewSimpleError wraps Error and trace on given standard error. If the given
// error is already a *Error, it will be returned directly.
func NewSimpleError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{
		error:        errors.Trace(err),
		brErrorClass: brerrors.ErrUnknown,
	}
}

// NewSimpleErrorWithMessage wraps Error on a standard error.
func NewSimpleErrorWithMessage(format string, args ...interface{}) *Error {
	return &Error{
		error:        fmt.Errorf(format, args...),
		brErrorClass: brerrors.ErrUnknown,
	}
}

func (e *Error) ToBRError() error {
	return errors.Annotate(e.brErrorClass, e.error.Error())
}

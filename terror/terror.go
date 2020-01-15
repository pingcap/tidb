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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	pterr "github.com/pingcap/parser/terror"
	log "github.com/sirupsen/logrus"
)

// Global error instances.
var (
	ErrCritical           = New(pterr.ClassGlobal, pterr.CodeExecResultIsEmpty, "critical error %v")
	ErrResultUndetermined = New(pterr.ClassGlobal, pterr.CodeResultUndetermined, "execution result undetermined")
)

var defaultMySQLErrorCode uint16

// TError presents error in TiDB.
type TError struct {
	*pterr.BaseError
}

// New creates an *Error with an error code and an error message.
// Usually used to create base *Error.
func New(ec pterr.ErrClass, code pterr.ErrCode, message string) *TError {
	return &TError{ec.NewBaseError(code, message)}
}

// ToSQLError convert Error to mysql.SQLError.
func (e *TError) ToSQLError() *mysql.SQLError {
	terr := e.ToBaseError()
	code := getMySQLErrorCode(terr)
	return mysql.NewErrf(code, "%s", terr.GetMsg())
}

// ToBaseError convert Error to parser's terror.BaseError.
func (e *TError) ToBaseError() *pterr.BaseError {
	return e.BaseError
}

// GenWithStack generates a new *Error with the same class and code, and a new formatted message.
func (e *TError) GenWithStack(format string, args ...interface{}) error {
	bErr := *e.BaseError
	err := TError{BaseError: &bErr}
	err.SetMessage(format)
	err.SetArgs(args)
	return errors.AddStack(&err)
}

// GenWithStackByArgs generates a new *Error with the same class and code, and new arguments.
func (e *TError) GenWithStackByArgs(args ...interface{}) error {
	bErr := *e.BaseError
	err := TError{BaseError: &bErr}
	err.SetArgs(args)
	return errors.AddStack(&err)
}

// FastGen generates a new *Error with the same class and code, and a new formatted message.
// This will not call runtime.Caller to get file and line.
func (e *TError) FastGen(format string, args ...interface{}) error {
	bErr := *e.BaseError
	err := TError{BaseError: &bErr}
	err.SetMessage(format)
	err.SetArgs(args)
	return errors.SuspendStack(&err)
}

// FastGenByArgs generates a new *Error with the same class and code, and a new arguments.
// This will not call runtime.Caller to get file and line.
func (e *TError) FastGenByArgs(args ...interface{}) error {
	bErr := *e.BaseError
	err := TError{BaseError: &bErr}
	err.SetArgs(args)
	return errors.SuspendStack(&err)
}

func getMySQLErrorCode(e *pterr.BaseError) uint16 {
	codeMap, ok := ErrClassToMySQLCodes[e.Class()]
	if !ok {
		log.Warnf("Unknown error class: %v", e.Class())
		return defaultMySQLErrorCode
	}
	code, ok := codeMap[e.Code()]
	if !ok {
		log.Debugf("Unknown error class: %v code: %v", e.Class(), e.Code())
		return defaultMySQLErrorCode
	}
	return code
}

var (
	// ErrClassToMySQLCodes is the map of ErrClass to code-map.
	ErrClassToMySQLCodes map[pterr.ErrClass]map[pterr.ErrCode]uint16
)

func init() {
	ErrClassToMySQLCodes = make(map[pterr.ErrClass]map[pterr.ErrCode]uint16)
	defaultMySQLErrorCode = mysql.ErrUnknown
}

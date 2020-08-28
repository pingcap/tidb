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
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// ErrCode represents a specific error type in a error class.
// Same error code can be used in different error classes.
type ErrCode int

const (
	// Executor error codes.

	// CodeUnknown is for errors of unknown reason.
	CodeUnknown ErrCode = -1
	// CodeExecResultIsEmpty indicates execution result is empty.
	CodeExecResultIsEmpty ErrCode = 3

	// Expression error codes.

	// CodeMissConnectionID indicates connection id is missing.
	CodeMissConnectionID ErrCode = 1

	// Special error codes.

	// CodeResultUndetermined indicates the sql execution result is undetermined.
	CodeResultUndetermined ErrCode = 2
)

// ErrClass represents a class of errors.
type ErrClass int

// Error classes.
var (
	ClassAutoid     = RegisterErrorClass(1, "autoid")
	ClassDDL        = RegisterErrorClass(2, "ddl")
	ClassDomain     = RegisterErrorClass(3, "domain")
	ClassEvaluator  = RegisterErrorClass(4, "evaluator")
	ClassExecutor   = RegisterErrorClass(5, "executor")
	ClassExpression = RegisterErrorClass(6, "expression")
	ClassAdmin      = RegisterErrorClass(7, "admin")
	ClassKV         = RegisterErrorClass(8, "kv")
	ClassMeta       = RegisterErrorClass(9, "meta")
	ClassOptimizer  = RegisterErrorClass(10, "planner")
	ClassParser     = RegisterErrorClass(11, "parser")
	ClassPerfSchema = RegisterErrorClass(12, "perfschema")
	ClassPrivilege  = RegisterErrorClass(13, "privilege")
	ClassSchema     = RegisterErrorClass(14, "schema")
	ClassServer     = RegisterErrorClass(15, "server")
	ClassStructure  = RegisterErrorClass(16, "structure")
	ClassVariable   = RegisterErrorClass(17, "variable")
	ClassXEval      = RegisterErrorClass(18, "xeval")
	ClassTable      = RegisterErrorClass(19, "table")
	ClassTypes      = RegisterErrorClass(20, "types")
	ClassGlobal     = RegisterErrorClass(21, "global")
	ClassMockTikv   = RegisterErrorClass(22, "mocktikv")
	ClassJSON       = RegisterErrorClass(23, "json")
	ClassTiKV       = RegisterErrorClass(24, "tikv")
	ClassSession    = RegisterErrorClass(25, "session")
	ClassPlugin     = RegisterErrorClass(26, "plugin")
	ClassUtil       = RegisterErrorClass(27, "util")
	// Add more as needed.
)

var errClass2Desc = make(map[ErrClass]string)
var errCodeMap = make(map[ErrCode]*Error)

// RegisterErrorClass registers new error class for terror.
func RegisterErrorClass(classCode int, desc string) ErrClass {
	errClass := ErrClass(classCode)
	if _, exists := errClass2Desc[errClass]; exists {
		panic(fmt.Sprintf("duplicate register ClassCode %d - %s", classCode, desc))
	}
	errClass2Desc[errClass] = desc
	return errClass
}

// String implements fmt.Stringer interface.
func (ec ErrClass) String() string {
	if s, exists := errClass2Desc[ec]; exists {
		return s
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
		return te.class == ec
	}
	return false
}

// NotEqualClass returns true if err is not *Error with the same class.
func (ec ErrClass) NotEqualClass(err error) bool {
	return !ec.EqualClass(err)
}

// New defines an *Error with an error code and an error message.
// Usually used to create base *Error.
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) New(code ErrCode, message string) *Error {
	clsMap, ok := ErrClassToMySQLCodes[ec]
	if !ok {
		clsMap = make(map[ErrCode]struct{})
		ErrClassToMySQLCodes[ec] = clsMap
	}
	clsMap[code] = struct{}{}
	err := &Error{
		class:   ec,
		code:    code,
		message: message,
	}
	errCodeMap[code] = err
	return err
}

// NewStd calls New using the standard message for the error code
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) NewStd(code ErrCode) *Error {
	return ec.New(code, mysql.MySQLErrName[uint16(code)])
}

// Synthesize synthesizes an *Error in the air
// it didn't register error into ErrClassToMySQLCodes
// so it's goroutine-safe
// and often be used to create Error came from other systems like TiKV.
func (ec ErrClass) Synthesize(code ErrCode, message string) *Error {
	return &Error{
		class:   ec,
		code:    code,
		message: message,
	}
}

// Error implements error interface and adds integer Class and Code, so
// errors with different message can be compared.
type Error struct {
	class      ErrClass
	code       ErrCode
	message    string
	workaround string
	args       []interface{}
	file       string
	line       int
}

// Class returns ErrClass
func (e *Error) Class() ErrClass {
	return e.class
}

// Code returns ErrCode
func (e *Error) Code() ErrCode {
	return e.code
}

// SetWorkaround is a decorator like method which add a workaround to
// error which is convenient for user to search.
func (e *Error) SetWorkaround(workaround string) *Error {
	e.workaround = workaround
	return e
}

// MarshalJSON implements json.Marshaler interface.
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Class ErrClass `json:"class"`
		Code  ErrCode  `json:"code"`
		Msg   string   `json:"message"`
	}{
		Class: e.class,
		Code:  e.code,
		Msg:   e.getMsg(),
	})
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (e *Error) UnmarshalJSON(data []byte) error {
	err := &struct {
		Class ErrClass `json:"class"`
		Code  ErrCode  `json:"code"`
		Msg   string   `json:"message"`
	}{}

	if err := json.Unmarshal(data, &err); err != nil {
		return errors.Trace(err)
	}

	e.class = err.Class
	e.code = err.Code
	e.message = err.Msg
	return nil
}

// Location returns the location where the error is created,
// implements juju/errors locationer interface.
func (e *Error) Location() (file string, line int) {
	return e.file, e.line
}

// Error implements error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("[%s:%d]%s", e.class, e.code, e.getMsg())
}

func (e *Error) getMsg() string {
	if len(e.args) > 0 {
		return fmt.Sprintf(e.message, e.args...)
	}
	return e.message
}

// GenWithStack generates a new *Error with the same class and code, and a new formatted message.
func (e *Error) GenWithStack(format string, args ...interface{}) error {
	err := *e
	err.message = format
	err.args = args
	return errors.AddStack(&err)
}

// GenWithStackByArgs generates a new *Error with the same class and code, and new arguments.
func (e *Error) GenWithStackByArgs(args ...interface{}) error {
	err := *e
	err.args = args
	return errors.AddStack(&err)
}

// FastGen generates a new *Error with the same class and code, and a new formatted message.
// This will not call runtime.Caller to get file and line.
func (e *Error) FastGen(format string, args ...interface{}) error {
	err := *e
	err.message = format
	err.args = args
	return errors.SuspendStack(&err)
}

// FastGen generates a new *Error with the same class and code, and a new arguments.
// This will not call runtime.Caller to get file and line.
func (e *Error) FastGenByArgs(args ...interface{}) error {
	err := *e
	err.args = args
	return errors.SuspendStack(&err)
}

// Equal checks if err is equal to e.
func (e *Error) Equal(err error) bool {
	originErr := errors.Cause(err)
	if originErr == nil {
		return false
	}

	if error(e) == originErr {
		return true
	}
	inErr, ok := originErr.(*Error)
	return ok && e.class == inErr.class && e.code == inErr.code
}

// NotEqual checks if err is not equal to e.
func (e *Error) NotEqual(err error) bool {
	return !e.Equal(err)
}

// ToSQLError convert Error to mysql.SQLError.
func (e *Error) ToSQLError() *mysql.SQLError {
	code := e.getMySQLErrorCode()
	return mysql.NewErrf(code, "%s", e.getMsg())
}

var defaultMySQLErrorCode uint16

func (e *Error) getMySQLErrorCode() uint16 {
	codeMap, ok := ErrClassToMySQLCodes[e.class]
	if !ok {
		log.Warn("Unknown error class", zap.Int("class", int(e.class)))
		return defaultMySQLErrorCode
	}
	_, ok = codeMap[e.code]
	if !ok {
		log.Debug("Unknown error code", zap.Int("class", int(e.class)), zap.Int("code", int(e.code)))
		return defaultMySQLErrorCode
	}
	return uint16(e.code)
}

var (
	// ErrClassToMySQLCodes is the map of ErrClass to code-set.
	ErrClassToMySQLCodes  = make(map[ErrClass]map[ErrCode]struct{})
	ErrCritical           = ClassGlobal.New(CodeExecResultIsEmpty, "critical error %v")
	ErrResultUndetermined = ClassGlobal.New(CodeResultUndetermined, "execution result undetermined")
)

func init() {
	defaultMySQLErrorCode = mysql.ErrUnknown
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
		return te1.class == te2.class && te1.code == te2.code
	}

	return e1.Error() == e2.Error()
}

// ErrorNotEqual returns a boolean indicating whether err1 isn't equal to err2.
func ErrorNotEqual(err1, err2 error) bool {
	return !ErrorEqual(err1, err2)
}

// MustNil cleans up and fatals if err is not nil.
func MustNil(err error, closeFuns ...func()) {
	if err != nil {
		for _, f := range closeFuns {
			f()
		}
		log.Fatal("unexpected error", zap.Error(err), zap.Stack("stack"))
	}
}

// Call executes a function and checks the returned err.
func Call(fn func() error) {
	err := fn()
	if err != nil {
		log.Error("function call errored", zap.Error(err), zap.Stack("stack"))
	}
}

// Log logs the error if it is not nil.
func Log(err error) {
	if err != nil {
		log.Error("encountered error", zap.Error(err), zap.Stack("stack"))
	}
}

// ExportErrorCodeAndWorkaround is used to produce error workaround.
func ExportErrorCodeAndWorkaround(fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	for code, e := range errCodeMap {
		workaround := fmt.Sprintf(
			"[error.%v]\nerror = '''%v'''\nworkaround = '''%v'''\n\n",
			code, e.message, e.workaround)
		_, err = file.WriteString(workaround)
		if err != nil {
			return err
		}
	}
	return nil
}

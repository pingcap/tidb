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
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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

// Error implements error interface.
type Error = errors.Error

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
var rfcCode2errClass = newCode2ErrClassMap()

type code2ErrClassMap struct {
	data sync.Map
}

func newCode2ErrClassMap() *code2ErrClassMap {
	return &code2ErrClassMap{
		data: sync.Map{},
	}
}

func (m *code2ErrClassMap) Get(key string) (ErrClass, bool) {
	ret, have := m.data.Load(key)
	if !have {
		return ErrClass(-1), false
	}
	return ret.(ErrClass), true
}

func (m *code2ErrClassMap) Put(key string, err ErrClass) {
	m.data.Store(key, err)
}

var registerFinish uint32

// RegisterFinish makes the register of new error panic.
// The use pattern should be register all the errors during initialization, and then call RegisterFinish.
func RegisterFinish() {
	atomic.StoreUint32(&registerFinish, 1)
}

func frozen() bool {
	return atomic.LoadUint32(&registerFinish) != 0
}

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
		rfcCode := te.RFCCode()
		if index := strings.Index(string(rfcCode), ":"); index > 0 {
			if class, has := rfcCode2errClass.Get(string(rfcCode)[:index]); has {
				return class == ec
			}
		}
	}
	return false
}

// NotEqualClass returns true if err is not *Error with the same class.
func (ec ErrClass) NotEqualClass(err error) bool {
	return !ec.EqualClass(err)
}

func (ec ErrClass) initError(code ErrCode) string {
	if frozen() {
		debug.PrintStack()
		panic("register error after initialized is prohibited")
	}
	clsMap, ok := ErrClassToMySQLCodes[ec]
	if !ok {
		clsMap = make(map[ErrCode]struct{})
		ErrClassToMySQLCodes[ec] = clsMap
	}
	clsMap[code] = struct{}{}
	class := errClass2Desc[ec]
	rfcCode := fmt.Sprintf("%s:%d", class, code)
	rfcCode2errClass.Put(class, ec)
	return rfcCode
}

// New defines an *Error with an error code and an error message.
// Usually used to create base *Error.
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
//
// Deprecated: use NewStd or NewStdErr instead.
func (ec ErrClass) New(code ErrCode, message string) *Error {
	rfcCode := ec.initError(code)
	err := errors.Normalize(message, errors.MySQLErrorCode(int(code)), errors.RFCCodeText(rfcCode))
	return err
}

// NewStdErr defines an *Error with an error code, an error
// message and workaround to create standard error.
func (ec ErrClass) NewStdErr(code ErrCode, message *mysql.ErrMessage) *Error {
	rfcCode := ec.initError(code)
	err := errors.Normalize(
		message.Raw, errors.RedactArgs(message.RedactArgPos),
		errors.MySQLErrorCode(int(code)), errors.RFCCodeText(rfcCode),
	)
	return err
}

// NewStd calls New using the standard message for the error code
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) NewStd(code ErrCode) *Error {
	return ec.NewStdErr(code, mysql.MySQLErrName[uint16(code)])
}

// Synthesize synthesizes an *Error in the air
// it didn't register error into ErrClassToMySQLCodes
// so it's goroutine-safe
// and often be used to create Error came from other systems like TiKV.
func (ec ErrClass) Synthesize(code ErrCode, message string) *Error {
	return errors.Normalize(
		message, errors.MySQLErrorCode(int(code)),
		errors.RFCCodeText(fmt.Sprintf("%s:%d", errClass2Desc[ec], code)),
	)
}

// ToSQLError convert Error to mysql.SQLError.
func ToSQLError(e *Error) *mysql.SQLError {
	code := getMySQLErrorCode(e)
	return mysql.NewErrf(code, "%s", nil, e.GetMsg())
}

var defaultMySQLErrorCode uint16

func getMySQLErrorCode(e *Error) uint16 {
	rfcCode := e.RFCCode()
	var class ErrClass
	if index := strings.Index(string(rfcCode), ":"); index > 0 {
		ec, has := rfcCode2errClass.Get(string(rfcCode)[:index])
		if !has {
			log.Warn("Unknown error class", zap.String("class", string(rfcCode)[:index]))
			return defaultMySQLErrorCode
		}
		class = ec
	}
	codeMap, ok := ErrClassToMySQLCodes[class]
	if !ok {
		log.Warn("Unknown error class", zap.Int("class", int(class)))
		return defaultMySQLErrorCode
	}
	_, ok = codeMap[ErrCode(e.Code())]
	if !ok {
		log.Debug("Unknown error code", zap.Int("class", int(class)), zap.Int("code", int(e.Code())))
		return defaultMySQLErrorCode
	}
	return uint16(e.Code())
}

var (
	// ErrClassToMySQLCodes is the map of ErrClass to code-set.
	ErrClassToMySQLCodes = make(map[ErrClass]map[ErrCode]struct{})
	// ErrCritical is the critical error class.
	ErrCritical = ClassGlobal.NewStdErr(CodeExecResultIsEmpty, mysql.Message("critical error %v", nil))
	// ErrResultUndetermined is the error when execution result is unknown.
	ErrResultUndetermined = ClassGlobal.NewStdErr(
		CodeResultUndetermined,
		mysql.Message("execution result undetermined", nil),
	)
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
		return te1.RFCCode() == te2.RFCCode()
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

// GetErrClass returns the error class of the error.
func GetErrClass(e *Error) ErrClass {
	rfcCode := e.RFCCode()
	if index := strings.Index(string(rfcCode), ":"); index > 0 {
		if class, has := rfcCode2errClass.Get(string(rfcCode)[:index]); has {
			return class
		}
	}
	return ErrClass(-1)
}

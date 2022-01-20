// Copyright 2021 PingCAP, Inc.
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
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestErrCode(t *testing.T) {
	require.Equal(t, ErrCode(1), CodeMissConnectionID)
	require.Equal(t, ErrCode(2), CodeResultUndetermined)
}

func TestTError(t *testing.T) {
	require.NotEmpty(t, ClassParser.String())
	require.NotEmpty(t, ClassOptimizer.String())
	require.NotEmpty(t, ClassKV.String())
	require.NotEmpty(t, ClassServer.String())

	parserErr := ClassParser.New(ErrCode(100), "error 100")
	require.NotEmpty(t, parserErr.Error())
	require.True(t, ClassParser.EqualClass(parserErr))
	require.False(t, ClassParser.NotEqualClass(parserErr))

	require.False(t, ClassOptimizer.EqualClass(parserErr))
	optimizerErr := ClassOptimizer.New(ErrCode(2), "abc")
	require.False(t, ClassOptimizer.EqualClass(errors.New("abc")))
	require.False(t, ClassOptimizer.EqualClass(nil))
	require.True(t, optimizerErr.Equal(optimizerErr.GenWithStack("def")))
	require.False(t, optimizerErr.Equal(nil))
	require.False(t, optimizerErr.Equal(errors.New("abc")))

	// Test case for FastGen.
	require.True(t, optimizerErr.Equal(optimizerErr.FastGen("def")))
	require.True(t, optimizerErr.Equal(optimizerErr.FastGen("def: %s", "def")))
	kvErr := ClassKV.New(1062, "key already exist")
	e := kvErr.FastGen("Duplicate entry '%d' for key 'PRIMARY'", 1)
	require.Equal(t, "[kv:1062]Duplicate entry '1' for key 'PRIMARY'", e.Error())
	sqlErr := ToSQLError(errors.Cause(e).(*Error))
	require.Equal(t, "Duplicate entry '1' for key 'PRIMARY'", sqlErr.Message)
	require.Equal(t, uint16(1062), sqlErr.Code)

	err := errors.Trace(ErrCritical.GenWithStackByArgs("test"))
	require.True(t, ErrCritical.Equal(err))

	err = errors.Trace(ErrCritical)
	require.True(t, ErrCritical.Equal(err))
}

func TestJson(t *testing.T) {
	prevTErr := errors.Normalize("json test", errors.MySQLErrorCode(int(CodeExecResultIsEmpty)))
	buf, err := json.Marshal(prevTErr)
	require.NoError(t, err)
	var curTErr errors.Error
	err = json.Unmarshal(buf, &curTErr)
	require.NoError(t, err)
	isEqual := prevTErr.Equal(&curTErr)
	require.True(t, isEqual)
}

var predefinedErr = ClassExecutor.New(ErrCode(123), "predefiend error")

func example() error {
	err := call()
	return errors.Trace(err)
}

func call() error {
	return predefinedErr.GenWithStack("error message:%s", "abc")
}

func TestErrorEqual(t *testing.T) {
	e1 := errors.New("test error")
	require.NotNil(t, e1)

	e2 := errors.Trace(e1)
	require.NotNil(t, e2)

	e3 := errors.Trace(e2)
	require.NotNil(t, e3)

	require.Equal(t, e1, errors.Cause(e2))
	require.Equal(t, e1, errors.Cause(e3))
	require.Equal(t, errors.Cause(e3), errors.Cause(e2))

	e4 := errors.New("test error")
	require.NotEqual(t, e1, errors.Cause(e4))

	e5 := errors.Errorf("test error")
	require.NotEqual(t, e1, errors.Cause(e5))

	require.True(t, ErrorEqual(e1, e2))
	require.True(t, ErrorEqual(e1, e3))
	require.True(t, ErrorEqual(e1, e4))
	require.True(t, ErrorEqual(e1, e5))

	var e6 error

	require.True(t, ErrorEqual(nil, nil))
	require.True(t, ErrorNotEqual(e1, e6))
	code1 := ErrCode(9001)
	code2 := ErrCode(9002)
	te1 := ClassParser.Synthesize(code1, "abc")
	te3 := ClassKV.New(code1, "abc")
	te4 := ClassKV.New(code2, "abc")
	require.False(t, ErrorEqual(te1, te3))
	require.False(t, ErrorEqual(te3, te4))
}

func TestLog(t *testing.T) {
	err := fmt.Errorf("xxx")
	Log(err)
}

func TestTraceAndLocation(t *testing.T) {
	err := example()
	stack := errors.ErrorStack(err)
	lines := strings.Split(stack, "\n")
	goroot := strings.ReplaceAll(runtime.GOROOT(), string(os.PathSeparator), "/")
	var sysStack = 0
	for _, line := range lines {
		if strings.Contains(line, goroot) {
			sysStack++
		}
	}
	require.Equalf(t, 11, len(lines)-(2*sysStack), "stack =\n%s", stack)
	var containTerr bool
	for _, v := range lines {
		if strings.Contains(v, "terror_test.go") {
			containTerr = true
			break
		}
	}
	require.True(t, containTerr)
}

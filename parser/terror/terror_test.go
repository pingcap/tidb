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
	"runtime"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testTErrorSuite{})

type testTErrorSuite struct {
}

func (s *testTErrorSuite) TestErrCode(c *C) {
	c.Assert(CodeMissConnectionID, Equals, ErrCode(1))
	c.Assert(CodeResultUndetermined, Equals, ErrCode(2))
}

func (s *testTErrorSuite) TestTError(c *C) {
	c.Assert(ClassParser.String(), Not(Equals), "")
	c.Assert(ClassOptimizer.String(), Not(Equals), "")
	c.Assert(ClassKV.String(), Not(Equals), "")
	c.Assert(ClassServer.String(), Not(Equals), "")

	parserErr := ClassParser.New(ErrCode(100), "error 100")
	c.Assert(parserErr.Error(), Not(Equals), "")
	c.Assert(ClassParser.EqualClass(parserErr), IsTrue)
	c.Assert(ClassParser.NotEqualClass(parserErr), IsFalse)

	c.Assert(ClassOptimizer.EqualClass(parserErr), IsFalse)
	optimizerErr := ClassOptimizer.New(ErrCode(2), "abc")
	c.Assert(ClassOptimizer.EqualClass(errors.New("abc")), IsFalse)
	c.Assert(ClassOptimizer.EqualClass(nil), IsFalse)
	c.Assert(optimizerErr.Equal(optimizerErr.GenWithStack("def")), IsTrue)
	c.Assert(optimizerErr.Equal(nil), IsFalse)
	c.Assert(optimizerErr.Equal(errors.New("abc")), IsFalse)

	// Test case for FastGen.
	c.Assert(optimizerErr.Equal(optimizerErr.FastGen("def")), IsTrue)
	c.Assert(optimizerErr.Equal(optimizerErr.FastGen("def: %s", "def")), IsTrue)
	kvErr := ClassKV.New(1062, "key already exist")
	e := kvErr.FastGen("Duplicate entry '%d' for key 'PRIMARY'", 1)
	c.Assert(e.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'PRIMARY'")
	sqlErr := ToSQLError(errors.Cause(e).(*Error))
	c.Assert(sqlErr.Message, Equals, "Duplicate entry '1' for key 'PRIMARY'")
	c.Assert(sqlErr.Code, Equals, uint16(1062))

	err := errors.Trace(ErrCritical.GenWithStackByArgs("test"))
	c.Assert(ErrCritical.Equal(err), IsTrue)

	err = errors.Trace(ErrCritical)
	c.Assert(ErrCritical.Equal(err), IsTrue)
}

func (s *testTErrorSuite) TestJson(c *C) {
	prevTErr := errors.Normalize("json test", errors.MySQLErrorCode(int(CodeExecResultIsEmpty)))
	buf, err := json.Marshal(prevTErr)
	c.Assert(err, IsNil)
	var curTErr errors.Error
	err = json.Unmarshal(buf, &curTErr)
	c.Assert(err, IsNil)
	isEqual := prevTErr.Equal(&curTErr)
	c.Assert(isEqual, IsTrue)
}

var predefinedErr = ClassExecutor.New(ErrCode(123), "predefiend error")

func example() error {
	err := call()
	return errors.Trace(err)
}

func call() error {
	return predefinedErr.GenWithStack("error message:%s", "abc")
}

func (s *testTErrorSuite) TestTraceAndLocation(c *C) {
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
	c.Assert(len(lines)-(2*sysStack), Equals, 15, Commentf("stack =\n%s", stack))
	var containTerr bool
	for _, v := range lines {
		if strings.Contains(v, "terror_test.go") {
			containTerr = true
			break
		}
	}
	c.Assert(containTerr, IsTrue)
}

func (s *testTErrorSuite) TestErrorEqual(c *C) {
	e1 := errors.New("test error")
	c.Assert(e1, NotNil)

	e2 := errors.Trace(e1)
	c.Assert(e2, NotNil)

	e3 := errors.Trace(e2)
	c.Assert(e3, NotNil)

	c.Assert(errors.Cause(e2), Equals, e1)
	c.Assert(errors.Cause(e3), Equals, e1)
	c.Assert(errors.Cause(e2), Equals, errors.Cause(e3))

	e4 := errors.New("test error")
	c.Assert(errors.Cause(e4), Not(Equals), e1)

	e5 := errors.Errorf("test error")
	c.Assert(errors.Cause(e5), Not(Equals), e1)

	c.Assert(ErrorEqual(e1, e2), IsTrue)
	c.Assert(ErrorEqual(e1, e3), IsTrue)
	c.Assert(ErrorEqual(e1, e4), IsTrue)
	c.Assert(ErrorEqual(e1, e5), IsTrue)

	var e6 error

	c.Assert(ErrorEqual(nil, nil), IsTrue)
	c.Assert(ErrorNotEqual(e1, e6), IsTrue)
	code1 := ErrCode(9001)
	code2 := ErrCode(9002)
	te1 := ClassParser.Synthesize(code1, "abc")
	te3 := ClassKV.New(code1, "abc")
	te4 := ClassKV.New(code2, "abc")
	c.Assert(ErrorEqual(te1, te3), IsFalse)
	c.Assert(ErrorEqual(te3, te4), IsFalse)
}

func (s *testTErrorSuite) TestLog(c *C) {
	err := fmt.Errorf("xxx")
	Log(err)
}

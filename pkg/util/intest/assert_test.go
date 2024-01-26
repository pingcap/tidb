// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package intest_test

import (
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

type foo struct{}

func TestAssert(t *testing.T) {
	require.True(t, intest.InTest)
	checkAssert(t, true, true)
	checkAssert(t, false, false)
	checkAssert(t, false, false, "assert failed, msg1", "msg1")
	checkAssert(t, false, false, "assert failed, msg2 a b 1", "msg2 %s %s %d", "a", "b", 1)
	checkAssert(t, false, false, "assert failed, 123", 123)
	checkAssertNotNil(t, "", true)
	checkAssertNotNil(t, "abc", true)
	checkAssertNotNil(t, 0, true)
	checkAssertNotNil(t, 123, true)
	checkAssertNotNil(t, foo{}, true)
	checkAssertNotNil(t, &foo{}, true)
	checkAssertNotNil(t, nil, false)
	checkAssertNotNil(t, true, true)
	checkAssertNotNil(t, false, true)
	var f *foo
	checkAssertNotNil(t, f, false)
	checkAssertNotNil(t, func() bool { return true }, true)
	checkAssertNotNil(t, func() bool { return false }, true)
	checkAssertNotNil(t, func(_ string) bool { return true }, true)
	checkAssertNotNil(t, nil, false, "assert failed, msg1", "msg1")
	checkAssertNotNil(t, nil, false, "assert failed, msg2 a b 1", "msg2 %s %s %d", "a", "b", 1)
	checkFuncAssert(t, func() bool { panic("inner panic1") }, false, "inner panic1")
	checkFuncAssert(t, func() bool { return true }, true)
	checkFuncAssert(t, func() bool { return false }, false)
	checkFuncAssert(t, func() bool { return false }, false, "assert failed, msg3", "msg3")
	checkFuncAssert(t, func() bool { return false }, false, "assert failed, msg4 c d 2", "msg4 %s %s %d", "c", "d", 2)
	checkFuncAssert(t, func() bool { panic("inner panic2") }, false, "inner panic2")
	checkFuncAssert(t, nil, false)
	checkAssertNoError(t, nil, true)
	checkAssertNoError(t, errors.New("mock err1"), false, "assert failed, error is not nil: mock err1")
	var err error
	checkAssertNoError(t, err, true)
}

func checkFuncAssert(t *testing.T, fn func() bool, pass bool, msgAndArgs ...any) {
	doCheckAssert(t, intest.AssertFunc, fn, pass, msgAndArgs...)
}

func checkAssert(t *testing.T, cond bool, pass bool, msgAndArgs ...any) {
	doCheckAssert(t, intest.Assert, cond, pass, msgAndArgs...)
}

func checkAssertNotNil(t *testing.T, obj any, pass bool, msgAndArgs ...any) {
	doCheckAssert(t, intest.AssertNotNil, obj, pass, msgAndArgs...)
}

func checkAssertNoError(t *testing.T, err error, pass bool, msgAndArgs ...any) {
	doCheckAssert(t, intest.AssertNoError, err, pass, msgAndArgs...)
}

func doCheckAssert(t *testing.T, fn any, cond any, pass bool, msgAndArgs ...any) {
	expectMsg := "assert failed"
	if len(msgAndArgs) > 0 {
		expectMsg = msgAndArgs[0].(string)
		msgAndArgs = msgAndArgs[1:]
	}

	fail := ""
	onlyCheckPrefix := false
	if !pass {
		defer func() {
			if fail != "" {
				require.FailNow(t, fail)
			}
			r := recover()
			require.NotNil(t, r)
			if onlyCheckPrefix {
				require.True(t, strings.HasPrefix(r.(string), expectMsg), "%q\nshould have prefix \n%q", r, expectMsg)
				require.Contains(t, r.(string), expectMsg)
			} else {
				require.Equal(t, expectMsg, r)
			}
		}()
	}

	switch assertFn := fn.(type) {
	case func(bool, ...any):
		assertFn(cond.(bool), msgAndArgs...)
	case func(any, ...any):
		assertFn(cond, msgAndArgs...)
	case func(func() bool, ...any):
		assertFn(cond.(func() bool), msgAndArgs...)
	case func(error, ...any):
		if cond == nil {
			assertFn(nil, msgAndArgs...)
		} else {
			onlyCheckPrefix = true
			assertFn(cond.(error), msgAndArgs...)
		}
	default:
		fail = "invalid input assert function"
	}
}

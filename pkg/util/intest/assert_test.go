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
	"testing"

	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

type foo struct{}

func TestAssert(t *testing.T) {
	require.True(t, intest.InTest)
	checkAssert(t, true, true)
	checkAssert(t, "", true)
	checkAssert(t, "abc", true)
	checkAssert(t, 0, true)
	checkAssert(t, 123, true)
	checkAssert(t, foo{}, true)
	checkAssert(t, &foo{}, true)
	checkAssert(t, false, false)
	checkAssert(t, false, false, "assert failed: msg1", "msg1")
	checkAssert(t, false, false, "assert failed: msg2 a b 1", "msg2 %s %s %d", "a", "b", 1)
	checkAssert(t, false, false, "assert failed: 123", 123)
	checkAssert(t, nil, false)
	var f *foo
	checkAssert(t, f, false)
	checkAssert(t, func() bool { return true }, false, "you should use `intest.Assert(fn != nil)` to assert a function is not nil, or use `intest.AssertFunc(fn)` to assert a function's return value is true")
	checkAssert(t, func(_ string) bool { return true }, false, "you should use `intest.Assert(fn != nil)` to assert a function is not nil, or use `intest.AssertFunc(fn)` to assert a function's return value is true")
	checkFuncAssert(t, func() bool { panic("inner panic1") }, false, "inner panic1")
	checkFuncAssert(t, func() bool { return true }, true)
	checkFuncAssert(t, func() bool { return false }, false)
	checkFuncAssert(t, func() bool { return false }, false, "assert failed: msg3", "msg3")
	checkFuncAssert(t, func() bool { return false }, false, "assert failed: msg4 c d 2", "msg4 %s %s %d", "c", "d", 2)
	checkFuncAssert(t, func() bool { panic("inner panic2") }, false, "inner panic2")
}

func checkFuncAssert(t *testing.T, fn func() bool, pass bool, msgAndArgs ...any) {
	doCheckAssert(t, intest.AssertFunc, fn, pass, msgAndArgs...)
}

func checkAssert(t *testing.T, cond any, pass bool, msgAndArgs ...any) {
	doCheckAssert(t, intest.Assert, cond, pass, msgAndArgs...)
}

func doCheckAssert(t *testing.T, fn any, cond any, pass bool, msgAndArgs ...any) {
	expectMsg := "assert failed"
	if len(msgAndArgs) > 0 {
		expectMsg = msgAndArgs[0].(string)
		msgAndArgs = msgAndArgs[1:]
	}

	if !pass {
		defer func() {
			r := recover()
			require.NotNil(t, r)
			require.Equal(t, expectMsg, r)
		}()
	}

	testFn, ok := fn.(func(any, ...any))
	if !ok {
		if fnAssert, ok := fn.(func(func() bool, ...any)); ok {
			testFn = func(any, ...any) {
				fnAssert(cond.(func() bool), msgAndArgs...)
			}
		} else {
			require.FailNow(t, "invalid assert function")
		}
	}

	if len(msgAndArgs) == 0 {
		testFn(cond)
	}

	if len(msgAndArgs) == 1 {
		testFn(cond, msgAndArgs[0])
	}

	testFn(cond, msgAndArgs...)
}

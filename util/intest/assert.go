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

//go:build intest

package intest

import (
	"fmt"
	"reflect"
)

// Assert asserts a condition. It only works in test (intest.InTest == true).
// You can assert a condition like this to assert a variable `foo` is not nil: `assert.Assert(foo != nil)`.
// Or you can pass foo as a parameter directly for simple: `assert.Assert(foo)`
// You can also assert a function that returns a bool: `intest.Assert(func() bool { return foo != nil })`
// If you pass a function without a signature `func() bool`, the function will always panic.
func Assert(cond any, msgAndArgs ...any) {
	if InTest {
		assert(cond, msgAndArgs...)
	}
}

// AssertFunc asserts a function condition
func AssertFunc(fn func() bool, msgAndArgs ...any) {
	if InTest {
		assert(fn(), msgAndArgs...)
	}
}

func assert(cond any, msgAndArgs ...any) {
	if !checkAssertObject(cond) {
		doPanic(msgAndArgs...)
	}
}

func doPanic(msgAndArgs ...any) {
	panic(assertionFailedMsg(msgAndArgs...))
}

func assertionFailedMsg(msgAndArgs ...any) string {
	if len(msgAndArgs) == 0 {
		return "assert failed"
	}

	msg, ok := msgAndArgs[0].(string)
	if !ok {
		msg = fmt.Sprintf("%+v", msgAndArgs[0])
	}

	msg = fmt.Sprintf("assert failed: %s", msg)
	return fmt.Sprintf(msg, msgAndArgs[1:]...)
}

func checkAssertObject(obj any) bool {
	if obj == nil {
		return false
	}

	value := reflect.ValueOf(obj)
	switch value.Kind() {
	case reflect.Bool:
		return obj.(bool)
	case reflect.Func:
		panic("you should use `intest.Assert(fn != nil)` to assert a function is not nil, " +
			"or use `intest.AssertFunc(fn)` to assert a function's return value is true")
	case reflect.Chan, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		return !value.IsNil()
	default:
		return true
	}
}

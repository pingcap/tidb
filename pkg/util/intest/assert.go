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

// Assert asserts a condition is true
func Assert(cond bool, msgAndArgs ...any) {
	if InTest && !cond {
		doPanic("", msgAndArgs...)
	}
}

// AssertNoError asserts an error is nil
func AssertNoError(err error, msgAndArgs ...any) {
	if InTest && err != nil {
		doPanic(fmt.Sprintf("error is not nil: %+v", err), msgAndArgs...)
	}
}

// AssertNotNil asserts an object is not nil
func AssertNotNil(obj any, msgAndArgs ...any) {
	if InTest {
		Assert(obj != nil, msgAndArgs...)
		value := reflect.ValueOf(obj)
		switch value.Kind() {
		case reflect.Func, reflect.Chan, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
			Assert(!value.IsNil(), msgAndArgs...)
		}
	}
}

// AssertFunc asserts a function condition
func AssertFunc(fn func() bool, msgAndArgs ...any) {
	if InTest {
		Assert(fn != nil, msgAndArgs...)
		Assert(fn(), msgAndArgs...)
	}
}

func doPanic(extraMsg string, userMsgAndArgs ...any) {
	panic(assertionFailedMsg(extraMsg, userMsgAndArgs...))
}

func assertionFailedMsg(extraMsg string, userMsgAndArgs ...any) string {
	msg := "assert failed"
	if len(userMsgAndArgs) == 0 {
		if extraMsg != "" {
			msg = fmt.Sprintf("%s, %s", msg, extraMsg)
		}
		return msg
	}

	if len(userMsgAndArgs) == 0 {
		return fmt.Sprintf("assert failed, %s", extraMsg)
	}

	userMsg, ok := userMsgAndArgs[0].(string)
	if !ok {
		userMsg = fmt.Sprintf("%+v", userMsgAndArgs[0])
	}

	msg = fmt.Sprintf("%s, %s", msg, userMsg)
	if extraMsg != "" {
		msg = fmt.Sprintf("%s, %s", msg, extraMsg)
	}

	return fmt.Sprintf(msg, userMsgAndArgs[1:]...)
}

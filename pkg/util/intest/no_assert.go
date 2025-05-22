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

//go:build !intest && !enableassert

package intest

// EnableAssert checks if the code is running in integration test.
var EnableAssert = false

// Assert asserts a condition is true
func Assert(cond bool, msgAndArgs ...any) {
	if EnableInternalCheck {
		doAssert(cond, msgAndArgs...)
	}
}

// AssertNoError asserts an error is nil
func AssertNoError(err error, msgAndArgs ...any) {
	if EnableInternalCheck {
		doAssertNoError(err, msgAndArgs...)
	}
}

// AssertNotNil asserts an object is not nil
func AssertNotNil(obj any, msgAndArgs ...any) {
	if EnableInternalCheck {
		doAssertNotNil(obj, msgAndArgs...)
	}
}

// AssertFunc asserts a function condition
func AssertFunc(fn func() bool, msgAndArgs ...any) {
	if EnableInternalCheck {
		doAssertFunc(fn, msgAndArgs...)
	}
}

// Copyright 2024 PingCAP, Inc.
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

//go:build !intest

package intest

// InTest checks if the code is running in test.
var InTest = false

// TestStackTrace provides a way to capture the stack trace in tests.
type TestStackTrace struct {
}

// NewTestStackTrace creates a new TestStackTrace instance with the current stack trace.
func NewTestStackTrace() TestStackTrace {
	return TestStackTrace{}
}

// IsEmpty checks if the stack trace is empty.
func (t *TestStackTrace) IsEmpty() bool {
	return true
}

// String returns a string representation of the stack trace.
func (t *TestStackTrace) String() string {
	return "TestStackTrace: no stack trace available in non-test builds"
}

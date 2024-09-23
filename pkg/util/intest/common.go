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

//go:build !intest

package intest

// InTest checks if the code is running in test.
const InTest = false

// Assert is a stub function in release build.
// See the same function in `util/intest/assert.go` for the real implement in test.
func Assert(_ bool, _ ...any) {}

// AssertNotNil is a stub function in release build.
// See the same function in `util/intest/assert.go` for the real implement in test.
func AssertNotNil(_ any, _ ...any) {}

// AssertNoError is a stub function in release build.
// See the same function in `util/intest/assert.go` for the real implement in test.
func AssertNoError(_ error, _ ...any) {}

// AssertFunc is a stub function in release build.
// See the same function `util/intest/assert.go` for the real implement in test.
func AssertFunc(_ func() bool, _ ...any) {}

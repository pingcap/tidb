//go:build intest

// Copyright 2026 PingCAP, Inc.
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

package expression

import "strings"

// RegisterLoadableFunctionForTest marks a loadable function name as registered for intest builds.
func RegisterLoadableFunctionForTest(name string) {
	loadableFuncs.Store(strings.ToLower(name), &loadableFuncClass{})
}

// UnregisterLoadableFunctionForTest removes a loadable function name registered for tests.
func UnregisterLoadableFunctionForTest(name string) {
	loadableFuncs.Delete(strings.ToLower(name))
}

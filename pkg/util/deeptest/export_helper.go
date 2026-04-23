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

package deeptest

import (
	"reflect"

	"github.com/stretchr/testify/require"
)

// ExportStaticTestHelper exports staticTestHelper type for testing
type ExportStaticTestHelper = staticTestHelper

// ExportNewStaticTestHelper creates a new staticTestHelper for testing
func ExportNewStaticTestHelper() *ExportStaticTestHelper {
	return &staticTestHelper{}
}

// ExportAssertRecursivelyNotEqual exports assertRecursivelyNotEqual method for testing
func ExportAssertRecursivelyNotEqual(h *ExportStaticTestHelper, t require.TestingT, valA, valB reflect.Value, path string) {
	h.assertRecursivelyNotEqual(t, valA, valB, path)
}

// ExportAssertDeepClonedEqual exports assertDeepClonedEqual method for testing
func ExportAssertDeepClonedEqual(h *ExportStaticTestHelper, t require.TestingT, valA, valB reflect.Value, path string) {
	h.assertDeepClonedEqual(t, valA, valB, path)
}

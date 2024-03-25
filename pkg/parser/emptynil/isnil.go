// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emptynil

// IsNilSlice asserts whether the given slice is nil.
func IsNilSlice[T any](arr []T) bool {
	return arr == nil
}

// IsNilMap asserts whether the given map is nil.
func IsNilMap[K comparable, V any](m map[K]V) bool {
	return m == nil
}

// Copyright 2021 PingCAP, Inc.
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

package slice

import (
	"slices"
	"strconv"
)

// AllOf returns true if all elements in the slice match the predict func.
func AllOf[T any](s []T, p func(T) bool) bool {
	// Use the inverse of ContainsFunc with negated predicate
	// AllOf(s, p) is equivalent to !ContainsFunc(s, !p)
	return !slices.ContainsFunc(s, func(x T) bool {
		return !p(x)
	})
}

// Int64sToStrings converts a slice of int64 to a slice of string.
func Int64sToStrings(ints []int64) []string {
	strs := make([]string, len(ints))
	for i, v := range ints {
		strs[i] = strconv.FormatInt(v, 10)
	}
	return strs
}

// DeepClone uses Clone() to clone a slice.
// The elements in the slice must implement func (T) Clone() T.
func DeepClone[T interface{ Clone() T }](s []T) []T {
	if s == nil {
		return nil
	}
	cloned := make([]T, 0, len(s))
	for _, item := range s {
		cloned = append(cloned, item.Clone())
	}
	return cloned
}

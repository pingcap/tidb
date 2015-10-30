// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

// CompareUnorderedStringSlice compare two string slices.
// If a and b is exactly the same except the order, it returns true.
// In otherwise return false.
func CompareUnorderedStringSlice(a []string, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, i := range a {
		_, ok := m[i]
		if !ok {
			m[i] = 1
		} else {
			m[i]++
		}
	}

	for _, i := range b {
		_, ok := m[i]
		if !ok {
			return false
		}
		m[i]--
		if m[i] == 0 {
			delete(m, i)
		}
	}
	return len(m) == 0
}

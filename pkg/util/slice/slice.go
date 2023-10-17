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

import "reflect"

// AnyOf returns true if any element in the slice matches the predict func.
func AnyOf(s interface{}, p func(int) bool) bool {
	l := reflect.ValueOf(s).Len()
	for i := 0; i < l; i++ {
		if p(i) {
			return true
		}
	}
	return false
}

// NoneOf returns true if no element in the slice matches the predict func.
func NoneOf(s interface{}, p func(int) bool) bool {
	return !AnyOf(s, p)
}

// AllOf returns true if all elements in the slice match the predict func.
func AllOf(s interface{}, p func(int) bool) bool {
	np := func(i int) bool {
		return !p(i)
	}
	return NoneOf(s, np)
}

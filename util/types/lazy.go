// Copyright 2019 PingCAP, Inc.
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

package types

// LazyStr defines string type wil be lazy eval.
// implement fmt.Stringer
type LazyStr func() string

// String implements fmt.Stringer
func (l LazyStr) String() string {
	return l()
}

// InstantStr defines a alias to normal string.
// implement fmt.Stringer
type InstantStr string

// String implements fmt.Stringer
func (i InstantStr) String() string {
	return string(i)
}

// MemoizeStr returns memoized version of a lazy string.
func MemoizeStr(l LazyStr) LazyStr {
	var result string
	return func() string {
		if result != "" {
			return result
		}
		result = l()
		return result
	}
}

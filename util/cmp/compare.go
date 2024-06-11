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

package cmp

import "golang.org/x/exp/constraints"

// Compare returns -1 if x is less than y, 0 if x equals y, and +1 if x is greater than y.
func Compare[T constraints.Ordered](x, y T) int {
	if x < y {
		return -1
	}
	if x > y {
		return +1
	}
	return 0
}

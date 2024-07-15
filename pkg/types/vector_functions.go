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

package types

// Compare returns an integer comparing two vectors. The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func (a VectorFloat32) Compare(b VectorFloat32) int {
	la := a.Len()
	lb := b.Len()
	commonLen := la
	if lb < commonLen {
		commonLen = lb
	}

	va := a.Elements()
	vb := b.Elements()

	for i := 0; i < commonLen; i++ {
		if va[i] < vb[i] {
			return -1
		} else if va[i] > vb[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	} else if la > lb {
		return 1
	}
	return 0
}

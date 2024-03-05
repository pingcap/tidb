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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"cmp"
	"math"

	"github.com/pingcap/tidb/pkg/util/collate"
)

// VecCompareUU returns []int64 comparing the []uint64 x to []uint64 y
func VecCompareUU(x, y []uint64, res []int64) {
	n := len(x)
	for i := 0; i < n; i++ {
		if x[i] < y[i] {
			res[i] = -1
		} else if x[i] == y[i] {
			res[i] = 0
		} else {
			res[i] = 1
		}
	}
}

// VecCompareII returns []int64 comparing the []int64 x to []int64 y
func VecCompareII(x, y, res []int64) {
	n := len(x)
	for i := 0; i < n; i++ {
		if x[i] < y[i] {
			res[i] = -1
		} else if x[i] == y[i] {
			res[i] = 0
		} else {
			res[i] = 1
		}
	}
}

// VecCompareUI returns []int64 comparing the []uint64 x to []int64y
func VecCompareUI(x []uint64, y, res []int64) {
	n := len(x)
	for i := 0; i < n; i++ {
		if y[i] < 0 || x[i] > math.MaxInt64 {
			res[i] = 1
		} else if int64(x[i]) < y[i] {
			res[i] = -1
		} else if int64(x[i]) == y[i] {
			res[i] = 0
		} else {
			res[i] = 1
		}
	}
}

// VecCompareIU returns []int64 comparing the []int64 x to []uint64y
func VecCompareIU(x []int64, y []uint64, res []int64) {
	n := len(x)
	for i := 0; i < n; i++ {
		if x[i] < 0 || y[i] > math.MaxInt64 {
			res[i] = -1
		} else if x[i] < int64(y[i]) {
			res[i] = -1
		} else if x[i] == int64(y[i]) {
			res[i] = 0
		} else {
			res[i] = 1
		}
	}
}

// CompareString returns an integer comparing the string x to y with the specified collation and length.
func CompareString(x, y, collation string) int {
	return collate.GetCollator(collation).Compare(x, y)
}

// CompareInt return an integer comparing the integer x to y with signed or unsigned.
func CompareInt(arg0 int64, isUnsigned0 bool, arg1 int64, isUnsigned1 bool) int {
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = cmp.Compare(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || uint64(arg0) > math.MaxInt64 {
			res = 1
		} else {
			res = cmp.Compare(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || uint64(arg1) > math.MaxInt64 {
			res = -1
		} else {
			res = cmp.Compare(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = cmp.Compare(arg0, arg1)
	}
	return res
}

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

package types

import (
	"math"
	"time"

	"github.com/pingcap/tidb/util/collate"
)

// CompareInt64 returns an integer comparing the int64 x to y.
func CompareInt64(x, y int64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareUint64 returns an integer comparing the uint64 x to y.
func CompareUint64(x, y uint64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

//VecCompareUU returns []int64 comparing the []uint64 x to []uint64 y
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

//VecCompareII returns []int64 comparing the []int64 x to []int64 y
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

//VecCompareUI returns []int64 comparing the []uint64 x to []int64y
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

//VecCompareIU returns []int64 comparing the []int64 x to []uint64y
func VecCompareIU(x []int64, y []uint64, res []int64) {
	n := len(x)
	for i := 0; i < n; i++ {
		if x[i] < 0 || uint64(y[i]) > math.MaxInt64 {
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

// CompareFloat64 returns an integer comparing the float64 x to y.
func CompareFloat64(x, y float64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareString returns an integer comparing the string x to y with the specified collation and length.
func CompareString(x, y, collation string) int {
	return collate.GetCollator(collation).Compare(x, y)
}

// CompareDuration returns an integer comparing the duration x to y.
func CompareDuration(x, y time.Duration) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

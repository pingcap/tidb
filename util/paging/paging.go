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

package paging

import "math"

// A paging request may be separated into multi requests if there are more data than a page.
// The paging size grows from min to max, it's not well tuned yet.
// e.g. a paging request scans over range (r1, r200), it requires 64 rows in the first batch,
// if it's not drained, then the paging size grows, the new range is calculated like (r100, r200), then send a request again.
// Compare with the common unary request, paging request allows early access of data, it offers a streaming-like way processing data.
// TODO: may make the paging parameters configurable.
const (
	MinPagingSize      uint64 = 64
	maxPagingSizeShift        = 7
	pagingSizeGrow            = 2
	MaxPagingSize             = MinPagingSize << maxPagingSizeShift
	pagingGrowingSum          = ((2 << maxPagingSizeShift) - 1) * MinPagingSize
	Threshold          uint64 = 960
)

// GrowPagingSize grows the paging size and ensures it does not exceed MaxPagingSize
func GrowPagingSize(size uint64) uint64 {
	size <<= 1
	if size > MaxPagingSize {
		return MaxPagingSize
	}
	return size
}

// CalculateSeekCnt calculates the seek count from expect count
func CalculateSeekCnt(expectCnt uint64) float64 {
	if expectCnt == 0 {
		return 0
	}
	if expectCnt > pagingGrowingSum {
		// if the expectCnt is larger than pagingGrowingSum, calculate the seekCnt for the excess.
		return float64(8 + (expectCnt-pagingGrowingSum+MaxPagingSize-1)/MaxPagingSize)
	}
	if expectCnt > MinPagingSize {
		// if the expectCnt is less than pagingGrowingSum,
		// calculate the seekCnt(number of terms) from the sum of a geometric progression.
		// expectCnt = minPagingSize * (pagingSizeGrow ^ seekCnt - 1) / (pagingSizeGrow - 1)
		// simplify (pagingSizeGrow ^ seekCnt - 1) to pagingSizeGrow ^ seekCnt, we can infer that
		// seekCnt = log((pagingSizeGrow - 1) * expectCnt / minPagingSize) / log(pagingSizeGrow)
		return 1 + float64(int(math.Log(float64((pagingSizeGrow-1)*expectCnt)/float64(MinPagingSize))/math.Log(float64(pagingSizeGrow))))
	}
	return 1
}

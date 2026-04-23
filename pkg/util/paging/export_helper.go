// Copyright 2023 PingCAP, Inc.
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

// ExportedGrowPagingSize is exported version of GrowPagingSize
func ExportedGrowPagingSize(size uint64, maxv uint64) uint64 {
	return GrowPagingSize(size, maxv)
}

// ExportedMinPagingSize is exported version of MinPagingSize constant
const ExportedMinPagingSize = MinPagingSize

// ExportedMinAllowedMaxPagingSize is exported version of MinAllowedMaxPagingSize constant
const ExportedMinAllowedMaxPagingSize = MinAllowedMaxPagingSize

// ExportedPagingSizeGrow is exported version of pagingSizeGrow constant
const ExportedPagingSizeGrow = pagingSizeGrow

// ExportedPagingGrowingSum is exported version of pagingGrowingSum constant
const ExportedPagingGrowingSum = pagingGrowingSum

// ExportedMaxPagingSizeShift is exported version of maxPagingSizeShift constant
const ExportedMaxPagingSizeShift = maxPagingSizeShift

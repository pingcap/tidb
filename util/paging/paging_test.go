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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrowPagingSize(t *testing.T) {
	require.Equal(t, GrowPagingSize(MinPagingSize), MinPagingSize*pagingSizeGrow)
	require.Equal(t, GrowPagingSize(MaxPagingSize), MaxPagingSize)
	require.Equal(t, GrowPagingSize(MaxPagingSize/pagingSizeGrow+1), MaxPagingSize)
}

func TestCalculateSeekCnt(t *testing.T) {
	require.InDelta(t, CalculateSeekCnt(0), 0, 0.1)
	require.InDelta(t, CalculateSeekCnt(1), 1, 0.1)
	require.InDelta(t, CalculateSeekCnt(MinPagingSize), 1, 0.1)
	require.InDelta(t, CalculateSeekCnt(pagingGrowingSum), maxPagingSizeShift+1, 0.1)
	require.InDelta(t, CalculateSeekCnt(pagingGrowingSum+1), maxPagingSizeShift+2, 0.1)
	require.InDelta(t, CalculateSeekCnt(pagingGrowingSum+MaxPagingSize), maxPagingSizeShift+2, 0.1)
}

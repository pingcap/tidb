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

package paging_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/stretchr/testify/require"
)

func RunGrowPagingSize(t *testing.T) {
	require.Equal(t, paging.ExportedGrowPagingSize(paging.ExportedMinPagingSize, paging.ExportedMinAllowedMaxPagingSize), paging.ExportedMinPagingSize*paging.ExportedPagingSizeGrow)
	require.Equal(t, paging.ExportedGrowPagingSize(paging.ExportedMinAllowedMaxPagingSize, paging.ExportedMinAllowedMaxPagingSize), uint64(paging.ExportedMinAllowedMaxPagingSize))
	require.Equal(t, paging.ExportedGrowPagingSize(paging.ExportedMinAllowedMaxPagingSize/paging.ExportedPagingSizeGrow+1, paging.ExportedMinAllowedMaxPagingSize), uint64(paging.ExportedMinAllowedMaxPagingSize))
}

func RunCalculateSeekCnt(t *testing.T) {
	require.InDelta(t, paging.CalculateSeekCnt(0), 0, 0.1)
	require.InDelta(t, paging.CalculateSeekCnt(1), 1, 0.1)
	require.InDelta(t, paging.CalculateSeekCnt(paging.ExportedMinPagingSize), 1, 0.1)
	require.InDelta(t, paging.CalculateSeekCnt(paging.ExportedPagingGrowingSum), paging.ExportedMaxPagingSizeShift+1, 0.1)
	require.InDelta(t, paging.CalculateSeekCnt(paging.ExportedPagingGrowingSum+1), paging.ExportedMaxPagingSizeShift+2, 0.1)
	require.InDelta(t, paging.CalculateSeekCnt(paging.ExportedPagingGrowingSum+paging.ExportedMinAllowedMaxPagingSize), paging.ExportedMaxPagingSizeShift+2, 0.1)
}

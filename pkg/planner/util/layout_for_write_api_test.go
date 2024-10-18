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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinarySearchOfColPosInfoSlice(t *testing.T) {
	tblColPosInfos := TblColPosInfoSlice{
		{
			Start: 0,
		},
		{
			Start: 6,
		},
		{
			Start: 3,
		},
	}

	tblColPosInfos.SortByStart()
	require.Equal(t, 0, tblColPosInfos[0].Start)
	require.Equal(t, 3, tblColPosInfos[1].Start)
	require.Equal(t, 6, tblColPosInfos[2].Start)

	idx, ok := tblColPosInfos.FindTblIdx(0)
	require.True(t, ok)
	require.Equal(t, 0, idx)

	idx, ok = tblColPosInfos.FindTblIdx(1)
	require.True(t, ok)
	require.Equal(t, 0, idx)

	idx, ok = tblColPosInfos.FindTblIdx(3)
	require.True(t, ok)
	require.Equal(t, 1, idx)

	idx, ok = tblColPosInfos.FindTblIdx(5)
	require.True(t, ok)
	require.Equal(t, 1, idx)

	idx, ok = tblColPosInfos.FindTblIdx(6)
	require.True(t, ok)
	require.Equal(t, 2, idx)

	idx, ok = tblColPosInfos.FindTblIdx(7)
	require.True(t, ok)
	require.Equal(t, 2, idx)
	idx, ok = tblColPosInfos.FindTblIdx(100)
	require.True(t, ok)
	require.Equal(t, 2, idx)

	_, ok = tblColPosInfos.FindTblIdx(-1)
	require.False(t, ok)
}

// Copyright 2026 PingCAP, Inc.
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

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreIDRange(t *testing.T) {
	require.Equal(t, []uint64{1, 2, 3, 4}, StoreIDRange(1, 4))
	require.Nil(t, StoreIDRange(1, 0))
}

func TestAddRoundRobinRegions(t *testing.T) {
	stores := StoreIDRange(1, 3)
	boundaries, err := BuildRegionLayout(
		AddRoundRobinRegions(5, stores...),
	)
	require.NoError(t, err)
	require.Len(t, boundaries, 5)

	require.Empty(t, boundaries[0].StartKey)
	require.Equal(t, []byte("k01"), boundaries[0].EndKey)
	require.EqualValues(t, 1, boundaries[0].StoreID)

	require.Equal(t, []byte("k04"), boundaries[4].StartKey)
	require.Empty(t, boundaries[4].EndKey)
	require.EqualValues(t, 2, boundaries[4].StoreID)
}

func TestAddRegionsBySplitKeys(t *testing.T) {
	boundaries, err := BuildRegionLayout(
		AddRegionsBySplitKeys([]string{"a", "d"}, 10, 11),
	)
	require.NoError(t, err)
	require.Len(t, boundaries, 3)

	require.Equal(t, []byte(""), boundaries[0].StartKey)
	require.Equal(t, []byte("a"), boundaries[0].EndKey)
	require.EqualValues(t, 10, boundaries[0].StoreID)

	require.Equal(t, []byte("a"), boundaries[1].StartKey)
	require.Equal(t, []byte("d"), boundaries[1].EndKey)
	require.EqualValues(t, 11, boundaries[1].StoreID)

	require.Equal(t, []byte("d"), boundaries[2].StartKey)
	require.Empty(t, boundaries[2].EndKey)
	require.EqualValues(t, 10, boundaries[2].StoreID)
}

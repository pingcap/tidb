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

package domain

import (
	"testing"

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestStoreMergeEmptyRegionsMinTableIDIfUnchanged(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	do := &Domain{store: store}

	minTableID, err := do.loadOrInitMergeEmptyRegionsMinTableID()
	require.NoError(t, err)
	require.Equal(t, mergeEmptyRegionsInitMinTableID, minTableID)

	updated, err := do.storeMergeEmptyRegionsMinTableIDIfUnchanged(minTableID, 42)
	require.NoError(t, err)
	require.True(t, updated)

	minTableID, err = do.loadOrInitMergeEmptyRegionsMinTableID()
	require.NoError(t, err)
	require.Equal(t, int64(42), minTableID)

	require.NoError(t, do.ResetMergeEmptyRegionsMinTableID())

	updated, err = do.storeMergeEmptyRegionsMinTableIDIfUnchanged(42, 84)
	require.NoError(t, err)
	require.False(t, updated)

	minTableID, err = do.loadOrInitMergeEmptyRegionsMinTableID()
	require.NoError(t, err)
	require.Equal(t, mergeEmptyRegionsInitMinTableID, minTableID)
}

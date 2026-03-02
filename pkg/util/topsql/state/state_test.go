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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTopRUEnableDisableAndResetInterval verifies TopRU enablement is ref-counted
// and that the item interval resets to default only when the last consumer leaves.
// It also confirms extra disables do not underflow the global counter.
func TestTopRUEnableDisableAndResetInterval(t *testing.T) {
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUItemInterval()

	require.False(t, TopRUEnabled())
	EnableTopRU()
	require.True(t, TopRUEnabled())
	EnableTopRU()
	require.True(t, TopRUEnabled())

	require.NoError(t, SetTopRUItemInterval(15))
	require.Equal(t, int64(15), GetTopRUItemInterval())

	DisableTopRU()
	require.True(t, TopRUEnabled())
	require.Equal(t, int64(15), GetTopRUItemInterval())

	DisableTopRU()
	require.False(t, TopRUEnabled())
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())

	DisableTopRU()
	require.False(t, TopRUEnabled())
}

// TestTopRUItemIntervalLastWriteWins verifies the global TopRU item interval
// always reflects the most recent valid setter.
func TestTopRUItemIntervalLastWriteWins(t *testing.T) {
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUItemInterval()

	require.NoError(t, SetTopRUItemInterval(30))
	require.Equal(t, int64(30), GetTopRUItemInterval())

	require.NoError(t, SetTopRUItemInterval(60))
	require.Equal(t, int64(60), GetTopRUItemInterval())

	require.NoError(t, SetTopRUItemInterval(15))
	require.Equal(t, int64(15), GetTopRUItemInterval())
}

// TestTopRUItemIntervalRejectsInvalid verifies invalid intervals
// return errors and do not override existing values.
func TestTopRUItemIntervalRejectsInvalid(t *testing.T) {
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUItemInterval()

	require.ErrorIs(t, SetTopRUItemInterval(1), ErrInvalidTopRUItemInterval)
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())

	require.NoError(t, SetTopRUItemInterval(0))
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())

	require.NoError(t, SetTopRUItemInterval(15))
	require.Equal(t, int64(15), GetTopRUItemInterval())

	require.ErrorIs(t, SetTopRUItemInterval(99), ErrInvalidTopRUItemInterval)
	require.Equal(t, int64(15), GetTopRUItemInterval())

	require.NoError(t, SetTopRUItemInterval(0)) // normalized to 60.
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())
}

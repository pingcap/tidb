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

func TestTopRUEnableDisableAndResetInterval(t *testing.T) {
	// Contract: TopRU enable is ref-counted; interval resets only when count reaches 0.
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUItemInterval()

	require.False(t, TopRUEnabled())
	EnableTopRU()
	require.True(t, TopRUEnabled())
	EnableTopRU()
	require.True(t, TopRUEnabled())

	SetTopRUItemInterval(15)
	require.Equal(t, int64(15), GetTopRUItemInterval())

	DisableTopRU()
	require.True(t, TopRUEnabled())
	require.Equal(t, int64(15), GetTopRUItemInterval())

	DisableTopRU()
	require.False(t, TopRUEnabled())
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())

	// Defensive extra disable should not underflow.
	DisableTopRU()
	require.False(t, TopRUEnabled())
}

func TestTopRUItemIntervalLastWriteWins(t *testing.T) {
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUItemInterval()

	SetTopRUItemInterval(30)
	require.Equal(t, int64(30), GetTopRUItemInterval())

	// Last write wins globally.
	SetTopRUItemInterval(60)
	require.Equal(t, int64(60), GetTopRUItemInterval())

	SetTopRUItemInterval(15)
	require.Equal(t, int64(15), GetTopRUItemInterval())
}

func TestTopRUItemIntervalNormalizeInvalidToDefault(t *testing.T) {
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUItemInterval()

	SetTopRUItemInterval(1)
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())

	SetTopRUItemInterval(0)
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())

	SetTopRUItemInterval(99)
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())

	// Invalid values are normalized to 60 and last write wins.
	SetTopRUItemInterval(15)
	require.Equal(t, int64(15), GetTopRUItemInterval())
	SetTopRUItemInterval(0) // normalized to 60.
	require.Equal(t, int64(DefTiDBTopRUItemIntervalSeconds), GetTopRUItemInterval())
}

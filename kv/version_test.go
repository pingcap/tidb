// Copyright 2016 PingCAP, Inc.
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

package kv

import (
	"testing"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	le := NewVersion(42).Cmp(NewVersion(43))
	gt := NewVersion(42).Cmp(NewVersion(41))
	eq := NewVersion(42).Cmp(NewVersion(42))

	require.True(t, le < 0)
	require.True(t, gt > 0)
	require.True(t, eq == 0)
	require.True(t, MinVersion.Cmp(MaxVersion) < 0)
}

func TestMppVersion(t *testing.T) {
	require.Equal(t, int64(2), GetNewestMppVersion().ToInt64())
	{
		v, ok := ToMppVersion("unspecified")
		require.True(t, ok)
		require.Equal(t, v, MppVersionUnspecified)
	}
	{
		v, ok := ToMppVersion("-1")
		require.True(t, ok)
		require.Equal(t, v, MppVersionUnspecified)
	}
	{
		v, ok := ToMppVersion("0")
		require.True(t, ok)
		require.Equal(t, v, MppVersionV0)
	}
	{
		v, ok := ToMppVersion("1")
		require.True(t, ok)
		require.Equal(t, v, MppVersionV1)
	}
	{
		v, ok := ToMppVersion("2")
		require.True(t, ok)
		require.Equal(t, v, MppVersionV2)
	}
}

func TestExchangeCompressionMode(t *testing.T) {
	require.Equal(t, "UNSPECIFIED", ExchangeCompressionModeUnspecified.Name())
	{
		a, ok := ToExchangeCompressionMode("UNSPECIFIED")
		require.Equal(t, a, ExchangeCompressionModeUnspecified)
		require.True(t, ok)
	}
	require.Equal(t, "NONE", ExchangeCompressionModeNONE.Name())
	{
		a, ok := ToExchangeCompressionMode("NONE")
		require.Equal(t, a, ExchangeCompressionModeNONE)
		require.True(t, ok)
	}
	require.Equal(t, "FAST", ExchangeCompressionModeFast.Name())
	{
		a, ok := ToExchangeCompressionMode("FAST")
		require.Equal(t, a, ExchangeCompressionModeFast)
		require.True(t, ok)
	}
	require.Equal(t, "HIGH_COMPRESSION", ExchangeCompressionModeHC.Name())
	{
		a, ok := ToExchangeCompressionMode("HIGH_COMPRESSION")
		require.Equal(t, a, ExchangeCompressionModeHC)
		require.True(t, ok)
	}
	// default `FAST`
	require.Equal(t, ExchangeCompressionModeFast, RecommendedExchangeCompressionMode)
	require.Equal(t, tipb.CompressionMode_FAST, RecommendedExchangeCompressionMode.ToTipbCompressionMode())
}

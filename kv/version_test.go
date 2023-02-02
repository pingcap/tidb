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
	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	le := NewVersion(42).Cmp(NewVersion(43))
	gt := NewVersion(42).Cmp(NewVersion(41))
	eq := NewVersion(42).Cmp(NewVersion(42))

	assert.True(t, le < 0)
	assert.True(t, gt > 0)
	assert.True(t, eq == 0)
	assert.True(t, MinVersion.Cmp(MaxVersion) < 0)
}

func TestMppVersion(t *testing.T) {
	assert.Equal(t, int64(1), GetNewestMppVersion().ToInt64())
	{
		v, ok := ToMppVersion("unspecified")
		assert.True(t, ok)
		assert.Equal(t, v, MppVersionUnspecified)
	}
	{
		v, ok := ToMppVersion("-1")
		assert.True(t, ok)
		assert.Equal(t, v, MppVersionUnspecified)
	}
	{
		v, ok := ToMppVersion("0")
		assert.True(t, ok)
		assert.Equal(t, v, MppVersionV0)
	}
	{
		v, ok := ToMppVersion("1")
		assert.True(t, ok)
		assert.Equal(t, v, MppVersionV1)
	}
}

func TestExchangeCompressionMode(t *testing.T) {
	assert.Equal(t, "UNSPECIFIED", ExchangeCompressionModeUnspecified.Name())
	{
		a, ok := ToExchangeCompressionMode("UNSPECIFIED")
		assert.Equal(t, a, ExchangeCompressionModeUnspecified)
		assert.True(t, ok)
	}
	assert.Equal(t, "NONE", ExchangeCompressionModeNONE.Name())
	{
		a, ok := ToExchangeCompressionMode("NONE")
		assert.Equal(t, a, ExchangeCompressionModeNONE)
		assert.True(t, ok)
	}
	assert.Equal(t, "FAST", ExchangeCompressionModeFast.Name())
	{
		a, ok := ToExchangeCompressionMode("FAST")
		assert.Equal(t, a, ExchangeCompressionModeFast)
		assert.True(t, ok)
	}
	assert.Equal(t, "HIGH_COMPRESSION", ExchangeCompressionModeHC.Name())
	{
		a, ok := ToExchangeCompressionMode("HIGH_COMPRESSION")
		assert.Equal(t, a, ExchangeCompressionModeHC)
		assert.True(t, ok)
	}
	// default `FAST`
	assert.Equal(t, ExchangeCompressionModeFast, RecommendedExchangeCompressionMode)
	assert.Equal(t, tipb.CompressionMode_FAST, RecommendedExchangeCompressionMode.ToTipbCompressionMode())
}

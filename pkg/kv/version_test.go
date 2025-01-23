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

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
	assert.Equal(t, int64(3), GetNewestMppVersion().ToInt64())
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
	{
		v, ok := ToMppVersion("2")
		assert.True(t, ok)
		assert.Equal(t, v, MppVersionV2)
	}
	{
		v, ok := ToMppVersion("3")
		assert.True(t, ok)
		assert.Equal(t, v, MppVersionV3)
	}
}

func TestExchangeCompressionMode(t *testing.T) {
	assert.Equal(t, "UNSPECIFIED", vardef.ExchangeCompressionModeUnspecified.Name())
	{
		a, ok := vardef.ToExchangeCompressionMode("UNSPECIFIED")
		assert.Equal(t, a, vardef.ExchangeCompressionModeUnspecified)
		assert.True(t, ok)
	}
	assert.Equal(t, "NONE", vardef.ExchangeCompressionModeNONE.Name())
	{
		a, ok := vardef.ToExchangeCompressionMode("NONE")
		assert.Equal(t, a, vardef.ExchangeCompressionModeNONE)
		assert.True(t, ok)
	}
	assert.Equal(t, "FAST", vardef.ExchangeCompressionModeFast.Name())
	{
		a, ok := vardef.ToExchangeCompressionMode("FAST")
		assert.Equal(t, a, vardef.ExchangeCompressionModeFast)
		assert.True(t, ok)
	}
	assert.Equal(t, "HIGH_COMPRESSION", vardef.ExchangeCompressionModeHC.Name())
	{
		a, ok := vardef.ToExchangeCompressionMode("HIGH_COMPRESSION")
		assert.Equal(t, a, vardef.ExchangeCompressionModeHC)
		assert.True(t, ok)
	}
	// default `FAST`
	assert.Equal(t, vardef.ExchangeCompressionModeFast, vardef.RecommendedExchangeCompressionMode)
	assert.Equal(t, tipb.CompressionMode_FAST, vardef.RecommendedExchangeCompressionMode.ToTipbCompressionMode())
}

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

package copr

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
)

func TestBuildCacheKey(t *testing.T) {
	req := coprocessor.Request{
		Tp:      0xAB,
		StartTs: 0xAABBCC,
		Data:    []uint8{0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0},
		Ranges: []*coprocessor.KeyRange{
			{
				Start: kv.Key{0x01},
				End:   kv.Key{0x01, 0x02},
			},
			{
				Start: kv.Key{0x01, 0x01, 0x02},
				End:   kv.Key{0x01, 0x01, 0x03},
			},
		},
	}

	key, err := coprCacheBuildKey(&req)
	require.NoError(t, err)
	expectKey := ""
	expectKey += "\xab"                             // 1 byte Tp
	expectKey += "\x08\x00\x00\x00"                 // 4 bytes Data len
	expectKey += "\x18\x00\x20\x00\x40\x00\x5a\x00" // Data
	expectKey += "\x01\x00"                         // 2 bytes StartKey len
	expectKey += "\x01"                             // StartKey
	expectKey += "\x02\x00"                         // 2 bytes EndKey len
	expectKey += "\x01\x02"                         // EndKey
	expectKey += "\x03\x00"                         // 2 bytes StartKey len
	expectKey += "\x01\x01\x02"                     // StartKey
	expectKey += "\x03\x00"                         // 2 bytes EndKey len
	expectKey += "\x01\x01\x03"                     // EndKey
	require.EqualValues(t, []byte(expectKey), key)

	req = coprocessor.Request{
		Tp:      0xABCC, // Tp too big
		StartTs: 0xAABBCC,
		Data:    []uint8{0x18},
		Ranges:  []*coprocessor.KeyRange{},
	}

	_, err = coprCacheBuildKey(&req)
	require.Error(t, err)
}

func TestDisable(t *testing.T) {
	cache, err := newCoprCache(&config.CoprocessorCache{CapacityMB: 0})
	require.NoError(t, err)
	require.Nil(t, cache)

	v := cache.Set([]byte("foo"), &coprCacheValue{})
	require.False(t, v)

	v2 := cache.Get([]byte("foo"))
	require.Nil(t, v2)

	v = cache.CheckResponseAdmission(1024, time.Second*5)
	require.False(t, v)

	cache, err = newCoprCache(&config.CoprocessorCache{CapacityMB: 0.001})
	require.Error(t, err)
	require.Nil(t, cache)

	cache, err = newCoprCache(&config.CoprocessorCache{CapacityMB: 0.001, AdmissionMaxResultMB: 1})
	require.NoError(t, err)
	require.NotNil(t, cache)
	defer cache.cache.Close()
}

func TestAdmission(t *testing.T) {
	cache, err := newCoprCache(&config.CoprocessorCache{AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: 1})
	require.NoError(t, err)
	require.NotNil(t, cache)
	defer cache.cache.Close()

	v := cache.CheckRequestAdmission(0)
	require.True(t, v)

	v = cache.CheckRequestAdmission(1000)
	require.True(t, v)

	v = cache.CheckResponseAdmission(0, 0)
	require.False(t, v)

	v = cache.CheckResponseAdmission(0, 4*time.Millisecond)
	require.False(t, v)

	v = cache.CheckResponseAdmission(0, 5*time.Millisecond)
	require.False(t, v)

	v = cache.CheckResponseAdmission(1, 0)
	require.False(t, v)

	v = cache.CheckResponseAdmission(1, 4*time.Millisecond)
	require.False(t, v)

	v = cache.CheckResponseAdmission(1, 5*time.Millisecond)
	require.True(t, v)

	v = cache.CheckResponseAdmission(1024, 5*time.Millisecond)
	require.True(t, v)

	v = cache.CheckResponseAdmission(1024*1024, 5*time.Millisecond)
	require.True(t, v)

	v = cache.CheckResponseAdmission(1024*1024+1, 5*time.Millisecond)
	require.False(t, v)

	v = cache.CheckResponseAdmission(1024*1024+1, 4*time.Millisecond)
	require.False(t, v)

	cache, err = newCoprCache(&config.CoprocessorCache{AdmissionMaxRanges: 5, AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: 1})
	require.NoError(t, err)
	require.NotNil(t, cache)
	defer cache.cache.Close()

	v = cache.CheckRequestAdmission(0)
	require.True(t, v)

	v = cache.CheckRequestAdmission(5)
	require.True(t, v)

	v = cache.CheckRequestAdmission(6)
	require.False(t, v)
}

func TestCacheValueLen(t *testing.T) {
	v := coprCacheValue{
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	}
	// 72 = (8 byte pointer + 8 byte for length + 8 byte for cap) * 2 + 8 byte * 3
	require.Equal(t, 72, v.Len())

	v = coprCacheValue{
		Key:               []byte("foobar"),
		Data:              []byte("12345678"),
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	}
	require.Equal(t, 72+len(v.Key)+len(v.Data), v.Len())
}

func TestGetSet(t *testing.T) {
	cache, err := newCoprCache(&config.CoprocessorCache{AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: 1})
	require.NoError(t, err)
	require.NotNil(t, cache)
	defer cache.cache.Close()

	v := cache.Get([]byte("foo"))
	require.Nil(t, v)

	v2 := cache.Set([]byte("foo"), &coprCacheValue{
		Data:              []byte("bar"),
		TimeStamp:         0x123,
		RegionID:          0x1,
		RegionDataVersion: 0x3,
	})
	require.True(t, v2)

	// See https://github.com/dgraph-io/ristretto/blob/83508260cb49a2c3261c2774c991870fd18b5a1b/cache_test.go#L13
	// Changed from 10ms to 50ms to resist from unstable CI environment.
	time.Sleep(time.Millisecond * 50)

	v = cache.Get([]byte("foo"))
	require.NotNil(t, v)
	require.EqualValues(t, []byte("bar"), v.Data)
}

func TestIssue24118(t *testing.T) {
	_, err := newCoprCache(&config.CoprocessorCache{AdmissionMinProcessMs: 5, AdmissionMaxResultMB: 1, CapacityMB: -1})
	require.EqualError(t, err, "Capacity must be > 0 to enable the cache")
}

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

package earlystopprofile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimitBucketForRows(t *testing.T) {
	require.Equal(t, LimitBucketLE1, LimitBucketForRows(0))
	require.Equal(t, LimitBucketLE1, LimitBucketForRows(1))
	require.Equal(t, LimitBucketLE10, LimitBucketForRows(2))
	require.Equal(t, LimitBucketLE10, LimitBucketForRows(10))
	require.Equal(t, LimitBucketLE100, LimitBucketForRows(11))
	require.Equal(t, LimitBucketLE100, LimitBucketForRows(100))
	require.Equal(t, LimitBucketLE1024, LimitBucketForRows(101))
	require.Equal(t, LimitBucketLE1024, LimitBucketForRows(1024))
	require.Equal(t, LimitBucketLE10000, LimitBucketForRows(1025))
	require.Equal(t, LimitBucketLE10000, LimitBucketForRows(10000))
	require.Equal(t, LimitBucketGT10000, LimitBucketForRows(10001))
}

func TestProfileStoreLookupCapAfterEnoughSamples(t *testing.T) {
	store := NewStore(8)
	key := Key{
		SchemaName:  "test",
		SQLDigest:   "sql",
		PlanDigest:  "plan",
		ReaderType:  ReaderTypeTable,
		KeepOrder:   true,
		LimitBucket: LimitBucketLE10,
	}
	sample := Sample{
		Candidate: Candidate{
			Key:       key,
			LimitRows: 10,
			CapUsed:   4,
		},
		ResultRows:    10,
		RequestCount:  16,
		ProcessedKeys: 10000,
		TotalKeys:     10000,
		Latency:       10 * time.Millisecond,
		Succeed:       true,
	}

	store.Observe(sample)
	_, ok := store.LookupCap(key)
	require.False(t, ok)
	store.Observe(sample)
	_, ok = store.LookupCap(key)
	require.False(t, ok)
	store.Observe(sample)
	cap, ok := store.LookupCap(key)
	require.True(t, ok)
	require.Equal(t, 1, cap)
}

func TestProfileStoreRecoversCapWithHealthySamples(t *testing.T) {
	store := NewStore(8)
	key := Key{
		SchemaName:  "test",
		SQLDigest:   "sql",
		PlanDigest:  "plan",
		ReaderType:  ReaderTypeTable,
		KeepOrder:   true,
		LimitBucket: LimitBucketLE100,
	}
	overReadSample := Sample{
		Candidate: Candidate{
			Key:       key,
			LimitRows: 100,
			BaseCap:   2,
			CapUsed:   2,
		},
		ResultRows:    100,
		RequestCount:  16,
		ProcessedKeys: 100000,
		TotalKeys:     100000,
		Latency:       10 * time.Millisecond,
		Succeed:       true,
	}
	for range 3 {
		store.Observe(overReadSample)
	}
	cap, ok := store.LookupCap(key)
	require.True(t, ok)
	require.Equal(t, 1, cap)

	healthySample := Sample{
		Candidate: Candidate{
			Key:       key,
			LimitRows: 100,
			BaseCap:   2,
			CapUsed:   1,
		},
		ResultRows:    100,
		RequestCount:  1,
		ProcessedKeys: 100,
		TotalKeys:     100,
		Latency:       time.Millisecond,
		Succeed:       true,
	}
	for range 40 {
		store.Observe(healthySample)
	}
	cap, ok = store.LookupCap(key)
	require.True(t, ok)
	require.Equal(t, 2, cap)
}

func TestProfileStoreIgnoresInvalidSamples(t *testing.T) {
	store := NewStore(8)
	key := Key{
		SchemaName:  "test",
		SQLDigest:   "sql",
		PlanDigest:  "plan",
		ReaderType:  ReaderTypeIndex,
		KeepOrder:   true,
		LimitBucket: LimitBucketLE1024,
	}
	valid := Sample{
		Candidate: Candidate{
			Key:       key,
			LimitRows: 100,
			CapUsed:   2,
		},
		ResultRows:    100,
		RequestCount:  4,
		ProcessedKeys: 100,
		Latency:       time.Millisecond,
		Succeed:       true,
	}

	for _, sample := range []Sample{
		{Candidate: valid.Candidate, ResultRows: 100, RequestCount: 4, ProcessedKeys: 100, Succeed: false},
		{Candidate: valid.Candidate, ResultRows: 100, RequestCount: 4, ProcessedKeys: 100, Succeed: true, Internal: true},
		{Candidate: valid.Candidate, ResultRows: 100, RequestCount: 4, ProcessedKeys: 0, Succeed: true},
		{Candidate: Candidate{Key: key, LimitRows: 0, CapUsed: 2}, ResultRows: 100, RequestCount: 4, ProcessedKeys: 100, Succeed: true},
		{Candidate: Candidate{Key: key, LimitRows: 100, CapUsed: 0}, ResultRows: 100, RequestCount: 4, ProcessedKeys: 100, Succeed: true},
	} {
		store.Observe(sample)
	}
	_, ok := store.LookupCap(key)
	require.False(t, ok)

	store.Observe(valid)
	store.Observe(valid)
	store.Observe(valid)
	cap, ok := store.LookupCap(key)
	require.True(t, ok)
	require.Equal(t, 2, cap)
}

func TestProfileKeyHashDistinguishesFields(t *testing.T) {
	base := Key{
		SchemaName:  "test",
		SQLDigest:   "sql",
		PlanDigest:  "plan",
		ReaderType:  ReaderTypeTable,
		KeepOrder:   true,
		LimitBucket: LimitBucketLE10,
	}
	changed := base
	changed.LimitBucket = LimitBucketLE100
	require.NotEqual(t, string(base.Hash()), string(changed.Hash()))

	changed = base
	changed.ReaderType = ReaderTypeIndex
	require.NotEqual(t, string(base.Hash()), string(changed.Hash()))

	changed = base
	changed.KeepOrder = false
	require.NotEqual(t, string(base.Hash()), string(changed.Hash()))
}

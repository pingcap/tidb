// Copyright 2025 PingCAP, Inc.
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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func genRandHex(length int) []byte {
	const chars = "0123456789abcdef"
	res := make([]byte, length)
	for i := range length {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return res
}

func TestCoprRequestLimiterWaitsUntilRelease(t *testing.T) {
	limiter := NewCoprRequestLimiter(1)
	done := make(chan struct{})
	require.False(t, limiter.Acquire(done))

	acquired := make(chan struct{})
	acquireExit := make(chan bool, 1)
	go func() {
		exit := limiter.Acquire(done)
		acquireExit <- exit
		close(acquired)
		if !exit {
			limiter.Release()
		}
	}()

	select {
	case <-acquired:
		require.Fail(t, "second acquire should wait until release")
	case <-time.After(10 * time.Millisecond):
	}

	limiter.Release()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		require.Fail(t, "second acquire should be admitted after release")
	}
	require.False(t, <-acquireExit)
}

func TestCoprRequestLimiterAcquireCanBeCanceled(t *testing.T) {
	limiter := NewCoprRequestLimiter(1)
	require.False(t, limiter.Acquire(make(chan struct{})))

	done := make(chan struct{})
	result := make(chan bool)
	var acquireStarted atomic.Bool
	go func() {
		acquireStarted.Store(true)
		result <- limiter.Acquire(done)
	}()

	require.Eventually(t, func() bool {
		return acquireStarted.Load()
	}, time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	close(done)
	require.True(t, <-result)

	limiter.Release()
	require.False(t, limiter.Acquire(make(chan struct{})))
	limiter.Release()
}

func TestCoprRequestLimiterRedundantReleasePanics(t *testing.T) {
	limiter := NewCoprRequestLimiter(1)
	require.Panics(t, func() {
		limiter.Release()
	})
}

func TestCoprRequestLimiterConcurrentAcquireRelease(t *testing.T) {
	const capacity = int64(3)
	limiter := NewCoprRequestLimiter(int(capacity))
	done := make(chan struct{})
	var active atomic.Int64
	var maxActive atomic.Int64
	var acquireExit atomic.Bool
	var capacityExceeded atomic.Bool
	var wg sync.WaitGroup

	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if limiter.Acquire(done) {
					acquireExit.Store(true)
					return
				}
				cur := active.Add(1)
				if cur > capacity {
					capacityExceeded.Store(true)
				}
				for {
					old := maxActive.Load()
					if cur <= old || maxActive.CompareAndSwap(old, cur) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				active.Add(-1)
				limiter.Release()
			}
		}()
	}

	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "concurrent acquire/release should finish")
	}
	require.False(t, acquireExit.Load())
	require.False(t, capacityExceeded.Load())
	require.LessOrEqual(t, maxActive.Load(), capacity)
	require.Equal(t, int64(0), active.Load())
}

func TestQueryCopStoreLimiter(t *testing.T) {
	require.Nil(t, NewQueryCopStoreLimiter(0))

	limiterGroup := NewQueryCopStoreLimiter(1)
	require.NotNil(t, limiterGroup)
	require.Equal(t, 1, limiterGroup.Capacity())
	require.Nil(t, limiterGroup.GetStoreLimiter(0))

	store1 := limiterGroup.GetStoreLimiter(1)
	require.NotNil(t, store1)
	require.Same(t, store1, limiterGroup.GetStoreLimiter(1))
	require.NotSame(t, store1, limiterGroup.GetStoreLimiter(2))

	done := make(chan struct{})
	require.False(t, store1.Acquire(done))

	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		require.False(t, store1.Acquire(done))
		store1.Release()
	}()

	select {
	case <-blocked:
		require.Fail(t, "same-store acquire should wait for the first release")
	case <-time.After(10 * time.Millisecond):
	}

	store2 := limiterGroup.GetStoreLimiter(2)
	require.False(t, store2.Acquire(done))
	store2.Release()

	store1.Release()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		require.Fail(t, "same-store acquire should succeed after release")
	}
}

func TestResourceGroupTagEncoding(t *testing.T) {
	sqlDigest := parser.NewDigest(nil)
	tag := NewResourceGroupTagBuilder(nil).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	require.Len(t, tag, 2)

	decodedSQLDigest, err := resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Len(t, decodedSQLDigest, 0)
	resTag := &tipb.ResourceGroupTag{}
	err = resTag.Unmarshal(tag)
	require.NoError(t, err)
	require.Nil(t, resTag.KeyspaceName)

	sqlDigest = parser.NewDigest([]byte{'a', 'a'})
	tag = NewResourceGroupTagBuilder(nil).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte)
	require.Len(t, tag, 6)

	decodedSQLDigest, err = resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)

	keyspaceName := []byte("123")
	sqlDigest = parser.NewDigest(genRandHex(64))
	tag = NewResourceGroupTagBuilder(keyspaceName).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	decodedSQLDigest, err = resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)
	resTag = &tipb.ResourceGroupTag{}
	err = resTag.Unmarshal(tag)
	require.NoError(t, err)
	require.NotNil(t, resTag.KeyspaceName)
	require.Equal(t, resTag.KeyspaceName, keyspaceName)

	sqlDigest = parser.NewDigest(genRandHex(510))
	tag = NewResourceGroupTagBuilder(keyspace.GetKeyspaceNameBytesBySettings()).SetSQLDigest(sqlDigest).EncodeTagWithKey([]byte(""))
	decodedSQLDigest, err = resourcegrouptag.DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)
	resTag = &tipb.ResourceGroupTag{}
	err = resTag.Unmarshal(tag)
	require.NoError(t, err)
	if kerneltype.IsNextGen() {
		require.NotNil(t, resTag.KeyspaceName)
		require.Equal(t, resTag.KeyspaceName, keyspace.GetKeyspaceNameBytesBySettings())
	} else {
		require.Nil(t, resTag.KeyspaceName)
	}
}

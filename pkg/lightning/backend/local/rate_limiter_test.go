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

package local

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimit(t *testing.T) {
	const interval = 100 * time.Millisecond
	l := newIngestLimiter(1000, 1000)

	err := l.Acquire(context.Background(), 0, 1000)
	require.NoError(t, err)
	acquired := make(chan struct{})
	go func() {
		require.NoError(t, l.Acquire(context.Background(), 0, 1))
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("should not acquire when concurrency exhausted")
	case <-time.After(interval):
	}

	l.Release(0, 1)

	select {
	case <-acquired:
	case <-time.After(interval):
		t.Fatal("should acquire after release")
	}
	l.Release(0, 1000)

	acquired = make(chan struct{})
	l.Acquire(context.Background(), 1, 1000)
	go func() {
		require.NoError(t, l.Acquire(context.Background(), 2, 1))
		close(acquired)
	}()
	select {
	case <-acquired:
	case <-time.After(interval):
		t.Fatal("should acquire for different storeID")
	}

	l.Release(1, 1000)
	l.Release(2, 1)
}

func TestConcurrentImportsShareIngestLimiterWithoutSharingContext(t *testing.T) {
	backend := &Backend{}
	first := backend.updateIngestLimiter(1, 0)
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	require.NoError(t, first.Acquire(ctxA, 7, 1))
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Same(t, first, backend.updateIngestLimiter(1, 0))
		}()
	}
	wg.Wait()
	second := backend.ingestLimiter.Load()
	result := make(chan error, 1)
	go func() { result <- second.Acquire(ctxB, 7, 1) }()
	cancelA()
	// Canceling A cannot cancel B or reset A's occupied capacity.
	select {
	case err := <-result:
		t.Fatalf("second import escaped shared capacity or inherited cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	first.Release(7, 1)
	require.NoError(t, <-result)
	second.Release(7, 1)
	require.ErrorIs(t, second.Acquire(ctxA, 7, 1), context.Canceled)
	require.NoError(t, first.Acquire(ctxB, 7, 1))
	first.Release(7, 1)
	// A changed setting still applies to subsequent imports.
	require.NotSame(t, first, backend.updateIngestLimiter(2, 0))
}

func TestRateLimit(t *testing.T) {
	l := newIngestLimiter(10, 10)

	start := time.Now()
	for i := 0; i < 10; i++ {
		require.NoError(t, l.Acquire(context.Background(), 0, 1))
	}
	burstDuration := time.Since(start)
	require.Less(t, burstDuration, 50*time.Millisecond, "burst should be immediate")

	l.Release(0, 10)
	start = time.Now()
	require.NoError(t, l.Acquire(context.Background(), 0, 1))
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "should respect rate limit")
	require.Less(t, elapsed, 150*time.Millisecond, "within tolerance")

	require.NoError(t, l.Acquire(context.Background(), 1, 10))
	l.Release(1, 10)
	start = time.Now()
	require.NoError(t, l.Acquire(context.Background(), 2, 10))
	burstDuration = time.Since(start)
	require.Less(t, burstDuration, 50*time.Millisecond, "acquire for different storeID should be immediate")
	l.Release(2, 10)
}

func TestContextCancelDuringConcurrencyWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newIngestLimiter(1, 100)

	require.NoError(t, l.Acquire(ctx, 0, 1))

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.Acquire(ctx, 0, 1)
	}()

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for error")
	}

	l.Release(0, 1)
}

func TestContextCancelDuringRateWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newIngestLimiter(100, 1)

	require.NoError(t, l.Acquire(ctx, 0, 1))

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.Acquire(ctx, 0, 1)
	}()

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for error")
	}
}

func TestIngestLimiterBurst(t *testing.T) {
	l := newIngestLimiter(0, 0)
	require.Equal(t, math.MaxInt, l.Burst())
	require.True(t, l.NoLimit())

	l = newIngestLimiter(0, 1000)
	require.Equal(t, 1000, l.Burst())
	require.False(t, l.NoLimit())

	l = newIngestLimiter(1000, 0)
	require.Equal(t, 1000, l.Burst())
	require.False(t, l.NoLimit())

	l = newIngestLimiter(1000, 1000)
	require.Equal(t, 1000, l.Burst())
	require.False(t, l.NoLimit())

	l = newIngestLimiter(1000, 567)
	require.Equal(t, 567, l.Burst())
	require.False(t, l.NoLimit())

	l = newIngestLimiter(567, 1000)
	require.Equal(t, 567, l.Burst())
	require.False(t, l.NoLimit())
}

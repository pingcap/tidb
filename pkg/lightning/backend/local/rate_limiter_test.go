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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimit(t *testing.T) {
	const maxConcurrency = 2
	const interval = 100 * time.Millisecond
	l := newIngestLimiter(context.Background(), maxConcurrency, 100)
	l.ctx = context.Background()

	for range maxConcurrency {
		err := l.Acquire()
		require.NoError(t, err)
	}
	acquired := make(chan struct{})
	go func() {
		require.NoError(t, l.Acquire())
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("should not acquire when concurrency exhausted")
	case <-time.After(interval):
	}

	l.Release()

	select {
	case <-acquired:
	case <-time.After(interval):
		t.Fatal("should acquire after release")
	}
	l.Release()
}

func TestRateLimit(t *testing.T) {
	const maxRate = 10 // 10 per second (100ms per request)
	l := newIngestLimiter(context.Background(), 100, maxRate)
	l.ctx = context.Background()

	start := time.Now()
	for range maxRate {
		require.NoError(t, l.Acquire())
	}
	burstDuration := time.Since(start)
	require.Less(t, burstDuration, 50*time.Millisecond, "burst should be immediate")

	start = time.Now()
	require.NoError(t, l.Acquire())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "should respect rate limit")
	require.Less(t, elapsed, 150*time.Millisecond, "within tolerance")
}

func TestContextCancelDuringConcurrencyWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newIngestLimiter(ctx, 1, 10)

	require.NoError(t, l.Acquire())

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.Acquire()
	}()

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for error")
	}

	l.Release()
}

func TestContextCancelDuringRateWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newIngestLimiter(ctx, 100, 1) // 1 req/sec

	require.NoError(t, l.Acquire())

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.Acquire()
	}()

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for error")
	}
}

func TestDoneWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newIngestLimiter(ctx, 1, 10)

	require.NoError(t, l.Acquire())
	require.Len(t, l.slots, 1)

	cancel()
	l.Release()

	require.Len(t, l.slots, 0, "slot should not be released")
}

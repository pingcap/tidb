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
	const interval = 100 * time.Millisecond
	l := newIngestLimiter(context.Background(), 1000)

	err := l.Acquire(1000)
	require.NoError(t, err)
	acquired := make(chan struct{})
	go func() {
		require.NoError(t, l.Acquire(1))
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("should not acquire when concurrency exhausted")
	case <-time.After(interval):
	}

	l.Release(1)

	select {
	case <-acquired:
	case <-time.After(interval):
		t.Fatal("should acquire after release")
	}
	l.Release(1000)
}

func TestRateLimit(t *testing.T) {
	l := newIngestLimiter(context.Background(), 10)

	start := time.Now()
	for i := 0; i < 10; i++ {
		require.NoError(t, l.Acquire(1))
	}
	burstDuration := time.Since(start)
	require.Less(t, burstDuration, 50*time.Millisecond, "burst should be immediate")

	l.Release(10)
	start = time.Now()
	require.NoError(t, l.Acquire(1))
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "should respect rate limit")
	require.Less(t, elapsed, 150*time.Millisecond, "within tolerance")
}

func TestContextCancelDuringConcurrencyWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newIngestLimiter(ctx, 1)

	require.NoError(t, l.Acquire(1))

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.Acquire(1)
	}()

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for error")
	}

	l.Release(1)
}

func TestContextCancelDuringRateWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := newIngestLimiter(ctx, 100)

	require.NoError(t, l.Acquire(100))

	errCh := make(chan error, 1)
	go func() {
		errCh <- l.Acquire(1)
	}()

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for error")
	}
}

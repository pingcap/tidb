// Copyright 2020 PingCAP, Inc.
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

package util

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBarrierEnterExitBasics(t *testing.T) {
	b := NewBarrier()
	require.False(t, b.IsPaused())

	var inFlightLocal int32
	var wg sync.WaitGroup
	n := 4
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			b.Enter()
			atomic.AddInt32(&inFlightLocal, 1)
			time.Sleep(10 * time.Millisecond)
			b.Exit()
			atomic.AddInt32(&inFlightLocal, -1)
			wg.Done()
		}()
	}

	require.Eventually(t, func() bool {
		return b.CurrentInFlight() > 0
	}, 500*time.Millisecond, 5*time.Millisecond)

	wg.Wait()
	require.Eventually(t, func() bool {
		return b.CurrentInFlight() == 0 && atomic.LoadInt32(&inFlightLocal) == 0
	}, time.Second, 5*time.Millisecond)
}

func TestBarrierPauseAndDrain(t *testing.T) {
	b := NewBarrier()

	started := make(chan struct{})
	released := make(chan struct{})

	go func() {
		b.Enter()
		close(started)
		time.Sleep(50 * time.Millisecond)
		b.Exit()
		close(released)
	}()

	require.Eventually(t, func() bool {
		return b.CurrentInFlight() == 1
	}, 500*time.Millisecond, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := b.PauseAndWait(ctx)
	require.NoError(t, err)
	require.True(t, b.IsPaused())

	done := make(chan struct{})
	go func() {
		b.Enter()
		b.Exit()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("enter was not blocked while paused")
	case <-time.After(60 * time.Millisecond):
	}

	b.Resume()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 500*time.Millisecond, 5*time.Millisecond)

	<-started
	<-released
}

func TestBarrierBlocksEnterWhenPaused(t *testing.T) {
	b := NewBarrier()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := b.PauseAndWait(ctx)
	require.NoError(t, err)
	require.True(t, b.IsPaused())

	proceeded := make(chan struct{})
	go func() {
		b.Enter()
		close(proceeded)
		b.Exit()
	}()

	select {
	case <-proceeded:
		t.Fatal("enter should block while paused")
	case <-time.After(80 * time.Millisecond):
	}

	b.Resume()
	require.Eventually(t, func() bool {
		select {
		case <-proceeded:
			return true
		default:
			return false
		}
	}, 500*time.Millisecond, 5*time.Millisecond)

	require.Equal(t, int32(0), b.CurrentInFlight())
}

func TestBarrierPauseContextCancel(t *testing.T) {
	b := NewBarrier()

	started := make(chan struct{})
	go func() {
		b.Enter()
		close(started)
		time.Sleep(300 * time.Millisecond)
		b.Exit()
	}()

	<-started
	require.Equal(t, int32(1), b.CurrentInFlight())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := b.PauseAndWait(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	if b.IsPaused() {
		b.Resume()
	}

	require.Eventually(t, func() bool {
		return b.CurrentInFlight() == 0
	}, time.Second, 5*time.Millisecond)
}

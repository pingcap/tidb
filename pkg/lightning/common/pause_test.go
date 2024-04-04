// Copyright 2019 PingCAP, Inc.
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

package common_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertUnblocksBetween(t *testing.T, wg *sync.WaitGroup, min, max time.Duration) {
	ch := make(chan time.Duration)
	start := time.Now()
	go func() {
		wg.Wait()
		ch <- time.Since(start)
	}()
	select {
	case dur := <-ch:
		if dur < min {
			t.Fatal("WaitGroup unblocked before minimum duration, it was " + dur.String())
		}
	case <-time.After(max):
		select {
		case dur := <-ch:
			t.Fatal("WaitGroup did not unblock after maximum duration, it was " + dur.String())
		case <-time.After(1 * time.Second):
			t.Fatal("WaitGroup did not unblock after maximum duration")
		}
	}
}

func TestPause(t *testing.T) {
	var wg sync.WaitGroup
	p := common.NewPauser()

	// initially these calls should not be blocking.

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := p.Wait(context.Background())
			assert.NoError(t, err)
		}()
	}

	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	assertUnblocksBetween(t, &wg, 0*time.Millisecond, 100*time.Millisecond)

	// after calling Pause(), these should be blocking...

	p.Pause()

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := p.Wait(context.Background())
			require.NoError(t, err)
		}()
	}

	// ... until we call Resume()

	go func() {
		time.Sleep(500 * time.Millisecond)
		p.Resume()
	}()

	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	assertUnblocksBetween(t, &wg, 500*time.Millisecond, 800*time.Millisecond)

	// if the context is canceled, Wait() should immediately unblock...

	ctx, cancel := context.WithCancel(context.Background())

	p.Pause()

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := p.Wait(ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()
	}

	cancel()
	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	assertUnblocksBetween(t, &wg, 0*time.Millisecond, 100*time.Millisecond)

	// canceling the context does not affect the state of the pauser

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Wait(context.Background())
		require.NoError(t, err)
	}()

	go func() {
		time.Sleep(500 * time.Millisecond)
		p.Resume()
	}()

	// Give them more time to unblock in case of time exceeding due to high pressure of CI.
	assertUnblocksBetween(t, &wg, 500*time.Millisecond, 800*time.Millisecond)
}

// Run `go test github.com/pingcap/tidb/pkg/lightning/common -check.b -test.v` to get benchmark result.
func BenchmarkWaitNoOp(b *testing.B) {
	p := common.NewPauser()
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = p.Wait(ctx)
	}
}

func BenchmarkWaitCtxCanceled(b *testing.B) {
	p := common.NewPauser()
	p.Pause()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < b.N; i++ {
		_ = p.Wait(ctx)
	}
}

func BenchmarkWaitContended(b *testing.B) {
	p := common.NewPauser()

	done := make(chan struct{})
	defer close(done)
	go func() {
		isPaused := false
		for {
			select {
			case <-done:
				return
			default:
				if isPaused {
					p.Pause()
				} else {
					p.Resume()
				}
				isPaused = !isPaused
			}
		}
	}()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = p.Wait(ctx)
	}
}

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

const (
	waitTimeout          = 10 * time.Second
	blockedCheckDuration = 100 * time.Millisecond
)

func waitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func waitUnblocked(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(waitTimeout):
		t.Fatal("WaitGroup did not unblock")
	}
}

func assertStillBlocked(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("WaitGroup unblocked before pause was released")
	case <-time.After(blockedCheckDuration):
	}
}

func TestPause(t *testing.T) {
	var wg sync.WaitGroup
	p := common.NewPauser()

	// initially these calls should not be blocking.

	wg.Add(10)
	for range 10 {
		go func() {
			defer wg.Done()
			err := p.Wait(context.Background())
			assert.NoError(t, err)
		}()
	}

	done := waitGroupDone(&wg)
	waitUnblocked(t, done)

	// after calling Pause(), these should be blocking...

	p.Pause()

	wg.Add(10)
	for range 10 {
		go func() {
			defer wg.Done()
			err := p.Wait(context.Background())
			require.NoError(t, err)
		}()
	}

	done = waitGroupDone(&wg)
	assertStillBlocked(t, done)

	// ... until we call Resume()
	p.Resume()
	waitUnblocked(t, done)

	// if the context is canceled, Wait() should immediately unblock...

	ctx, cancel := context.WithCancel(context.Background())

	p.Pause()

	wg.Add(10)
	for range 10 {
		go func() {
			defer wg.Done()
			err := p.Wait(ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()
	}

	done = waitGroupDone(&wg)
	assertStillBlocked(t, done)
	cancel()
	waitUnblocked(t, done)

	// canceling the context does not affect the state of the pauser

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Wait(context.Background())
		require.NoError(t, err)
	}()

	done = waitGroupDone(&wg)
	assertStillBlocked(t, done)

	p.Resume()
	waitUnblocked(t, done)
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

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

package copr

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func TestRUEMASeedAndConverge(t *testing.T) {
	e := newRUEMA()
	require.Zero(t, e.Predict(), "fresh EMA: no prediction")

	now := time.Now()
	e.Observe(1_000_000, now)
	require.Equal(t, uint64(1_000_000), e.Predict(),
		"first sample seeds the EMA via a ~infinite dt (alpha≈1)")

	e.Observe(1_000_000, now.Add(100*time.Millisecond))
	require.InDelta(t, float64(1_000_000), float64(e.Predict()), 1,
		"steady input: prediction stays at the input")
}

func TestRUEMATracksShift(t *testing.T) {
	e := newRUEMA()
	now := time.Now()
	for i := 0; i < 5; i++ {
		e.Observe(100_000, now.Add(time.Duration(i)*100*time.Millisecond))
	}
	require.InDelta(t, float64(100_000), float64(e.Predict()), 1,
		"EMA should have converged to 100K after 5 identical samples")

	// Shift workload to 500K per RPC. After enough new samples with tau=1s
	// and 100 ms gaps, the EMA should follow the new regime.
	for i := 5; i < 20; i++ {
		e.Observe(500_000, now.Add(time.Duration(i)*100*time.Millisecond))
	}
	require.Greater(t, e.Predict(), uint64(400_000),
		"EMA should have shifted well above the old steady value")
	require.LessOrEqual(t, e.Predict(), uint64(500_000),
		"EMA should not overshoot the new steady value")
}

func TestRUEMALargeGapCollapsesWeight(t *testing.T) {
	e := newRUEMA()
	now := time.Now()
	e.Observe(100_000, now)
	// A gap much larger than tau (default 1s) means alpha ≈ 1, so the new
	// sample should dominate the EMA almost entirely.
	e.Observe(1_000_000, now.Add(10*time.Second))
	require.InDelta(t, float64(1_000_000), float64(e.Predict()), 1_000,
		"a gap >> tau should collapse the older sample's weight")
}

func TestPagingResponseReadBytes(t *testing.T) {
	// A nil response or one without ScanDetailV2 should produce 0 so the
	// caller can skip the observation cleanly.
	require.Zero(t, pagingResponseReadBytes(nil))
	require.Zero(t, pagingResponseReadBytes(&coprocessor.Response{}))

	// A normal paging response carries processed_versions_size on ScanDetailV2.
	resp := &coprocessor.Response{
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
			ScanDetailV2: &kvrpcpb.ScanDetailV2{
				ProcessedVersionsSize: 1_048_576,
			},
		},
	}
	require.Equal(t, uint64(1_048_576), pagingResponseReadBytes(resp))
}

func TestRUEMAConcurrentObserveAndPredict(t *testing.T) {
	// One EMA per copIterator is shared by multiple workers, so Observe and
	// Predict must be race-free. Run with `go test -race` to exercise the
	// mutex; this test guarantees no panic/deadlock and that readiness is
	// eventually observed from a reader goroutine.
	e := newRUEMA()
	const writers = 8
	const iters = 200
	done := make(chan struct{})
	go func() {
		// Reader: hammer Predict while writers fan in samples.
		for {
			select {
			case <-done:
				return
			default:
				_ = e.Predict()
			}
		}
	}()
	base := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				e.Observe(uint64(100_000+id*1_000+i),
					base.Add(time.Duration(i)*time.Millisecond))
			}
		}(w)
	}
	wg.Wait()
	close(done)
	require.Greater(t, e.Predict(), uint64(0), "prediction must be positive after observations")
}

func TestRUEMANonMonotonicTime(t *testing.T) {
	// If for any reason a later observation carries an earlier timestamp
	// (clock skew, test fixture), Observe must not blow up with a negative
	// Δt. The behavior we want: treat the gap as zero and use the new
	// sample only minimally.
	e := newRUEMA()
	now := time.Now()
	e.Observe(100_000, now)
	e.Observe(500_000, now.Add(-1*time.Second))
	// With Δt clamped to 0, alpha = 0, so the EMA should stay at the first
	// sample's value.
	require.InDelta(t, float64(100_000), float64(e.Predict()), 1,
		"negative Δt should be clamped so the new sample has ~zero weight")
}

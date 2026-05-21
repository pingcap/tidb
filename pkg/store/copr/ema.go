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
	"math"
	"sync"
	"time"
)

// defaultRUEMATau is the decay time constant τ: sample weight drops to
// 1/e after τ.
const defaultRUEMATau = time.Second

// ruEMA is a time-aware EMA weighting older samples by exp(-Δt/τ) so long
// gaps decay stale samples. Safe for concurrent Observe/Predict.
type ruEMA struct {
	mu        sync.Mutex
	tau       time.Duration
	value     float64
	lastObsAt time.Time
}

func newRUEMA() *ruEMA {
	return &ruEMA{tau: defaultRUEMATau}
}

func (e *ruEMA) Observe(bytes uint64, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	dt := now.Sub(e.lastObsAt)
	if dt < 0 {
		dt = 0
	}
	alpha := 1 - math.Exp(-float64(dt)/float64(e.tau))
	e.value += alpha * (float64(bytes) - e.value)
	// Don't rewind on out-of-order Observes.
	if now.After(e.lastObsAt) {
		e.lastObsAt = now
	}
}

// Predict returns the current estimate.
func (e *ruEMA) Predict() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.value < 0 {
		return 0
	}
	return uint64(e.value)
}

// ruPagePredictor predicts read bytes for paging coprocessor requests.
//
// A paging request can grow by row-count, for example 128 rows -> 512 rows ->
// 2048 rows. A single page-level EMA underestimates after row-count grow
// because it uses smaller earlier pages to predict larger later pages. This
// predictor keeps three estimates and uses the largest one:
//
//   - pageEMA: recent bytes per paging response. This is the original fallback
//     signal and works well when page shape is stable.
//   - bytesPerRow: recent bytes per returned row. This bridges cold-start gaps
//     when paging grows to a row-count bucket that has no direct samples yet.
//   - buckets: bytes per paging response for each exact row-count bucket. Once
//     a bucket has samples, it tracks that bucket without being polluted by
//     smaller or larger pages.
//
// Taking the maximum is intentionally conservative for precharge: a lower
// prediction can leak RU before RC can throttle, while a higher prediction only
// queues the request earlier.
type ruPagePredictor struct {
	mu          sync.Mutex
	pageEMA     *ruEMA
	bytesPerRow *ruEMA
	buckets     map[uint64]*ruEMA
}

func newRUPagePredictor() *ruPagePredictor {
	return &ruPagePredictor{
		pageEMA:     newRUEMA(),
		bytesPerRow: newRUEMA(),
		buckets:     make(map[uint64]*ruEMA),
	}
}

func (p *ruPagePredictor) Observe(pagingRows uint64, bytes uint64, terminal bool, now time.Time) {
	if terminal || bytes == 0 {
		return
	}

	var bucket *ruEMA
	if pagingRows > 0 {
		p.mu.Lock()
		bucket = p.buckets[pagingRows]
		if bucket == nil {
			bucket = newRUEMA()
			p.buckets[pagingRows] = bucket
		}
		p.mu.Unlock()

		// Learn the row-size signal from any non-terminal page. It lets a
		// later unseen bucket scale from existing samples, e.g. 128 rows at
		// 87 KiB can predict roughly 348 KiB for the first 512-row page.
		p.bytesPerRow.Observe((bytes+pagingRows/2)/pagingRows, now)
	}

	// Keep both the global page signal and the exact row-count bucket. The
	// global signal is still useful for non-growing or sparse samples; the
	// bucket signal becomes more precise once the current pagingRows repeats.
	p.pageEMA.Observe(bytes, now)
	if bucket != nil {
		bucket.Observe(bytes, now)
	}
}

func (p *ruPagePredictor) Predict(pagingRows uint64) uint64 {
	// Start with the original page-level estimate so callers always get the
	// same baseline behavior when row-count signals are unavailable.
	predicted := p.pageEMA.Predict()

	p.mu.Lock()
	bucket := p.buckets[pagingRows]
	p.mu.Unlock()

	// If this exact row-count bucket has history, it is usually the most
	// relevant estimate for the next request with the same pagingRows.
	if bucket != nil {
		if bucketPredicted := bucket.Predict(); bucketPredicted > predicted {
			predicted = bucketPredicted
		}
	}
	if pagingRows > 0 {
		if bytesPerRow := p.bytesPerRow.Predict(); bytesPerRow > 0 {
			// Use bytes-per-row to cover the first request after grow, before
			// the new row-count bucket has its own samples.
			rowPredicted := uint64(^uint64(0))
			if bytesPerRow <= rowPredicted/pagingRows {
				rowPredicted = bytesPerRow * pagingRows
			}
			if rowPredicted > predicted {
				predicted = rowPredicted
			}
		}
	}
	return predicted
}

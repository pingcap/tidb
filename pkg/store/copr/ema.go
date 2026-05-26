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

type pageBreakReason uint8

const (
	pageBreakReasonUnknown pageBreakReason = iota
	pageBreakReasonRowLimit
	pageBreakReasonByteLimit
	pageBreakReasonRangeEnd
)

// ruPagePredictor predicts read bytes for paging coprocessor requests.
//
// A paging request can stop for different reasons. Fullscan is normally
// byte-limited by pagingSizeBytes, while the incident join workload is normally
// row-limited by pagingSize. Mixing those samples makes the predictor either
// over-precharge fullscan or under-precharge join. The predictor keeps separate
// signals for the two modes:
//
//   - pageEMA: compatibility fallback for legacy or unknown samples.
//   - bytePageEMA: actual response bytes for byte-limited pages. This may be
//     larger than pagingSizeBytes because TiKV can only stop after a batch.
//   - rowPageEMA: recent bytes for row-limited pages, used when row extrapolation
//     is not trusted.
//   - rowBytesPerRow: bytes per returned row learned only from row-limited pages.
//     It bridges cold row-count buckets, e.g. 128 rows -> 512 rows.
//   - rowBuckets: exact row-count buckets learned only from row-limited pages.
//     Once a grown bucket repeats, it should not be polluted by byte-limited
//     fullscan samples.
type ruPagePredictor struct {
	mu             sync.Mutex
	pageEMA        *ruEMA
	bytePageEMA    *ruEMA
	rowPageEMA     *ruEMA
	rowBytesPerRow *ruEMA
	rowBuckets     map[uint64]*ruEMA
	lastReason     pageBreakReason
}

func newRUPagePredictor() *ruPagePredictor {
	return &ruPagePredictor{
		pageEMA:        newRUEMA(),
		bytePageEMA:    newRUEMA(),
		rowPageEMA:     newRUEMA(),
		rowBytesPerRow: newRUEMA(),
		rowBuckets:     make(map[uint64]*ruEMA),
	}
}

func (p *ruPagePredictor) Observe(pagingRows uint64, bytes uint64, reason pageBreakReason, now time.Time) {
	if reason == pageBreakReasonRangeEnd || bytes == 0 {
		return
	}

	p.mu.Lock()
	p.lastReason = reason
	p.mu.Unlock()

	p.pageEMA.Observe(bytes, now)

	switch reason {
	case pageBreakReasonByteLimit:
		p.bytePageEMA.Observe(bytes, now)
	case pageBreakReasonRowLimit, pageBreakReasonUnknown:
		p.observeRowLimited(pagingRows, bytes, now)
	}
}

func (p *ruPagePredictor) observeRowLimited(pagingRows uint64, bytes uint64, now time.Time) {
	p.rowPageEMA.Observe(bytes, now)
	if pagingRows == 0 {
		return
	}

	p.rowBytesPerRow.Observe((bytes+pagingRows/2)/pagingRows, now)

	p.mu.Lock()
	bucket := p.rowBuckets[pagingRows]
	if bucket == nil {
		bucket = newRUEMA()
		p.rowBuckets[pagingRows] = bucket
	}
	p.mu.Unlock()
	bucket.Observe(bytes, now)
}

func (p *ruPagePredictor) Predict(pagingRows, pagingSizeBytes uint64) uint64 {
	p.mu.Lock()
	lastReason := p.lastReason
	bucket := p.rowBuckets[pagingRows]
	p.mu.Unlock()

	if lastReason == pageBreakReasonByteLimit {
		if predicted := p.bytePageEMA.Predict(); predicted > 0 {
			return predicted
		}
	}

	if pagingRows > 0 {
		if bucket != nil {
			return bucket.Predict()
		}

		if rowPredicted := p.predictByRows(pagingRows, pagingSizeBytes); rowPredicted > 0 {
			return rowPredicted
		}
	}

	if predicted := p.bytePageEMA.Predict(); predicted > 0 {
		return predicted
	}
	if predicted := p.pageEMA.Predict(); predicted > 0 {
		return predicted
	}
	// Cold-start fallback: before any page sample is available, use the
	// configured byte budget so the first paging request is still precharged.
	return pagingSizeBytes
}

func (p *ruPagePredictor) predictByRows(pagingRows, pagingSizeBytes uint64) uint64 {
	bytesPerRow := p.rowBytesPerRow.Predict()
	if bytesPerRow == 0 {
		return 0
	}
	rowPredicted := uint64(^uint64(0))
	if bytesPerRow <= rowPredicted/pagingRows {
		rowPredicted = bytesPerRow * pagingRows
	}
	if pagingSizeBytes == 0 || rowPredicted <= pagingSizeBytes {
		return rowPredicted
	}
	return p.rowPageEMA.Predict()
}

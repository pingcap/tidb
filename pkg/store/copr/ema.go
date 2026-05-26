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
// The predictor is scoped to one copIterator, so it only needs to model the
// page sequence of a single logical scan. Byte-limited pages are learned as
// page-sized bytes, while row-limited pages are learned as bytes per returned
// row and extrapolated by the next paging row count.
//
//   - pageEMA: compatibility fallback for legacy or unknown samples.
//   - bytePageEMA: actual response bytes for byte-limited pages. This may be
//     larger than pagingSizeBytes because TiKV can only stop after a batch.
//   - rowBytesPerRow: bytes per returned row learned only from row-limited
//     pages. It bridges row-count growth, e.g. 128 rows -> 512 rows.
type ruPagePredictor struct {
	mu              sync.Mutex
	pagingSizeBytes uint64
	pageEMA         *ruEMA
	bytePageEMA     *ruEMA
	rowBytesPerRow  *ruEMA
	lastReason      pageBreakReason
}

func newRUPagePredictor(pagingSizeBytes uint64) *ruPagePredictor {
	return &ruPagePredictor{
		pagingSizeBytes: pagingSizeBytes,
		pageEMA:         newRUEMA(),
		bytePageEMA:     newRUEMA(),
		rowBytesPerRow:  newRUEMA(),
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
	case pageBreakReasonRowLimit:
		p.observeRowLimited(pagingRows, bytes, now)
	}
}

func (p *ruPagePredictor) observeRowLimited(pagingRows uint64, bytes uint64, now time.Time) {
	if pagingRows == 0 {
		return
	}
	p.rowBytesPerRow.Observe((bytes+pagingRows/2)/pagingRows, now)
}

func (p *ruPagePredictor) Predict(pagingRows uint64) uint64 {
	p.mu.Lock()
	lastReason := p.lastReason
	p.mu.Unlock()

	switch lastReason {
	case pageBreakReasonByteLimit:
		if predicted := p.bytePageEMA.Predict(); predicted > 0 {
			return predicted
		}
		return p.pagingSizeBytes
	case pageBreakReasonRowLimit:
		if predicted := p.predictByRows(pagingRows); predicted > 0 {
			return predicted
		}
		return p.pagingSizeBytes
	case pageBreakReasonUnknown:
		if predicted := p.pageEMA.Predict(); predicted > 0 {
			return predicted
		}
		return p.pagingSizeBytes
	default:
		return p.pagingSizeBytes
	}
}

func (p *ruPagePredictor) predictByRows(pagingRows uint64) uint64 {
	if pagingRows == 0 {
		return 0
	}
	bytesPerRow := p.rowBytesPerRow.Predict()
	if bytesPerRow == 0 {
		return 0
	}

	rowPredicted := uint64(^uint64(0))
	if bytesPerRow <= rowPredicted/pagingRows {
		rowPredicted = bytesPerRow * pagingRows
	}
	if p.pagingSizeBytes > 0 && rowPredicted > p.pagingSizeBytes {
		return p.pagingSizeBytes
	}
	return rowPredicted
}

// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runaway

import (
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	minFlushRetryBackoff = time.Second
	maxFlushRetryBackoff = 30 * time.Second
)

type batchFlusher[K comparable, V any] struct {
	name          string
	buffer        map[K]V
	ticker        *time.Ticker
	threshold     int
	lastFlushTime time.Time
	mergeFn       func(map[K]V, K, V)
	flushFn       func(map[K]V) error
	// After a failed flush, the records are kept and no flush is tried before nextRetryTime.
	retryBackoff  time.Duration
	nextRetryTime time.Time

	batchSizeObserver   prometheus.Observer
	durationObserver    prometheus.Observer
	intervalObserver    prometheus.Observer
	flushSuccessCounter prometheus.Counter
	flushErrorCounter   prometheus.Counter
	addCounter          prometheus.Counter
}

func newBatchFlusher[K comparable, V any](
	name string,
	interval time.Duration,
	threshold int,
	mergeFn func(map[K]V, K, V),
	genSQL func(map[K]V) (string, []any),
	pool util.SessionPool,
) *batchFlusher[K, V] {
	f := &batchFlusher[K, V]{
		name:                name,
		buffer:              make(map[K]V, threshold),
		ticker:              time.NewTicker(interval),
		threshold:           threshold,
		mergeFn:             mergeFn,
		batchSizeObserver:   metrics.RunawayFlusherBatchSizeHistogram.WithLabelValues(name),
		durationObserver:    metrics.RunawayFlusherDurationHistogram.WithLabelValues(name),
		intervalObserver:    metrics.RunawayFlusherIntervalHistogram.WithLabelValues(name),
		flushSuccessCounter: metrics.RunawayFlusherCounter.WithLabelValues(name, metrics.LblOK),
		flushErrorCounter:   metrics.RunawayFlusherCounter.WithLabelValues(name, metrics.LblError),
		addCounter:          metrics.RunawayFlusherAddCounter.WithLabelValues(name),
	}
	f.flushFn = func(buffer map[K]V) error {
		count := len(buffer)
		if count == 0 {
			return nil
		}
		sql, params := genSQL(buffer)
		if _, err := ExecRCRestrictedSQL(pool, sql, params); err != nil {
			logutil.BgLogger().Error("batch flush failed",
				zap.String("name", name),
				zap.Int("count", count),
				zap.Error(err))
			return err
		}
		return nil
	}
	return f
}

func (f *batchFlusher[K, V]) tickerCh() <-chan time.Time {
	return f.ticker.C
}

func (f *batchFlusher[K, V]) stop() {
	logutil.BgLogger().Info("flushing remaining records before stop", zap.String("name", f.name))
	f.nextRetryTime = time.Time{}
	f.flush()
	logutil.BgLogger().Info("stopped flusher", zap.String("name", f.name))
	f.ticker.Stop()
}

func (f *batchFlusher[K, V]) add(key K, value V) {
	f.addCounter.Inc()
	f.mergeFn(f.buffer, key, value)
	shouldFlush := len(f.buffer) >= f.threshold
	failpoint.Inject("FastRunawayGC", func() {
		shouldFlush = true
	})
	if shouldFlush {
		f.flush()
	}
}

func (f *batchFlusher[K, V]) flush() {
	failpoint.Inject("skipFlush", func() {
		failpoint.Return()
	})
	if len(f.buffer) == 0 {
		// Reset lastFlushTime so the next real flush after an idle period
		// does not record the idle gap as flush interval.
		if !f.lastFlushTime.IsZero() {
			f.lastFlushTime = time.Time{}
		}
		return
	}

	now := time.Now()
	if now.Before(f.nextRetryTime) {
		return
	}
	if !f.lastFlushTime.IsZero() {
		f.intervalObserver.Observe(now.Sub(f.lastFlushTime).Seconds())
	}
	f.lastFlushTime = now

	// Flush in batches of at most threshold records, so that the records kept by failed flushes do not make the
	// statements larger.
	for len(f.buffer) > 0 {
		batch := f.buffer
		if len(batch) > f.threshold {
			batch = make(map[K]V, f.threshold)
			for k, v := range f.buffer {
				batch[k] = v
				if len(batch) == f.threshold {
					break
				}
			}
		}
		start := time.Now()
		err := f.flushFn(batch)
		f.batchSizeObserver.Observe(float64(len(batch)))
		f.durationObserver.Observe(time.Since(start).Seconds())
		if err != nil {
			f.flushErrorCounter.Inc()
			// Keep the records and retry them after a backoff.
			f.retryBackoff = min(max(2*f.retryBackoff, minFlushRetryBackoff), maxFlushRetryBackoff)
			f.nextRetryTime = time.Now().Add(f.retryBackoff)
			return
		}
		f.flushSuccessCounter.Inc()
		if len(batch) == len(f.buffer) {
			f.buffer = make(map[K]V, f.threshold)
		} else {
			for k := range batch {
				delete(f.buffer, k)
			}
		}
	}
	f.retryBackoff = 0
}

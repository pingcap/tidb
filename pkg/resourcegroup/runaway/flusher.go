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
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// flushThreshold is the number of unique keys that triggers a flush signal.
	flushThreshold = 512
	// maxBufferSize caps the number of unique keys in the buffer to prevent
	// unbounded memory growth when flush is slow and unique keys are many.
	// Records whose key already exists are still merged (deduplication).
	//
	// Per-flusher worst-case memory at maxBufferSize (32768 entries):
	//   runawayRecordFlusher:    key ~64B + val(ptr 8B + Record ~232B) ≈ ~304B × 32768 ≈ 9.5 MB
	//   quarantineRecordFlusher: key ~48B + val(ptr 8B + QuarantineRecord ~200B) ≈ ~256B × 32768 ≈ 8.0 MB
	//   staleQuarantineFlusher:  key 8B + val(ptr 8B + QuarantineRecord ~200B)  ≈ ~216B × 32768 ≈ 6.8 MB
	// Total worst-case across all three flushers: ~24 MB.
	maxBufferSize = 32 * 1024
)

type batchFlusher[K comparable, V any] struct {
	mu            sync.Mutex
	name          string
	buffer        map[K]V
	ticker        *time.Ticker
	flushCh       chan struct{} // capacity 1, threshold-triggered signal
	stopCh        chan struct{} // close to trigger stop
	done          chan struct{} // closed after run() exits
	lastFlushTime time.Time
	mergeFn       func(map[K]V, K, V)
	flushFn       func(map[K]V) error

	batchSizeObserver   prometheus.Observer
	durationObserver    prometheus.Observer
	intervalObserver    prometheus.Observer
	flushSuccessCounter prometheus.Counter
	flushErrorCounter   prometheus.Counter
	addCounter          prometheus.Counter
	dropCounter         prometheus.Counter
}

func newBatchFlusher[K comparable, V any](
	name string,
	interval time.Duration,
	mergeFn func(map[K]V, K, V),
	genSQL func(map[K]V) (string, []any),
	pool util.SessionPool,
) *batchFlusher[K, V] {
	f := &batchFlusher[K, V]{
		name:                name,
		buffer:              make(map[K]V, flushThreshold),
		ticker:              time.NewTicker(interval),
		flushCh:             make(chan struct{}, 1),
		stopCh:              make(chan struct{}),
		done:                make(chan struct{}),
		mergeFn:             mergeFn,
		batchSizeObserver:   metrics.RunawayFlusherBatchSizeHistogram.WithLabelValues(name),
		durationObserver:    metrics.RunawayFlusherDurationHistogram.WithLabelValues(name),
		intervalObserver:    metrics.RunawayFlusherIntervalHistogram.WithLabelValues(name),
		flushSuccessCounter: metrics.RunawayFlusherCounter.WithLabelValues(name, metrics.LblOK),
		flushErrorCounter:   metrics.RunawayFlusherCounter.WithLabelValues(name, metrics.LblError),
		addCounter:          metrics.RunawayFlusherAddCounter.WithLabelValues(name),
		dropCounter:         metrics.RunawayFlusherDropCounter.WithLabelValues(name),
	}
	f.flushFn = func(buffer map[K]V) error {
		sql, params := genSQL(buffer)
		if _, err := ExecRCRestrictedSQL(pool, sql, params); err != nil {
			logutil.BgLogger().Error("batch flush failed",
				zap.String("name", name),
				zap.Int("count", len(buffer)),
				zap.Error(err))
			return err
		}
		return nil
	}
	return f
}

func (f *batchFlusher[K, V]) run() {
	defer func() {
		logutil.BgLogger().Info("flushing remaining records before stop", zap.String("name", f.name))
		f.flush()
		logutil.BgLogger().Info("stopped flusher", zap.String("name", f.name))
		close(f.done)
	}()
	for {
		select {
		case <-f.stopCh:
			return
		case <-f.ticker.C:
			f.flush()
		case <-f.flushCh:
			f.flush()
		}
	}
}

func (f *batchFlusher[K, V]) stop() {
	f.ticker.Stop()
	close(f.stopCh)
	<-f.done
}

func (f *batchFlusher[K, V]) notifyFlush() {
	select {
	case f.flushCh <- struct{}{}:
	default:
	}
}

func (f *batchFlusher[K, V]) add(key K, value V) {
	f.addCounter.Inc()
	f.mu.Lock()
	if _, exists := f.buffer[key]; !exists && len(f.buffer) >= maxBufferSize {
		f.mu.Unlock()
		f.dropCounter.Inc()
		return
	}
	f.mergeFn(f.buffer, key, value)
	shouldFlush := len(f.buffer) >= flushThreshold
	f.mu.Unlock()
	failpoint.Inject("FastRunawayGC", func() {
		shouldFlush = true
	})
	if shouldFlush {
		f.notifyFlush()
	}
}

func (f *batchFlusher[K, V]) bufferLen() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.buffer)
}

func (f *batchFlusher[K, V]) flush() {
	failpoint.Inject("skipFlush", func() {
		failpoint.Return()
	})
	f.mu.Lock()
	batchSize := len(f.buffer)
	if batchSize == 0 {
		f.mu.Unlock()
		return
	}
	toFlush := f.buffer
	f.buffer = make(map[K]V, flushThreshold)
	now := time.Now()
	if !f.lastFlushTime.IsZero() {
		f.intervalObserver.Observe(now.Sub(f.lastFlushTime).Seconds())
	}
	f.lastFlushTime = now
	f.mu.Unlock()

	err := f.flushFn(toFlush)
	duration := time.Since(now)
	f.batchSizeObserver.Observe(float64(batchSize))
	f.durationObserver.Observe(duration.Seconds())
	if err != nil {
		f.flushErrorCounter.Inc()
	} else {
		f.flushSuccessCounter.Inc()
	}
}

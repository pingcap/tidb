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
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type batchFlusher[K comparable, V any] struct {
	name      string
	buffer    map[K]V
	timer     *time.Timer
	interval  time.Duration
	threshold int
	fired     bool
	mergeFn   func(map[K]V, K, V)
	flushFn   func(map[K]V)
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
		name:      name,
		buffer:    make(map[K]V, threshold),
		timer:     time.NewTimer(interval),
		interval:  interval,
		threshold: threshold,
		mergeFn:   mergeFn,
	}
	f.flushFn = func(buffer map[K]V) {
		sql, params := genSQL(buffer)
		if _, err := ExecRCRestrictedSQL(pool, sql, params); err != nil {
			logutil.BgLogger().Error("batch flush failed",
				zap.String("name", name),
				zap.Error(err),
				zap.Int("count", len(buffer)))
		}
	}
	return f
}

func (f *batchFlusher[K, V]) timerChan() <-chan time.Time {
	return f.timer.C
}

func (f *batchFlusher[K, V]) onTimer() {
	f.flush()
	f.fired = true
}

func (f *batchFlusher[K, V]) add(key K, value V) {
	f.mergeFn(f.buffer, key, value)
	failpoint.Inject("FastRunawayGC", func() {
		f.flush()
	})
	if len(f.buffer) >= f.threshold {
		f.flush()
	} else if f.fired {
		f.fired = false
		f.timer.Reset(f.interval)
	}
}

func (f *batchFlusher[K, V]) flush() {
	failpoint.Inject("skipFlush", func() {
		failpoint.Return()
	})
	if len(f.buffer) == 0 {
		return
	}
	f.flushFn(f.buffer)
	f.buffer = make(map[K]V, f.threshold)
}

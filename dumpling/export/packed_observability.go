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

package export

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
)

// packedExportObservation is the single observability handle carried by the
// packed-export path. All counters and timing state stay in this file.
type packedExportObservation struct {
	tctx        *tcontext.Context
	started     time.Time
	scans       packedScanTotals
	decodeNanos atomic.Int64
	decoded     atomic.Uint64
	decodeErr   atomic.Uint64
}

type packedScanTotals struct {
	started  atomic.Uint64
	finished atomic.Uint64
	failed   atomic.Uint64
	first    atomic.Uint64
	kv       atomic.Uint64
	keyBytes atomic.Uint64
	valBytes atomic.Uint64
	firstRow atomic.Int64
	read     atomic.Int64
}

func newPackedExportObservation(tctx *tcontext.Context) *packedExportObservation {
	return &packedExportObservation{tctx: tctx, started: time.Now()}
}

func (o *packedExportObservation) decode(run func() error) error {
	started := time.Now()
	err := run()
	o.decodeNanos.Add(time.Since(started).Nanoseconds())
	o.decoded.Add(1)
	if err != nil {
		o.decodeErr.Add(1)
	}
	return err
}

func (o *packedExportObservation) finish(resultErr error) {
	finished := o.scans.finished.Load()
	failed := o.scans.failed.Load()
	o.tctx.L().Info("packed export perf",
		zap.String("part", "scan"),
		zap.Uint64("scans", o.scans.started.Load()),
		zap.Uint64("done", finished),
		zap.Uint64("failed", failed),
		zap.Duration("first_sum", time.Duration(o.scans.firstRow.Load())),
		zap.Uint64("first_n", o.scans.first.Load()),
		zap.Duration("read_sum", time.Duration(o.scans.read.Load())))
	o.tctx.L().Info("packed export perf",
		zap.String("part", "data"),
		zap.Uint64("kv", o.scans.kv.Load()),
		zap.Uint64("kv_bytes", o.scans.keyBytes.Load()+o.scans.valBytes.Load()),
		zap.Duration("decode_sum", time.Duration(o.decodeNanos.Load())),
		zap.Uint64("decoded", o.decoded.Load()),
		zap.Uint64("decode_err", o.decodeErr.Load()),
		zap.Duration("wall", time.Since(o.started)),
		zap.Bool("ok", resultErr == nil))
}

type packedScanContext struct {
	parent     *packedExportObservation
	started    time.Time
	firstDur   time.Duration
	readDur    time.Duration
	kv         uint64
	keyBytes   uint64
	valBytes   uint64
	finishOnce sync.Once
}

func newPackedScanContext(parent *packedExportObservation) *packedScanContext {
	observation := &packedScanContext{parent: parent, started: time.Now()}
	if parent != nil {
		parent.scans.started.Add(1)
	}
	return observation
}

func (o *packedScanContext) readRow(
	input io.Reader,
	keyBuffer, valueBuffer []byte,
) (key, value []byte, end bool, err error) {
	started := time.Now()
	key, value, end, err = readPackedRow(input, keyBuffer, valueBuffer)
	o.readDur += time.Since(started)
	if err == nil && !end {
		if o.kv == 0 {
			o.firstDur = time.Since(o.started)
		}
		o.kv++
		o.keyBytes += uint64(len(key))
		o.valBytes += uint64(len(value))
	}
	return key, value, end, err
}

func (o *packedExportObservation) forwardCSE(line string) {
	if o == nil {
		return
	}
	o.tctx.L().Debug(line)
}

func (o *packedScanContext) finish(err error) {
	o.finishOnce.Do(func() {
		if o.parent == nil {
			return
		}
		totals := &o.parent.scans
		totals.finished.Add(1)
		if err != nil {
			totals.failed.Add(1)
		}
		if o.firstDur > 0 {
			totals.first.Add(1)
			totals.firstRow.Add(o.firstDur.Nanoseconds())
		}
		totals.kv.Add(o.kv)
		totals.keyBytes.Add(o.keyBytes)
		totals.valBytes.Add(o.valBytes)
		totals.read.Add(o.readDur.Nanoseconds())
	})
}

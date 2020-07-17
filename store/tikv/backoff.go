// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// NoJitter makes the backoff sequence strict exponential.
	NoJitter = 1 + iota
	// FullJitter applies random factors to strict exponential.
	FullJitter
	// EqualJitter is also randomized, but prevents very short sleeps.
	EqualJitter
	// DecorrJitter increases the maximum jitter based on the last random value.
	DecorrJitter
)

var (
	tikvBackoffCounterRPC            = metrics.TiKVBackoffCounter.WithLabelValues("tikvRPC")
	tikvBackoffCounterLock           = metrics.TiKVBackoffCounter.WithLabelValues("txnLock")
	tikvBackoffCounterLockFast       = metrics.TiKVBackoffCounter.WithLabelValues("tikvLockFast")
	tikvBackoffCounterPD             = metrics.TiKVBackoffCounter.WithLabelValues("pdRPC")
	tikvBackoffCounterRegionMiss     = metrics.TiKVBackoffCounter.WithLabelValues("regionMiss")
	tikvBackoffCounterUpdateLeader   = metrics.TiKVBackoffCounter.WithLabelValues("updateLeader")
	tikvBackoffCounterServerBusy     = metrics.TiKVBackoffCounter.WithLabelValues("serverBusy")
	tikvBackoffCounterStaleCmd       = metrics.TiKVBackoffCounter.WithLabelValues("staleCommand")
	tikvBackoffCounterEmpty          = metrics.TiKVBackoffCounter.WithLabelValues("")
	tikvBackoffHistogramRPC          = metrics.TiKVBackoffHistogram.WithLabelValues("tikvRPC")
	tikvBackoffHistogramLock         = metrics.TiKVBackoffHistogram.WithLabelValues("txnLock")
	tikvBackoffHistogramLockFast     = metrics.TiKVBackoffHistogram.WithLabelValues("tikvLockFast")
	tikvBackoffHistogramPD           = metrics.TiKVBackoffHistogram.WithLabelValues("pdRPC")
	tikvBackoffHistogramRegionMiss   = metrics.TiKVBackoffHistogram.WithLabelValues("regionMiss")
	tikvBackoffHistogramUpdateLeader = metrics.TiKVBackoffHistogram.WithLabelValues("updateLeader")
	tikvBackoffHistogramServerBusy   = metrics.TiKVBackoffHistogram.WithLabelValues("serverBusy")
	tikvBackoffHistogramStaleCmd     = metrics.TiKVBackoffHistogram.WithLabelValues("staleCommand")
	tikvBackoffHistogramEmpty        = metrics.TiKVBackoffHistogram.WithLabelValues("")
)

func (t backoffType) metric() (prometheus.Counter, prometheus.Observer) {
	switch t {
	case boTiKVRPC:
		return tikvBackoffCounterRPC, tikvBackoffHistogramRPC
	case BoTxnLock:
		return tikvBackoffCounterLock, tikvBackoffHistogramLock
	case boTxnLockFast:
		return tikvBackoffCounterLockFast, tikvBackoffHistogramLockFast
	case BoPDRPC:
		return tikvBackoffCounterPD, tikvBackoffHistogramPD
	case BoRegionMiss:
		return tikvBackoffCounterRegionMiss, tikvBackoffHistogramRegionMiss
	case BoUpdateLeader:
		return tikvBackoffCounterUpdateLeader, tikvBackoffHistogramUpdateLeader
	case boServerBusy:
		return tikvBackoffCounterServerBusy, tikvBackoffHistogramServerBusy
	case boStaleCmd:
		return tikvBackoffCounterStaleCmd, tikvBackoffHistogramStaleCmd
	}
	return tikvBackoffCounterEmpty, tikvBackoffHistogramEmpty
}

// NewBackoffFn creates a backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
func NewBackoffFn(base, cap, jitter int) func(ctx context.Context, maxSleepMs int) int {
	if base < 2 {
		// Top prevent panic in 'rand.Intn'.
		base = 2
	}
	attempts := 0
	lastSleep := base
	return func(ctx context.Context, maxSleepMs int) int {
		var sleep int
		switch jitter {
		case NoJitter:
			sleep = expo(base, cap, attempts)
		case FullJitter:
			v := expo(base, cap, attempts)
			sleep = rand.Intn(v)
		case EqualJitter:
			v := expo(base, cap, attempts)
			sleep = v/2 + rand.Intn(v/2)
		case DecorrJitter:
			sleep = int(math.Min(float64(cap), float64(base+rand.Intn(lastSleep*3-base))))
		}
		logutil.Logger(context.Background()).Debug("backoff",
			zap.Int("base", base),
			zap.Int("sleep", sleep))

		realSleep := sleep
		// when set maxSleepMs >= 0 in `tikv.BackoffWithMaxSleep` will force sleep maxSleepMs milliseconds.
		if maxSleepMs >= 0 && realSleep > maxSleepMs {
			realSleep = maxSleepMs
		}
		select {
		case <-time.After(time.Duration(realSleep) * time.Millisecond):
			attempts++
			lastSleep = sleep
			return realSleep
		case <-ctx.Done():
			return 0
		}
	}
}

func expo(base, cap, n int) int {
	return int(math.Min(float64(cap), float64(base)*math.Pow(2.0, float64(n))))
}

type backoffType int

// Back off types.
const (
	boTiKVRPC backoffType = iota
	BoTxnLock
	boTxnLockFast
	BoPDRPC
	BoRegionMiss
	BoUpdateLeader
	boServerBusy
	boStaleCmd
)

func (t backoffType) createFn(vars *kv.Variables) func(context.Context, int) int {
	if vars.Hook != nil {
		vars.Hook(t.String(), vars)
	}
	switch t {
	case boTiKVRPC:
		return NewBackoffFn(100, 2000, EqualJitter)
	case BoTxnLock:
		return NewBackoffFn(200, 3000, EqualJitter)
	case boTxnLockFast:
		return NewBackoffFn(vars.BackoffLockFast, 3000, EqualJitter)
	case BoPDRPC:
		return NewBackoffFn(500, 3000, EqualJitter)
	case BoRegionMiss:
		// change base time to 2ms, because it may recover soon.
		return NewBackoffFn(2, 500, NoJitter)
	case BoUpdateLeader:
		return NewBackoffFn(1, 10, NoJitter)
	case boServerBusy:
		return NewBackoffFn(2000, 10000, EqualJitter)
	case boStaleCmd:
		return NewBackoffFn(2, 1000, NoJitter)
	}
	return nil
}

func (t backoffType) String() string {
	switch t {
	case boTiKVRPC:
		return "tikvRPC"
	case BoTxnLock:
		return "txnLock"
	case boTxnLockFast:
		return "txnLockFast"
	case BoPDRPC:
		return "pdRPC"
	case BoRegionMiss:
		return "regionMiss"
	case BoUpdateLeader:
		return "updateLeader"
	case boServerBusy:
		return "serverBusy"
	case boStaleCmd:
		return "staleCommand"
	}
	return ""
}

func (t backoffType) TError() error {
	switch t {
	case boTiKVRPC:
		return ErrTiKVServerTimeout
	case BoTxnLock, boTxnLockFast:
		return ErrResolveLockTimeout
	case BoPDRPC:
		return ErrPDServerTimeout
	case BoRegionMiss, BoUpdateLeader:
		return ErrRegionUnavailable
	case boServerBusy:
		return ErrTiKVServerBusy
	case boStaleCmd:
		return ErrTiKVStaleCommand
	}
	return terror.ClassTiKV.New(mysql.ErrUnknown, mysql.MySQLErrName[mysql.ErrUnknown])
}

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	copBuildTaskMaxBackoff         = 5000
	tsoMaxBackoff                  = 15000
	scannerNextMaxBackoff          = 20000
	batchGetMaxBackoff             = 20000
	copNextMaxBackoff              = 20000
	getMaxBackoff                  = 20000
	prewriteMaxBackoff             = 20000
	cleanupMaxBackoff              = 20000
	GcOneRegionMaxBackoff          = 20000
	GcResolveLockMaxBackoff        = 100000
	deleteRangeOneRegionMaxBackoff = 100000
	rawkvMaxBackoff                = 20000
	splitRegionBackoff             = 20000
	maxSplitRegionsBackoff         = 120000
	scatterRegionBackoff           = 20000
	waitScatterRegionFinishBackoff = 120000
	locateRegionMaxBackoff         = 20000
	pessimisticLockMaxBackoff      = 10000
	pessimisticRollbackMaxBackoff  = 10000
)

// CommitMaxBackoff is max sleep time of the 'commit' command
var CommitMaxBackoff = 41000

// Backoffer is a utility for retrying queries.
type Backoffer struct {
	ctx context.Context

	fn         map[backoffType]func(context.Context, int) int
	maxSleep   int
	totalSleep int
	errors     []error
	types      []fmt.Stringer
	vars       *kv.Variables
	noop       bool
}

// txnStartKey is a key for transaction start_ts info in context.Context.
const txnStartKey = "_txn_start_key"

// NewBackoffer creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	return &Backoffer{
		ctx:      ctx,
		maxSleep: maxSleep,
		vars:     kv.DefaultVars,
	}
}

// NewNoopBackoff create a Backoffer do nothing just return error directly
func NewNoopBackoff(ctx context.Context) *Backoffer {
	return &Backoffer{ctx: ctx, noop: true}
}

// WithVars sets the kv.Variables to the Backoffer and return it.
func (b *Backoffer) WithVars(vars *kv.Variables) *Backoffer {
	if vars != nil {
		b.vars = vars
	}
	// maxSleep is the max sleep time in millisecond.
	// When it is multiplied by BackOffWeight, it should not be greater than MaxInt32.
	if math.MaxInt32/b.vars.BackOffWeight >= b.maxSleep {
		b.maxSleep *= b.vars.BackOffWeight
	}
	return b
}

// Backoff sleeps a while base on the backoffType and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *Backoffer) Backoff(typ backoffType, err error) error {
	return b.BackoffWithMaxSleep(typ, -1, err)
}

// BackoffWithMaxSleep sleeps a while base on the backoffType and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithMaxSleep(typ backoffType, maxSleepMs int, err error) error {
	if strings.Contains(err.Error(), mismatchClusterID) {
		logutil.Logger(context.Background()).Fatal("critical error", zap.Error(err))
	}
	select {
	case <-b.ctx.Done():
		return errors.Trace(err)
	default:
	}

	b.errors = append(b.errors, errors.Errorf("%s at %s", err.Error(), time.Now().Format(time.RFC3339Nano)))
	b.types = append(b.types, typ)
	if b.noop || (b.maxSleep > 0 && b.totalSleep >= b.maxSleep) {
		errMsg := fmt.Sprintf("%s backoffer.maxSleep %dms is exceeded, errors:", typ.String(), b.maxSleep)
		for i, err := range b.errors {
			// Print only last 3 errors for non-DEBUG log levels.
			if log.GetLevel() == zapcore.DebugLevel || i >= len(b.errors)-3 {
				errMsg += "\n" + err.Error()
			}
		}
		logutil.Logger(context.Background()).Warn(errMsg)
		// Use the first backoff type to generate a MySQL error.
		return b.types[0].(backoffType).TError()
	}

	backoffCounter, backoffDuration := typ.metric()
	backoffCounter.Inc()
	// Lazy initialize.
	if b.fn == nil {
		b.fn = make(map[backoffType]func(context.Context, int) int)
	}
	f, ok := b.fn[typ]
	if !ok {
		f = typ.createFn(b.vars)
		b.fn[typ] = f
	}

	realSleep := f(b.ctx, maxSleepMs)
	backoffDuration.Observe(float64(realSleep) / 1000)
	b.totalSleep += realSleep

	if b.vars != nil && b.vars.Killed != nil {
		if atomic.LoadUint32(b.vars.Killed) == 1 {
			return ErrQueryInterrupted
		}
	}

	var startTs interface{}
	if ts := b.ctx.Value(txnStartKey); ts != nil {
		startTs = ts
	}
	logutil.Logger(context.Background()).Debug("retry later",
		zap.Error(err),
		zap.Int("totalSleep", b.totalSleep),
		zap.Int("maxSleep", b.maxSleep),
		zap.Stringer("type", typ),
		zap.Reflect("txnStartTS", startTs))
	return nil
}

func (b *Backoffer) String() string {
	if b.totalSleep == 0 {
		return ""
	}
	return fmt.Sprintf(" backoff(%dms %v)", b.totalSleep, b.types)
}

// Clone creates a new Backoffer which keeps current Backoffer's sleep time and errors, and shares
// current Backoffer's context.
func (b *Backoffer) Clone() *Backoffer {
	return &Backoffer{
		ctx:        b.ctx,
		maxSleep:   b.maxSleep,
		totalSleep: b.totalSleep,
		errors:     b.errors,
		vars:       b.vars,
	}
}

// Fork creates a new Backoffer which keeps current Backoffer's sleep time and errors, and holds
// a child context of current Backoffer's context.
func (b *Backoffer) Fork() (*Backoffer, context.CancelFunc) {
	ctx, cancel := context.WithCancel(b.ctx)
	return &Backoffer{
		ctx:        ctx,
		maxSleep:   b.maxSleep,
		totalSleep: b.totalSleep,
		errors:     b.errors,
		vars:       b.vars,
	}, cancel
}

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

package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/util"
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

func (t BackoffType) metric() prometheus.Observer {
	switch t {
	// TODO: distinguish tikv and tiflash in metrics
	case boTiKVRPC, BoTiFlashRPC:
		return metrics.BackoffHistogramRPC
	case BoTxnLock:
		return metrics.BackoffHistogramLock
	case BoTxnLockFast:
		return metrics.BackoffHistogramLockFast
	case BoPDRPC:
		return metrics.BackoffHistogramPD
	case BoRegionMiss:
		return metrics.BackoffHistogramRegionMiss
	case BoTiKVServerBusy, BoTiFlashServerBusy:
		return metrics.BackoffHistogramServerBusy
	case BoStaleCmd:
		return metrics.BackoffHistogramStaleCmd
	}
	return metrics.BackoffHistogramEmpty
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
		logutil.BgLogger().Debug("backoff",
			zap.Int("base", base),
			zap.Int("sleep", sleep),
			zap.Int("attempts", attempts))

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

// BackoffType defines the backoff type.
type BackoffType int

// Back off types.
const (
	boTiKVRPC BackoffType = iota
	BoTiFlashRPC
	BoTxnLock
	BoTxnLockFast
	BoPDRPC
	BoRegionMiss
	BoTiKVServerBusy
	BoTiFlashServerBusy
	BoTxnNotFound
	BoStaleCmd
	BoMaxTsNotSynced
)

func (t BackoffType) createFn(vars *kv.Variables) func(context.Context, int) int {
	if vars.Hook != nil {
		vars.Hook(t.String(), vars)
	}
	switch t {
	case boTiKVRPC, BoTiFlashRPC:
		return NewBackoffFn(100, 2000, EqualJitter)
	case BoTxnLock:
		return NewBackoffFn(200, 3000, EqualJitter)
	case BoTxnLockFast:
		return NewBackoffFn(vars.BackoffLockFast, 3000, EqualJitter)
	case BoPDRPC:
		return NewBackoffFn(500, 3000, EqualJitter)
	case BoRegionMiss:
		// change base time to 2ms, because it may recover soon.
		return NewBackoffFn(2, 500, NoJitter)
	case BoTxnNotFound:
		return NewBackoffFn(2, 500, NoJitter)
	case BoTiKVServerBusy, BoTiFlashServerBusy:
		return NewBackoffFn(2000, 10000, EqualJitter)
	case BoStaleCmd:
		return NewBackoffFn(2, 1000, NoJitter)
	case BoMaxTsNotSynced:
		return NewBackoffFn(2, 500, NoJitter)
	}
	return nil
}

func (t BackoffType) String() string {
	switch t {
	case boTiKVRPC:
		return "tikvRPC"
	case BoTiFlashRPC:
		return "tiflashRPC"
	case BoTxnLock:
		return "txnLock"
	case BoTxnLockFast:
		return "txnLockFast"
	case BoPDRPC:
		return "pdRPC"
	case BoRegionMiss:
		return "regionMiss"
	case BoTiKVServerBusy:
		return "tikvServerBusy"
	case BoTiFlashServerBusy:
		return "tiflashServerBusy"
	case BoStaleCmd:
		return "staleCommand"
	case BoTxnNotFound:
		return "txnNotFound"
	case BoMaxTsNotSynced:
		return "maxTsNotSynced"
	}
	return ""
}

// TError returns pingcap/error of the backoff type.
func (t BackoffType) TError() error {
	switch t {
	case boTiKVRPC:
		return tikverr.ErrTiKVServerTimeout
	case BoTiFlashRPC:
		return tikverr.ErrTiFlashServerTimeout
	case BoTxnLock, BoTxnLockFast, BoTxnNotFound:
		return tikverr.ErrResolveLockTimeout
	case BoPDRPC:
		return tikverr.NewErrPDServerTimeout("")
	case BoRegionMiss:
		return tikverr.ErrRegionUnavailable
	case BoTiKVServerBusy:
		return tikverr.ErrTiKVServerBusy
	case BoTiFlashServerBusy:
		return tikverr.ErrTiFlashServerBusy
	case BoStaleCmd:
		return tikverr.ErrTiKVStaleCommand
	case BoMaxTsNotSynced:
		return tikverr.ErrTiKVMaxTimestampNotSynced
	}
	return tikverr.ErrUnknown
}

// Backoffer is a utility for retrying queries.
type Backoffer struct {
	ctx context.Context

	fn         map[BackoffType]func(context.Context, int) int
	maxSleep   int
	totalSleep int
	errors     []error
	types      []fmt.Stringer
	vars       *kv.Variables
	noop       bool

	backoffSleepMS map[BackoffType]int
	backoffTimes   map[BackoffType]int
}

type txnStartCtxKeyType struct{}

// TxnStartKey is a key for transaction start_ts info in context.Context.
var TxnStartKey interface{} = txnStartCtxKeyType{}

// NewBackoffer (Deprecated) creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	return &Backoffer{
		ctx:      ctx,
		maxSleep: maxSleep,
		vars:     kv.DefaultVars,
	}
}

// NewBackofferWithVars creates a Backoffer with maximum sleep time(in ms) and kv.Variables.
func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer {
	return NewBackoffer(ctx, maxSleep).withVars(vars)
}

// NewNoopBackoff create a Backoffer do nothing just return error directly
func NewNoopBackoff(ctx context.Context) *Backoffer {
	return &Backoffer{ctx: ctx, noop: true}
}

// withVars sets the kv.Variables to the Backoffer and return it.
func (b *Backoffer) withVars(vars *kv.Variables) *Backoffer {
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
func (b *Backoffer) Backoff(typ BackoffType, err error) error {
	if span := opentracing.SpanFromContext(b.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("tikv.backoff.%s", typ), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(b.ctx, span1)
	}
	return b.BackoffWithMaxSleep(typ, -1, err)
}

// BackoffTiKVRPC calls Backoff with boTiKVRPC.
func (b *Backoffer) BackoffTiKVRPC(err error) error {
	return b.Backoff(boTiKVRPC, err)
}

// BackoffWithMaxSleep sleeps a while base on the backoffType and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithMaxSleep(typ BackoffType, maxSleepMs int, err error) error {
	if strings.Contains(err.Error(), tikverr.MismatchClusterID) {
		logutil.BgLogger().Fatal("critical error", zap.Error(err))
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
		logutil.BgLogger().Warn(errMsg)
		// Use the first backoff type to generate a MySQL error.
		return b.types[0].(BackoffType).TError()
	}

	// Lazy initialize.
	if b.fn == nil {
		b.fn = make(map[BackoffType]func(context.Context, int) int)
	}
	f, ok := b.fn[typ]
	if !ok {
		f = typ.createFn(b.vars)
		b.fn[typ] = f
	}

	realSleep := f(b.ctx, maxSleepMs)
	typ.metric().Observe(float64(realSleep) / 1000)
	b.totalSleep += realSleep
	if b.backoffSleepMS == nil {
		b.backoffSleepMS = make(map[BackoffType]int)
	}
	b.backoffSleepMS[typ] += realSleep
	if b.backoffTimes == nil {
		b.backoffTimes = make(map[BackoffType]int)
	}
	b.backoffTimes[typ]++

	stmtExec := b.ctx.Value(util.ExecDetailsKey)
	if stmtExec != nil {
		detail := stmtExec.(*util.ExecDetails)
		atomic.AddInt64(&detail.BackoffDuration, int64(realSleep)*int64(time.Millisecond))
		atomic.AddInt64(&detail.BackoffCount, 1)
	}

	if b.vars != nil && b.vars.Killed != nil {
		if atomic.LoadUint32(b.vars.Killed) == 1 {
			return tikverr.ErrQueryInterrupted
		}
	}

	var startTs interface{}
	if ts := b.ctx.Value(TxnStartKey); ts != nil {
		startTs = ts
	}
	logutil.Logger(b.ctx).Debug("retry later",
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

// GetVars returns the binded vars.
func (b *Backoffer) GetVars() *kv.Variables {
	return b.vars
}

// GetTotalSleep returns total sleep time.
func (b *Backoffer) GetTotalSleep() int {
	return b.totalSleep
}

// GetTypes returns type list.
func (b *Backoffer) GetTypes() []fmt.Stringer {
	return b.types
}

// GetCtx returns the binded context.
func (b *Backoffer) GetCtx() context.Context {
	return b.ctx
}

// SetCtx sets the binded context to ctx.
func (b *Backoffer) SetCtx(ctx context.Context) {
	b.ctx = ctx
}

// GetBackoffTimes returns a map contains backoff time count by type.
func (b *Backoffer) GetBackoffTimes() map[BackoffType]int {
	return b.backoffTimes
}

// GetBackoffSleepMS returns a map contains backoff sleep time by type.
func (b *Backoffer) GetBackoffSleepMS() map[BackoffType]int {
	return b.backoffSleepMS
}

// HasErrors returns true if b.errors is not empty.
func (b *Backoffer) HasErrors() bool {
	return len(b.errors) != 0
}

// ErrorsNum returns the number of errors.
func (b *Backoffer) ErrorsNum() int {
	return len(b.errors)
}

// SetVarsHook sets the vars.Hook is used for test to verify the variable take effect.
func (b *Backoffer) SetVarsHook(name string, vars *kv.Variables) {
	if b.vars != nil && b.vars.Hook != nil {
		b.vars.Hook(name, vars)
	}
}

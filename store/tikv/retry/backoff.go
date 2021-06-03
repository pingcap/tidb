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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Backoffer is a utility for retrying queries.
type Backoffer struct {
	ctx context.Context

	fn         map[string]backoffFn
	maxSleep   int
	totalSleep int
	errors     []error
	configs    []*Config
	vars       *kv.Variables
	noop       bool

	backoffSleepMS map[string]int
	backoffTimes   map[string]int
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

// Backoff sleeps a while base on the Config and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *Backoffer) Backoff(cfg *Config, err error) error {
	if span := opentracing.SpanFromContext(b.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("tikv.backoff.%s", cfg), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(b.ctx, span1)
	}
	return b.BackoffWithCfgAndMaxSleep(cfg, -1, err)
}

// BackoffWithMaxSleepTxnLockFast sleeps a while base on the MaxSleepTxnLock and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithMaxSleepTxnLockFast(maxSleepMs int, err error) error {
	cfg := BoTxnLockFast
	return b.BackoffWithCfgAndMaxSleep(cfg, maxSleepMs, err)
}

// BackoffWithMaxSleep is deprecated, please use BackoffWithCfgAndMaxSleep instead. TODO: remove it when br is ready.
func (b *Backoffer) BackoffWithMaxSleep(typ int, maxSleepMs int, err error) error {
	// Back off types.
	const (
		boTiKVRPC int = iota
		boTiFlashRPC
		boTxnLock
		boTxnLockFast
		boPDRPC
		boRegionMiss
		boTiKVServerBusy
		boTiFlashServerBusy
		boTxnNotFound
		boStaleCmd
		boMaxTsNotSynced
	)
	switch typ {
	case boTiKVRPC:
		return b.BackoffWithCfgAndMaxSleep(BoTiKVRPC, maxSleepMs, err)
	case boTiFlashRPC:
		return b.BackoffWithCfgAndMaxSleep(BoTiFlashRPC, maxSleepMs, err)
	case boTxnLock:
		return b.BackoffWithCfgAndMaxSleep(BoTxnLock, maxSleepMs, err)
	case boTxnLockFast:
		return b.BackoffWithCfgAndMaxSleep(BoTxnLockFast, maxSleepMs, err)
	case boPDRPC:
		return b.BackoffWithCfgAndMaxSleep(BoPDRPC, maxSleepMs, err)
	case boRegionMiss:
		return b.BackoffWithCfgAndMaxSleep(BoRegionMiss, maxSleepMs, err)
	case boTiKVServerBusy:
		return b.BackoffWithCfgAndMaxSleep(BoTiKVServerBusy, maxSleepMs, err)
	case boTiFlashServerBusy:
		return b.BackoffWithCfgAndMaxSleep(BoTiFlashServerBusy, maxSleepMs, err)
	case boTxnNotFound:
		return b.BackoffWithCfgAndMaxSleep(BoTxnNotFound, maxSleepMs, err)
	case boStaleCmd:
		return b.BackoffWithCfgAndMaxSleep(BoStaleCmd, maxSleepMs, err)
	case boMaxTsNotSynced:
		return b.BackoffWithCfgAndMaxSleep(BoMaxTsNotSynced, maxSleepMs, err)
	}
	cfg := NewConfig("", &metrics.BackoffHistogramEmpty, nil, tikverr.ErrUnknown)
	return b.BackoffWithCfgAndMaxSleep(cfg, maxSleepMs, err)
}

// BackoffWithCfgAndMaxSleep sleeps a while base on the Config and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithCfgAndMaxSleep(cfg *Config, maxSleepMs int, err error) error {
	if strings.Contains(err.Error(), tikverr.MismatchClusterID) {
		logutil.BgLogger().Fatal("critical error", zap.Error(err))
	}
	select {
	case <-b.ctx.Done():
		return errors.Trace(err)
	default:
	}

	b.errors = append(b.errors, errors.Errorf("%s at %s", err.Error(), time.Now().Format(time.RFC3339Nano)))
	b.configs = append(b.configs, cfg)
	if b.noop || (b.maxSleep > 0 && b.totalSleep >= b.maxSleep) {
		errMsg := fmt.Sprintf("%s backoffer.maxSleep %dms is exceeded, errors:", cfg.String(), b.maxSleep)
		for i, err := range b.errors {
			// Print only last 3 errors for non-DEBUG log levels.
			if log.GetLevel() == zapcore.DebugLevel || i >= len(b.errors)-3 {
				errMsg += "\n" + err.Error()
			}
		}
		logutil.BgLogger().Warn(errMsg)
		// Use the first backoff type to generate a MySQL error.
		return b.configs[0].err
	}

	// Lazy initialize.
	if b.fn == nil {
		b.fn = make(map[string]backoffFn)
	}
	f, ok := b.fn[cfg.name]
	if !ok {
		f = cfg.createBackoffFn(b.vars)
		b.fn[cfg.name] = f
	}
	realSleep := f(b.ctx, maxSleepMs)
	if cfg.metric != nil {
		(*cfg.metric).Observe(float64(realSleep) / 1000)
	}
	b.totalSleep += realSleep
	if b.backoffSleepMS == nil {
		b.backoffSleepMS = make(map[string]int)
	}
	b.backoffSleepMS[cfg.name] += realSleep
	if b.backoffTimes == nil {
		b.backoffTimes = make(map[string]int)
	}
	b.backoffTimes[cfg.name]++

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
		zap.Stringer("type", cfg),
		zap.Reflect("txnStartTS", startTs))
	return nil
}

func (b *Backoffer) String() string {
	if b.totalSleep == 0 {
		return ""
	}
	return fmt.Sprintf(" backoff(%dms %v)", b.totalSleep, b.configs)
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
func (b *Backoffer) GetTypes() []string {
	typs := make([]string, 0, len(b.configs))
	for _, cfg := range b.configs {
		typs = append(typs, cfg.String())
	}
	return typs
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
func (b *Backoffer) GetBackoffTimes() map[string]int {
	return b.backoffTimes
}

// GetBackoffSleepMS returns a map contains backoff sleep time by type.
func (b *Backoffer) GetBackoffSleepMS() map[string]int {
	return b.backoffSleepMS
}

// ErrorsNum returns the number of errors.
func (b *Backoffer) ErrorsNum() int {
	return len(b.errors)
}

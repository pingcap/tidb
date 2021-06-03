// Copyright 2021 PingCAP, Inc.
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
	"math"
	"math/rand"
	"strings"
	"time"

	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Config is the configuration of the Backoff function.
type Config struct {
	name   string
	metric *prometheus.Observer
	fnCfg  *BackoffFnCfg
	err    error
}

// backoffFn is the backoff function which compute the sleep time and do sleep.
type backoffFn func(ctx context.Context, maxSleepMs int) int

func (c *Config) createBackoffFn(vars *kv.Variables) backoffFn {
	if strings.EqualFold(c.name, txnLockFastName) {
		return newBackoffFn(vars.BackoffLockFast, c.fnCfg.cap, c.fnCfg.jitter)
	}
	return newBackoffFn(c.fnCfg.base, c.fnCfg.cap, c.fnCfg.jitter)
}

// BackoffFnCfg is the configuration for the backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
type BackoffFnCfg struct {
	base   int
	cap    int
	jitter int
}

// NewBackoffFnCfg creates the config for BackoffFn.
func NewBackoffFnCfg(base, cap, jitter int) *BackoffFnCfg {
	return &BackoffFnCfg{
		base,
		cap,
		jitter,
	}
}

// NewConfig creates a new Config for the Backoff operation.
func NewConfig(name string, metric *prometheus.Observer, backoffFnCfg *BackoffFnCfg, err error) *Config {
	return &Config{
		name:   name,
		metric: metric,
		fnCfg:  backoffFnCfg,
		err:    err,
	}
}

func (c *Config) String() string {
	return c.name
}

const txnLockFastName = "txnLockFast"

// Backoff Config variables.
var (
	// TODO: distinguish tikv and tiflash in metrics
	BoTiKVRPC    = NewConfig("tikvRPC", &metrics.BackoffHistogramRPC, NewBackoffFnCfg(100, 2000, EqualJitter), tikverr.ErrTiKVServerTimeout)
	BoTiFlashRPC = NewConfig("tiflashRPC", &metrics.BackoffHistogramRPC, NewBackoffFnCfg(100, 2000, EqualJitter), tikverr.ErrTiFlashServerTimeout)
	BoTxnLock    = NewConfig("txnLock", &metrics.BackoffHistogramLock, NewBackoffFnCfg(200, 3000, EqualJitter), tikverr.ErrResolveLockTimeout)
	BoPDRPC      = NewConfig("pdRPC", &metrics.BackoffHistogramPD, NewBackoffFnCfg(500, 3000, EqualJitter), tikverr.NewErrPDServerTimeout(""))
	// change base time to 2ms, because it may recover soon.
	BoRegionMiss        = NewConfig("regionMiss", &metrics.BackoffHistogramRegionMiss, NewBackoffFnCfg(2, 500, NoJitter), tikverr.ErrRegionUnavailable)
	BoTiKVServerBusy    = NewConfig("tikvServerBusy", &metrics.BackoffHistogramServerBusy, NewBackoffFnCfg(2000, 10000, EqualJitter), tikverr.ErrTiKVServerBusy)
	BoTiFlashServerBusy = NewConfig("tiflashServerBusy", &metrics.BackoffHistogramServerBusy, NewBackoffFnCfg(2000, 10000, EqualJitter), tikverr.ErrTiFlashServerBusy)
	BoTxnNotFound       = NewConfig("txnNotFound", &metrics.BackoffHistogramEmpty, NewBackoffFnCfg(2, 500, NoJitter), tikverr.ErrResolveLockTimeout)
	BoStaleCmd          = NewConfig("staleCommand", &metrics.BackoffHistogramStaleCmd, NewBackoffFnCfg(2, 1000, NoJitter), tikverr.ErrTiKVStaleCommand)
	BoMaxTsNotSynced    = NewConfig("maxTsNotSynced", &metrics.BackoffHistogramEmpty, NewBackoffFnCfg(2, 500, NoJitter), tikverr.ErrTiKVMaxTimestampNotSynced)
	// TxnLockFast's `base` load from vars.BackoffLockFast when create BackoffFn.
	BoTxnLockFast = NewConfig(txnLockFastName, &metrics.BackoffHistogramLockFast, NewBackoffFnCfg(2, 3000, EqualJitter), tikverr.ErrResolveLockTimeout)
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

// newBackoffFn creates a backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
func newBackoffFn(base, cap, jitter int) backoffFn {
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

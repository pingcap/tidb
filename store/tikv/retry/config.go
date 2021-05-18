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
	"time"

	"github.com/pingcap/tidb/kv"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// BackoffFn is the backoff function which compute the sleep time and do sleep.
type BackoffFn func(ctx context.Context, maxSleepMs int) int

// Config is the configuration of the Backoff function.
type Config struct {
	name      string
	metric    prometheus.Observer
	backoffFn BackoffFn
	err       error
}

// NewConfig creates a new Config for the Backoff operation.
func NewConfig(name string, metric prometheus.Observer, backoffFn BackoffFn, err error) *Config {
	return &Config{
		name:      name,
		metric:    metric,
		backoffFn: backoffFn,
		err:       err,
	}
}

func (c *Config) String() string {
	return c.name
}

// Backoff Config samples.
var (
	// TODO: distinguish tikv and tiflash in metrics
	BoTiKVRPC    = NewConfig("tikvRPC", metrics.BackoffHistogramRPC, NewBackoffFn(100, 2000, EqualJitter), tikverr.ErrTiKVServerTimeout)
	BoTiFlashRPC = NewConfig("tiflashRPC", metrics.BackoffHistogramRPC, NewBackoffFn(100, 2000, EqualJitter), tikverr.ErrTiFlashServerTimeout)
	BoTxnLock    = NewConfig("txnLock", metrics.BackoffHistogramLock, NewBackoffFn(200, 3000, EqualJitter), tikverr.ErrResolveLockTimeout)
	BoPDRPC      = NewConfig("pdRPC", metrics.BackoffHistogramPD, NewBackoffFn(500, 3000, EqualJitter), tikverr.NewErrPDServerTimeout(""))
	// change base time to 2ms, because it may recover soon.
	BoRegionMiss        = NewConfig("regionMiss", metrics.BackoffHistogramRegionMiss, NewBackoffFn(2, 500, NoJitter), tikverr.ErrRegionUnavailable)
	BoTiKVServerBusy    = NewConfig("tikvServerBusy", metrics.BackoffHistogramServerBusy, NewBackoffFn(2000, 10000, EqualJitter), tikverr.ErrTiKVServerBusy)
	BoTiFlashServerBusy = NewConfig("tiflashServerBusy", metrics.BackoffHistogramServerBusy, NewBackoffFn(2000, 10000, EqualJitter), tikverr.ErrTiFlashServerBusy)
	BoTxnNotFound       = NewConfig("txnNotFound", metrics.BackoffHistogramEmpty, NewBackoffFn(2, 500, NoJitter), tikverr.ErrResolveLockTimeout)
	BoStaleCmd          = NewConfig("staleCommand", metrics.BackoffHistogramStaleCmd, NewBackoffFn(2, 1000, NoJitter), tikverr.ErrTiKVStaleCommand)
	BoMaxTsNotSynced    = NewConfig("maxTsNotSynced", metrics.BackoffHistogramEmpty, NewBackoffFn(2, 500, NoJitter), tikverr.ErrTiKVMaxTimestampNotSynced)
)

// BoTxnLockFast returns the config for TxnLockFast.
func BoTxnLockFast(vars *kv.Variables) *Config {
	name := "txnLockFast"
	if vars.Hook != nil {
		vars.Hook(name, vars)
	}
	return NewConfig(name, metrics.BackoffHistogramLockFast, NewBackoffFn(vars.BackoffLockFast, 3000, EqualJitter), tikverr.ErrResolveLockTimeout)
}

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

// NewBackoffFn creates a backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
func NewBackoffFn(base, cap, jitter int) BackoffFn {
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

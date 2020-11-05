// Copyright 2017 PingCAP, Inc.
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

package systimemon

import (
	"os"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/procfs"
	"go.uber.org/zap"
)

// StartMonitor calls systimeErrHandler if system time jump backward.
func StartMonitor(now func() time.Time, systimeErrHandler func(), successCallback func()) {
	logutil.BgLogger().Info("start system time monitor")
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	tickCount := 0
	for {
		last := now().UnixNano()
		<-tick.C
		if now().UnixNano() < last {
			logutil.BgLogger().Error("system time jump backward", zap.Int64("last", last))
			systimeErrHandler()
		}
		// call sucessCallback per second.
		tickCount++
		if tickCount >= 10 {
			tickCount = 0
			successCallback()
		}
	}
}

// StartAutoFreeOSMemory used to auto free os memory when exceed the threshold.
func StartAutoFreeOSMemory() {
	logutil.BgLogger().Info("start process memory monitor")
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	var lastLogErrTime time.Time
	logErrFn := func(msg string, fields ...zap.Field) {
		if time.Since(lastLogErrTime).Seconds() < 60 {
			return
		}
		lastLogErrTime = time.Now()
		logutil.BgLogger().Error(msg, fields...)
	}
	count := 0
	for range tick.C {
		threshold := int(atomic.LoadUint64(&config.GetGlobalConfig().AutoFreeOSMemoryThreshold))
		if threshold <= 0 {
			continue
		}
		pid := os.Getpid()
		p, err := procfs.NewProc(pid)
		if err != nil {
			logErrFn("get process failed", zap.Error(err))
			continue
		}

		stat, err := p.Stat()
		if err != nil {
			logErrFn("get process stat failed", zap.Int("pid", pid), zap.Error(err))
			continue
		}
		rss := stat.ResidentMemory()
		if rss > threshold {
			debug.FreeOSMemory()
			count++
			logutil.BgLogger().Warn("auto free os memory", zap.Int("rss", rss), zap.Int("threshold", threshold), zap.Int("count", count))
		}
	}
}

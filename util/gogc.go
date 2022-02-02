// Copyright 2020 PingCAP, Inc.
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

package util

import (
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/metrics"
)

var gogcValue int64

func init() {
	gogcValue = 100
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		gogcValue = int64(val)
	}
	metrics.GOGC.Set(float64(gogcValue))
}

// SetGOGC update GOGC and related metrics.
func SetGOGC(val int) {
	if val <= 0 {
		val = 100
	}
	debug.SetGCPercent(val)
	metrics.GOGC.Set(float64(val))
	atomic.StoreInt64(&gogcValue, int64(val))
}

// GetGOGC returns the current value of GOGC.
func GetGOGC() int {
	return int(atomic.LoadInt64(&gogcValue))
}

type GOGCStatSampler struct {
	last struct {
		now         int64
		lastGC      time.Time
		gcPauseTime uint64
	}
	GCRunCount uint64
	exitChan   chan struct{}
}

func NewRuntimeStatSampler() *GOGCStatSampler {
	return &GOGCStatSampler{}
}

var gc debug.GCStats

func (gss *GOGCStatSampler) sample() {
	debug.ReadGCStats(&gc)
	now := time.Now().UnixNano()
	dur := float64(now - gss.last.now)
	gcPauseRatio := float64(uint64(gc.PauseTotal)-gss.last.gcPauseTime) / dur
	if gss.last.lastGC.Before(gc.LastGC) {
		gcIntervals := gc.LastGC.Sub(gss.last.lastGC)
		gss.last.lastGC = gc.LastGC
		metrics.GCIntervals.Set(float64(gcIntervals.Nanoseconds()))
	}
	gss.last.gcPauseTime = uint64(gc.PauseTotal)
	metrics.GCCount.Set(float64(gc.NumGC))
	metrics.GCPauseNS.Set(float64(gc.PauseTotal))
	metrics.GCPausePercent.Set(gcPauseRatio)
}

func (gss *GOGCStatSampler) Start() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			gss.sample()
		case <-gss.exitChan:
			return
		}
	}
}

func (gss *GOGCStatSampler) Close() {
	close(gss.exitChan)
}

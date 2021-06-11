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
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"os"
	"runtime/debug"
	"strconv"
	"sync/atomic"

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

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
	"time"

	"github.com/pingcap/tidb/v4/util/logutil"
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

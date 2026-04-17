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

package reporter

import (
	"sync"
	"time"

	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
)

const defaultReportTickerInterval = time.Duration(topsqlstate.DefTiDBTopSQLReportIntervalSeconds) * time.Second

var reportTickerIntervalMu sync.Mutex
var reportTickerInterval = defaultReportTickerInterval

func newReportTicker() *time.Ticker {
	reportTickerIntervalMu.Lock()
	interval := reportTickerInterval
	reportTickerIntervalMu.Unlock()
	return time.NewTicker(interval)
}

// SetReportTickerIntervalSecondsForTest overrides report ticker interval in tests.
// A non-positive value resets to the default interval.
// The returned function restores the previous override.
func SetReportTickerIntervalSecondsForTest(seconds int) (restore func()) {
	interval := defaultReportTickerInterval
	if seconds > 0 {
		interval = time.Duration(seconds) * time.Second
	}

	reportTickerIntervalMu.Lock()
	prev := reportTickerInterval
	reportTickerInterval = interval
	reportTickerIntervalMu.Unlock()

	return func() {
		reportTickerIntervalMu.Lock()
		reportTickerInterval = prev
		reportTickerIntervalMu.Unlock()
	}
}

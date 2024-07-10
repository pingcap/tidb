// Copyright 2024 PingCAP, Inc.
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

package initstats

import (
	"runtime"

	"github.com/pingcap/tidb/pkg/config"
)

// getConcurrency gets the concurrency of loading stats.
// the concurrency is from 2 to 16.
// when Performance.ForceInitStats is true, the concurrency is from 2 to GOMAXPROCS(0)-2.
// -2 is to ensure that the system has enough resources to handle other tasks. such as GC and stats cache internal.
// when Performance.ForceInitStats is false, the concurrency is from 2 to GOMAXPROCS(0)/2.
// it is to ensure that concurrency doesn't affect the performance of customer's business.
func getConcurrency() int {
	var concurrency int
	if config.GetGlobalConfig().Performance.ForceInitStats {
		concurrency = runtime.GOMAXPROCS(0) - 2
	} else {
		concurrency = runtime.GOMAXPROCS(0) / 2
	}
	concurrency = min(max(2, concurrency), 16)
	return concurrency
}

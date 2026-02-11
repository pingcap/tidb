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

package mvs

import tidbmetrics "github.com/pingcap/tidb/pkg/metrics"

func reportCounterDelta(counter interface{ Add(float64) }, last *int64, current int64) {
	if current > *last {
		counter.Add(float64(current - *last))
	}
	*last = current
}

func (*serverHelper) reportMetrics(s *MVService) {
	// Executor metrics
	reportCounterDelta(tidbmetrics.MVTaskExecutorSubmittedCounter, &s.reportCache.submittedCount, s.executor.metrics.submittedCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorCompletedCounter, &s.reportCache.completedCount, s.executor.metrics.completedCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorFailedCounter, &s.reportCache.failedCount, s.executor.metrics.failedCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorTimeoutCounter, &s.reportCache.timeoutCount, s.executor.metrics.timeoutCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorRejectedCounter, &s.reportCache.rejectedCount, s.executor.metrics.rejectedCount.Load())
	tidbmetrics.MVTaskExecutorRunningTaskGauge.Set(float64(s.executor.metrics.runningCount.Load()))
	tidbmetrics.MVTaskExecutorWaitingTaskGauge.Set(float64(s.executor.metrics.waitingCount.Load()))

	// MVService metrics
	tidbmetrics.MVServiceMVRefreshPendingGauge.Set(float64(s.metrics.mvCount.Load()))
	tidbmetrics.MVServiceMVLogPurgePendingGauge.Set(float64(s.metrics.mvLogCount.Load()))
	tidbmetrics.MVServiceMVRefreshRunningGauge.Set(float64(s.metrics.runningMVRefreshCount.Load()))
	tidbmetrics.MVServiceMVLogPurgeRunningGauge.Set(float64(s.metrics.runningMVLogPurgeCount.Load()))
}

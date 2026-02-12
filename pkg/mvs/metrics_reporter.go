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

import (
	"time"

	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
)

func reportCounterDelta(counter interface{ Add(float64) }, last *int64, current int64) {
	if current > *last {
		counter.Add(float64(current - *last))
	}
	*last = current
}

func (h *serverHelper) reportMetrics(s *MVService) {
	// Executor metrics
	reportCounterDelta(tidbmetrics.MVTaskExecutorSubmittedCounter, &h.reportCache.submittedCount, s.executor.metrics.counters.submittedCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorCompletedCounter, &h.reportCache.completedCount, s.executor.metrics.counters.completedCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorFailedCounter, &h.reportCache.failedCount, s.executor.metrics.counters.failedCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorTimeoutCounter, &h.reportCache.timeoutCount, s.executor.metrics.counters.timeoutCount.Load())
	reportCounterDelta(tidbmetrics.MVTaskExecutorRejectedCounter, &h.reportCache.rejectedCount, s.executor.metrics.counters.rejectedCount.Load())
	tidbmetrics.MVTaskExecutorRunningTaskGauge.Set(float64(s.executor.metrics.gauges.runningCount.Load()))
	tidbmetrics.MVTaskExecutorWaitingTaskGauge.Set(float64(s.executor.metrics.gauges.waitingCount.Load()))
	tidbmetrics.MVTaskExecutorTimedOutRunningTaskGauge.Set(float64(s.executor.metrics.gauges.timedOutRunningCount.Load()))

	// MVService metrics
	tidbmetrics.MVServiceMVRefreshTotalGauge.Set(float64(s.metrics.mvCount.Load()))
	tidbmetrics.MVServiceMVLogPurgeTotalGauge.Set(float64(s.metrics.mvLogCount.Load()))
	tidbmetrics.MVServiceMVRefreshRunningGauge.Set(float64(s.metrics.runningMVRefreshCount.Load()))
	tidbmetrics.MVServiceMVLogPurgeRunningGauge.Set(float64(s.metrics.runningMVLogPurgeCount.Load()))
}

func (h *serverHelper) observeTaskDuration(taskType, result string, duration time.Duration) {
	if duration < 0 {
		return
	}
	h.getDurationObserver(taskType, result).Observe(duration.Seconds())
}

func (h *serverHelper) observeFetchDuration(fetchType, result string, duration time.Duration) {
	if duration < 0 {
		return
	}
	h.getDurationObserver(fetchType, result).Observe(duration.Seconds())
}

func (h *serverHelper) observeRunEvent(eventType string) {
	if eventType == "" {
		return
	}
	h.getRunEventCounter(eventType).Inc()
}

func (h *serverHelper) getDurationObserver(metricType, result string) mvMetricObserver {
	if h == nil {
		return tidbmetrics.MVServiceMetaFetchDurationHistogramVec.WithLabelValues(metricType, result)
	}
	key := mvMetricTypeResultKey{typ: metricType, result: result}
	return h.durationObserverCache.getOrCreate(key, func() mvMetricObserver {
		return tidbmetrics.MVServiceMetaFetchDurationHistogramVec.WithLabelValues(metricType, result)
	})
}

func (h *serverHelper) getRunEventCounter(eventType string) mvMetricCounter {
	if h == nil {
		return tidbmetrics.MVServiceRunEventCounterVec.WithLabelValues(eventType)
	}
	return h.runEventCounterCache.getOrCreate(eventType, func() mvMetricCounter {
		return tidbmetrics.MVServiceRunEventCounterVec.WithLabelValues(eventType)
	})
}

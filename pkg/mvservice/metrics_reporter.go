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

package mvservice

import (
	"time"

	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// reportCounterDelta reports only the positive delta since last flush.
func reportCounterDelta(counter interface{ Add(float64) }, last *int64, current int64) {
	if current > *last {
		counter.Add(float64(current - *last))
	}
	*last = current
}

// reportMetrics flushes MVService runtime metrics into the metrics module.
func (h *serviceHelper) reportMetrics(s *MVService) {
	// Executor metrics
	executorMetrics := s.combinedTaskExecutorMetrics()
	reportCounterDelta(tidbmetrics.MVTaskExecutorSubmittedCounter, &h.reportCache.submittedCount, executorMetrics.submittedCount)
	reportCounterDelta(tidbmetrics.MVTaskExecutorFinishedCounter, &h.reportCache.finishedCount, executorMetrics.finishedCount)
	reportCounterDelta(tidbmetrics.MVTaskExecutorFailedCounter, &h.reportCache.failedCount, executorMetrics.failedCount)
	reportCounterDelta(tidbmetrics.MVTaskExecutorTimeoutCounter, &h.reportCache.timeoutCount, executorMetrics.timeoutCount)
	reportCounterDelta(tidbmetrics.MVTaskExecutorRejectedCounter, &h.reportCache.rejectedCount, executorMetrics.rejectedCount)
	reportCounterDelta(tidbmetrics.MVTaskExecutorBackpressureCounter, &h.reportCache.backpressureCount, executorMetrics.backpressureCount)
	tidbmetrics.MVTaskExecutorRunningTaskGauge.Set(float64(executorMetrics.runningCount))
	tidbmetrics.MVTaskExecutorWaitingTaskGauge.Set(float64(executorMetrics.waitingCount))
	tidbmetrics.MVTaskExecutorTimedOutRunningTaskGauge.Set(float64(executorMetrics.timedOutRunningCount))
	tidbmetrics.MVTaskExecutorBackpressureBlockedGauge.Set(float64(executorMetrics.backpressureBlocked))

	// MVService metrics
	tidbmetrics.MVServiceMVRefreshTotalGauge.Set(float64(s.metrics.mvCount.Load()))
	tidbmetrics.MVServiceMVLogPurgeTotalGauge.Set(float64(s.metrics.mvLogCount.Load()))
	tidbmetrics.MVServiceMVRefreshRunningGauge.Set(float64(s.metrics.runningMVRefreshCount.Load()))
	tidbmetrics.MVServiceMVLogPurgeRunningGauge.Set(float64(s.metrics.runningMVLogPurgeCount.Load()))
	tidbmetrics.MVServiceMVRefreshWarningGauge.Set(float64(s.metrics.alertWarningCount.Load()))
	tidbmetrics.MVServiceMVRefreshOverdueGauge.Set(float64(s.metrics.alertOverdueCount.Load()))
}

// observeTaskDuration reports one task execution duration sample.
func (h *serviceHelper) observeTaskDuration(taskType, result string, duration time.Duration) {
	if duration < 0 {
		return
	}
	h.getDurationObserver(taskType, result).Observe(duration.Seconds())
}

// observeRunEvent increments one run-loop event counter.
func (h *serviceHelper) observeRunEvent(eventType string) {
	if eventType == "" {
		return
	}
	h.getRunEventCounter(eventType).Inc()
}

// getDurationObserver returns a cached observer for (type, result) labels.
func (h *serviceHelper) getDurationObserver(metricType, result string) prometheus.Observer {
	if h == nil {
		return tidbmetrics.MVServiceOperationDurationHistogramVec.WithLabelValues(metricType, result)
	}
	key := mvMetricTypeResultKey{typ: metricType, result: result}
	h.durationObserverCache.mu.RLock()
	if observer, ok := h.durationObserverCache.data[key]; ok {
		h.durationObserverCache.mu.RUnlock()
		return observer
	}
	h.durationObserverCache.mu.RUnlock()

	h.durationObserverCache.mu.Lock()
	defer h.durationObserverCache.mu.Unlock()
	if observer, ok := h.durationObserverCache.data[key]; ok {
		return observer
	}
	observer := tidbmetrics.MVServiceOperationDurationHistogramVec.WithLabelValues(metricType, result)
	h.durationObserverCache.data[key] = observer
	return observer
}

// getRunEventCounter returns a cached counter for eventType label.
func (h *serviceHelper) getRunEventCounter(eventType string) prometheus.Counter {
	if h == nil {
		return tidbmetrics.MVServiceRunEventCounterVec.WithLabelValues(eventType)
	}
	h.runEventCounterCache.mu.RLock()
	if counter, ok := h.runEventCounterCache.data[eventType]; ok {
		h.runEventCounterCache.mu.RUnlock()
		return counter
	}
	h.runEventCounterCache.mu.RUnlock()

	h.runEventCounterCache.mu.Lock()
	defer h.runEventCounterCache.mu.Unlock()
	if counter, ok := h.runEventCounterCache.data[eventType]; ok {
		return counter
	}
	counter := tidbmetrics.MVServiceRunEventCounterVec.WithLabelValues(eventType)
	h.runEventCounterCache.data[eventType] = counter
	return counter
}

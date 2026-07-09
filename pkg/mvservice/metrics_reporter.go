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

const (
	mvMetricComponentService = "service"

	mvMetricExecutorEventSubmitted    = "submitted"
	mvMetricExecutorEventFinished     = "finished"
	mvMetricExecutorEventFailed       = "failed"
	mvMetricExecutorEventTimeout      = "timeout"
	mvMetricExecutorEventRejected     = "rejected"
	mvMetricExecutorEventBackpressure = "backpressure"

	mvMetricExecutorStatusRunning             = "running"
	mvMetricExecutorStatusWaiting             = "waiting"
	mvMetricExecutorStatusTimedOutRunning     = "timed_out_running"
	mvMetricExecutorStatusBackpressureBlocked = "backpressure_blocked"
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
	h.reportTaskExecutorMetrics(mvTaskDurationTypeRefresh, snapshotTaskExecutorMetrics(s.refreshExecutor), &h.reportCache.refresh)
	h.reportTaskExecutorMetrics(mvTaskDurationTypePurge, snapshotTaskExecutorMetrics(s.purgeExecutor), &h.reportCache.purge)
	h.reportTaskExecutorMetrics(mvTaskDurationTypeMLogAnalyze, snapshotTaskExecutorMetrics(s.mlogAnalyzeExecutor), &h.reportCache.mlogAnalyze)

	// MVService metrics
	tidbmetrics.MVServiceMVRefreshTotalGauge.Set(float64(s.metrics.mvCount.Load()))
	tidbmetrics.MVServiceMVLogPurgeTotalGauge.Set(float64(s.metrics.mvLogCount.Load()))
	tidbmetrics.MVServiceMVRefreshWarningGauge.Set(float64(s.metrics.alertWarningCount.Load()))
	tidbmetrics.MVServiceMVRefreshOverdueGauge.Set(float64(s.metrics.alertOverdueCount.Load()))
	tidbmetrics.MVServiceMVLogAccumulationGauge.Set(float64(s.metrics.mvLogAccumulationCount.Load()))
}

func (h *serviceHelper) reportTaskExecutorMetrics(component string, metrics taskExecutorMetricsSnapshot, cache *taskExecutorReportCache) {
	if cache == nil {
		return
	}
	reportCounterDelta(h.getRunEventCounter(component, mvMetricExecutorEventSubmitted), &cache.submittedCount, metrics.submittedCount)
	reportCounterDelta(h.getRunEventCounter(component, mvMetricExecutorEventFinished), &cache.finishedCount, metrics.finishedCount)
	reportCounterDelta(h.getRunEventCounter(component, mvMetricExecutorEventFailed), &cache.failedCount, metrics.failedCount)
	reportCounterDelta(h.getRunEventCounter(component, mvMetricExecutorEventTimeout), &cache.timeoutCount, metrics.timeoutCount)
	reportCounterDelta(h.getRunEventCounter(component, mvMetricExecutorEventRejected), &cache.rejectedCount, metrics.rejectedCount)
	reportCounterDelta(h.getRunEventCounter(component, mvMetricExecutorEventBackpressure), &cache.backpressureCount, metrics.backpressureCount)

	tidbmetrics.MVServiceTaskStatusGaugeVec.WithLabelValues(component, mvMetricExecutorStatusRunning).Set(float64(metrics.runningCount))
	tidbmetrics.MVServiceTaskStatusGaugeVec.WithLabelValues(component, mvMetricExecutorStatusWaiting).Set(float64(metrics.waitingCount))
	tidbmetrics.MVServiceTaskStatusGaugeVec.WithLabelValues(component, mvMetricExecutorStatusTimedOutRunning).Set(float64(metrics.timedOutRunningCount))
	tidbmetrics.MVServiceTaskStatusGaugeVec.WithLabelValues(component, mvMetricExecutorStatusBackpressureBlocked).Set(float64(metrics.backpressureBlocked))
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
	h.getRunEventCounter(mvMetricComponentService, eventType).Inc()
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
func (h *serviceHelper) getRunEventCounter(component, eventType string) prometheus.Counter {
	if h == nil {
		return tidbmetrics.MVServiceRunEventCounterVec.WithLabelValues(component, eventType)
	}
	key := mvMetricComponentTypeKey{component: component, typ: eventType}
	h.runEventCounterCache.mu.RLock()
	if counter, ok := h.runEventCounterCache.data[key]; ok {
		h.runEventCounterCache.mu.RUnlock()
		return counter
	}
	h.runEventCounterCache.mu.RUnlock()

	h.runEventCounterCache.mu.Lock()
	defer h.runEventCounterCache.mu.Unlock()
	if counter, ok := h.runEventCounterCache.data[key]; ok {
		return counter
	}
	counter := tidbmetrics.MVServiceRunEventCounterVec.WithLabelValues(component, eventType)
	h.runEventCounterCache.data[key] = counter
	return counter
}

// Copyright 2023 PingCAP, Inc.
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

package domain

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	generateHistoricalStatsSuccessCounter prometheus.Counter
	generateHistoricalStatsFailedCounter  prometheus.Counter

	planReplayerDumpTaskSuccess prometheus.Counter
	planReplayerDumpTaskFailed  prometheus.Counter

	planReplayerCaptureTaskSendCounter    prometheus.Counter
	planReplayerCaptureTaskDiscardCounter prometheus.Counter

	planReplayerRegisterTaskGauge prometheus.Gauge
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init domain metrics vars.
func InitMetricsVars() {
	generateHistoricalStatsSuccessCounter = metrics.HistoricalStatsCounter.WithLabelValues("generate", "success")
	generateHistoricalStatsFailedCounter = metrics.HistoricalStatsCounter.WithLabelValues("generate", "fail")

	planReplayerDumpTaskSuccess = metrics.PlanReplayerTaskCounter.WithLabelValues("dump", "success")
	planReplayerDumpTaskFailed = metrics.PlanReplayerTaskCounter.WithLabelValues("dump", "fail")

	planReplayerCaptureTaskSendCounter = metrics.PlanReplayerTaskCounter.WithLabelValues("capture", "send")
	planReplayerCaptureTaskDiscardCounter = metrics.PlanReplayerTaskCounter.WithLabelValues("capture", "discard")

	planReplayerRegisterTaskGauge = metrics.PlanReplayerRegisterTaskGauge
}

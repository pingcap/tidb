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

package metrics

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// domain metrics vars
var (
	GenerateHistoricalStatsSuccessCounter prometheus.Counter
	GenerateHistoricalStatsFailedCounter  prometheus.Counter

	PlanReplayerDumpTaskSuccess prometheus.Counter
	PlanReplayerDumpTaskFailed  prometheus.Counter

	PlanReplayerCaptureTaskSendCounter    prometheus.Counter
	PlanReplayerCaptureTaskDiscardCounter prometheus.Counter

	PlanReplayerRegisterTaskGauge prometheus.Gauge
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init domain metrics vars.
func InitMetricsVars() {
	GenerateHistoricalStatsSuccessCounter = metrics.HistoricalStatsCounter.WithLabelValues("generate", "success")
	GenerateHistoricalStatsFailedCounter = metrics.HistoricalStatsCounter.WithLabelValues("generate", "fail")

	PlanReplayerDumpTaskSuccess = metrics.PlanReplayerTaskCounter.WithLabelValues("dump", "success")
	PlanReplayerDumpTaskFailed = metrics.PlanReplayerTaskCounter.WithLabelValues("dump", "fail")

	PlanReplayerCaptureTaskSendCounter = metrics.PlanReplayerTaskCounter.WithLabelValues("capture", "send")
	PlanReplayerCaptureTaskDiscardCounter = metrics.PlanReplayerTaskCounter.WithLabelValues("capture", "discard")

	PlanReplayerRegisterTaskGauge = metrics.PlanReplayerRegisterTaskGauge
}

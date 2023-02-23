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

import "github.com/prometheus/client_golang/prometheus"

var KeyspaceLabels prometheus.Labels

// SetServerlessLabels sets keyspace_id label for metrics.
func SetKeyspaceLabels(keyspaceId string) {
	KeyspaceLabels = make(prometheus.Labels)
	KeyspaceLabels["keyspace_id"] = keyspaceId
}

// NewCounter wraps a prometheus.NewCounter.
func NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	opts.ConstLabels = KeyspaceLabels
	return prometheus.NewCounter(opts)
}

// NewCounterVec wraps a prometheus.NewCounterVec.
func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	opts.ConstLabels = KeyspaceLabels
	return prometheus.NewCounterVec(opts, labelNames)
}

// NewGauge wraps a prometheus.NewGauge.
func NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	opts.ConstLabels = KeyspaceLabels
	return prometheus.NewGauge(opts)
}

// NewGaugeVec wraps a prometheus.NewGaugeVec.
func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	opts.ConstLabels = KeyspaceLabels
	return prometheus.NewGaugeVec(opts, labelNames)
}

// NewHistogram wraps a prometheus.NewHistogram.
func NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	opts.ConstLabels = KeyspaceLabels
	return prometheus.NewHistogram(opts)
}

// NewHistogramVec wraps a prometheus.NewHistogramVec.
func NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	opts.ConstLabels = KeyspaceLabels
	return prometheus.NewHistogramVec(opts, labelNames)
}

// NewSummaryVec wraps a prometheus.NewSummaryVec.
func NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *prometheus.SummaryVec {
	opts.ConstLabels = KeyspaceLabels
	return prometheus.NewSummaryVec(opts, labelNames)
}

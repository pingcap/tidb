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
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

var constLabels prometheus.Labels

// SetConstLabels sets constant labels for metrics.
func SetConstLabels(kv ...string) {
	if len(kv)%2 == 1 {
		panic(fmt.Sprintf("got the odd number of inputs for const labels: %d", len(kv)))
	}
	constLabels = make(prometheus.Labels, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		constLabels[strings.ToLower(kv[i])] = kv[i+1]
	}
}

// NewCounter wraps a prometheus.NewCounter.
func NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	opts.ConstLabels = constLabels
	return prometheus.NewCounter(opts)
}

// NewCounterVec wraps a prometheus.NewCounterVec.
func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	opts.ConstLabels = constLabels
	return prometheus.NewCounterVec(opts, labelNames)
}

// NewGauge wraps a prometheus.NewGauge.
func NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	opts.ConstLabels = constLabels
	return prometheus.NewGauge(opts)
}

// NewGaugeVec wraps a prometheus.NewGaugeVec.
func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	opts.ConstLabels = constLabels
	return prometheus.NewGaugeVec(opts, labelNames)
}

// NewHistogram wraps a prometheus.NewHistogram.
func NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	opts.ConstLabels = constLabels
	return prometheus.NewHistogram(opts)
}

// NewHistogramVec wraps a prometheus.NewHistogramVec.
func NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	opts.ConstLabels = constLabels
	return prometheus.NewHistogramVec(opts, labelNames)
}

// NewSummaryVec wraps a prometheus.NewSummaryVec.
func NewSummaryVec(opts prometheus.SummaryOpts, labelNames []string) *prometheus.SummaryVec {
	opts.ConstLabels = constLabels
	return prometheus.NewSummaryVec(opts, labelNames)
}

// Copyright 2022 PingCAP, Inc.
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

package promutil

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Factory is the interface to create some native prometheus metric.
type Factory interface {
	// NewCounter creates a new Counter based on the provided CounterOpts.
	NewCounter(opts prometheus.CounterOpts) prometheus.Counter

	// NewCounterVec creates a new CounterVec based on the provided CounterOpts and
	// partitioned by the given label names.
	NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec

	// NewGauge creates a new Gauge based on the provided GaugeOpts.
	NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge

	// NewGaugeVec creates a new GaugeVec based on the provided GaugeOpts and
	// partitioned by the given label names.
	NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec

	// NewHistogram creates a new Histogram based on the provided HistogramOpts. It
	// panics if the buckets in HistogramOpts are not in strictly increasing order.
	NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram

	// NewHistogramVec creates a new HistogramVec based on the provided HistogramOpts and
	// partitioned by the given label names.
	NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec
}

var _ Factory = defaultFactory{}

type defaultFactory struct{}

func (defaultFactory) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	return prometheus.NewCounter(opts)
}

func (defaultFactory) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(opts, labelNames)
}

func (defaultFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	return prometheus.NewGauge(opts)
}

func (defaultFactory) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(opts, labelNames)
}

func (defaultFactory) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	return prometheus.NewHistogram(opts)
}

func (defaultFactory) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(opts, labelNames)
}

// NewDefaultFactory returns a default implementation of Factory.
func NewDefaultFactory() Factory {
	return defaultFactory{}
}

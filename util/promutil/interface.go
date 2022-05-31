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

import "github.com/prometheus/client_golang/prometheus"

// Factory is the interface to create some native prometheus metric
// copied from tiflow/engine/pkg/promutil/factory.go.
type Factory interface {
	// NewCounter works like the function of the same name in the prometheus
	// package, but it automatically registers the Counter with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewCounter(opts prometheus.CounterOpts) prometheus.Counter

	// NewCounterVec works like the function of the same name in the
	// prometheus, package but it automatically registers the CounterVec with
	// the Factory's Registerer. Panic if it can't register successfully.
	NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec

	// NewGauge works like the function of the same name in the prometheus
	// package, but it automatically registers the Gauge with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge

	// NewGaugeVec works like the function of the same name in the prometheus
	// package but it automatically registers the GaugeVec with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec

	// NewHistogram works like the function of the same name in the prometheus
	// package but it automatically registers the Histogram with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram

	// NewHistogramVec works like the function of the same name in the
	// prometheus package but it automatically registers the HistogramVec
	// with the Factory's Registerer. Panic if it can't register successfully.
	NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec
}

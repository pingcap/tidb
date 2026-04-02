// Copyright 2024 PingCAP, Inc.
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

//revive:disable:exported

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	VectorSearchDistanceFnUsageCounter             *prometheus.CounterVec
	VectorSearchDistanceFnL1UsageCounter           prometheus.Counter
	VectorSearchDistanceFnL2UsageCounter           prometheus.Counter
	VectorSearchDistanceFnCosineUsageCounter       prometheus.Counter
	VectorSearchDistanceFnInnerProductUsageCounter prometheus.Counter

	VectorSearchIndexQueryTopK    prometheus.Histogram
	VectorSearchIndexQueryLatency prometheus.Histogram

	// The following metrics will be set from telemetryV2.
	// They will not be reported when current instance is not DDL owner
	// to avoid duplications.
	VectorSearchTableNumsWithVectorData  OptionalCollector[prometheus.Gauge]
	VectorSearchTableNumsWithVectorIndex OptionalCollector[prometheus.Gauge]
	VectorSearchVectorColumnDimension    OptionalCollector[prometheus.Histogram]
)

// InitVectorSearchMetrics initializes vector data type and vector search metrics.
func InitVectorSearchMetrics() {

	VectorSearchDistanceFnUsageCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "vector_search",
			Name:      "distance_fn_usage",
			Help:      "Counter of used vector distance functions",
		}, []string{"fn_name"})
	VectorSearchDistanceFnL1UsageCounter = VectorSearchDistanceFnUsageCounter.WithLabelValues("l1")
	VectorSearchDistanceFnL2UsageCounter = VectorSearchDistanceFnUsageCounter.WithLabelValues("l2")
	VectorSearchDistanceFnCosineUsageCounter = VectorSearchDistanceFnUsageCounter.WithLabelValues("cosine")
	VectorSearchDistanceFnInnerProductUsageCounter = VectorSearchDistanceFnUsageCounter.WithLabelValues("inner_product")

	VectorSearchIndexQueryTopK = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "vector_search",
			Name:      "index_query_top_k",
			Help:      "Which TopK is used when calling Vector Search with index",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1~512
		})
	VectorSearchIndexQueryLatency = NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "vector_search",
			Name:      "index_query_latency_seconds",
			Help:      "Query latency (in seconds) when calling Vector Search with index",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524sec
		})

	VectorSearchTableNumsWithVectorData = CollectOptionally(NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "vector_search",
			Name:      "table_nums_with_vector_data",
			Help:      "Number of tables with vector data type",
		}))

	VectorSearchTableNumsWithVectorIndex = CollectOptionally(NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "vector_search",
			Name:      "table_nums_with_vector_index",
			Help:      "Number of tables with vector index",
		}))

	VectorSearchVectorColumnDimension = CollectOptionally(NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "vector_search",
			Name:      "vector_column_dimension",
			Help:      "Dimension of vector column when dimension is specified",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 16), // 1~32768
		}))

}

func RegisterVectorSearchMetrics() {
	prometheus.MustRegister(VectorSearchDistanceFnUsageCounter)
	prometheus.MustRegister(VectorSearchIndexQueryTopK)
	prometheus.MustRegister(VectorSearchIndexQueryLatency)
	prometheus.MustRegister(VectorSearchTableNumsWithVectorData)
	prometheus.MustRegister(VectorSearchTableNumsWithVectorIndex)
	prometheus.MustRegister(VectorSearchVectorColumnDimension)
}

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

//nolint:revive
const (
	StatsHealthyBucket0To50 = iota
	StatsHealthyBucket50To55
	StatsHealthyBucket55To60
	StatsHealthyBucket60To70
	StatsHealthyBucket70To80
	StatsHealthyBucket80To100
	StatsHealthyBucket100To100
	StatsHealthyBucketTotal
	StatsHealthyBucketUnneededAnalyze
	StatsHealthyBucketPseudo
	StatsHealthyBucketCount
)

// HealthyBucketConfig describes a single stats healthy bucket. UpperBound is
// exclusive; configs with UpperBound <= 0 are special categories.
//
//nolint:fieldalignment
type HealthyBucketConfig struct {
	Index      int
	UpperBound int64
	Label      string
}

// HealthyBucketConfigs is the list of all healthy bucket configs.
var HealthyBucketConfigs = []HealthyBucketConfig{
	{Index: StatsHealthyBucket0To50, UpperBound: 50, Label: "[0,50)"},
	{Index: StatsHealthyBucket50To55, UpperBound: 55, Label: "[50,55)"},
	{Index: StatsHealthyBucket55To60, UpperBound: 60, Label: "[55,60)"},
	{Index: StatsHealthyBucket60To70, UpperBound: 70, Label: "[60,70)"},
	{Index: StatsHealthyBucket70To80, UpperBound: 80, Label: "[70,80)"},
	{Index: StatsHealthyBucket80To100, UpperBound: 100, Label: "[80,100)"},
	{Index: StatsHealthyBucket100To100, UpperBound: 101, Label: "[100,100]"},
	// Technically [0,100] should be called "total", but we keep this label to stay
	// compatible with earlier versions.
	{Index: StatsHealthyBucketTotal, Label: "[0,100]"},
	{Index: StatsHealthyBucketUnneededAnalyze, Label: "unneeded analyze"},
	{Index: StatsHealthyBucketPseudo, Label: "pseudo"},
}

// statistics metrics vars
var (
	StatsHealthyGauges                []prometheus.Gauge
	DumpHistoricalStatsSuccessCounter prometheus.Counter
	DumpHistoricalStatsFailedCounter  prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init statistics metrics vars.
func InitMetricsVars() {
	if len(HealthyBucketConfigs) != StatsHealthyBucketCount {
		panic("HealthyBucketConfigs length mismatch")
	}
	StatsHealthyGauges = make([]prometheus.Gauge, len(HealthyBucketConfigs))
	for idx, cfg := range HealthyBucketConfigs {
		StatsHealthyGauges[idx] = metrics.StatsHealthyGauge.WithLabelValues(cfg.Label)
	}

	DumpHistoricalStatsSuccessCounter = metrics.HistoricalStatsCounter.WithLabelValues("dump", "success")
	DumpHistoricalStatsFailedCounter = metrics.HistoricalStatsCounter.WithLabelValues("dump", "fail")
}

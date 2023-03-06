// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// planner core metrics vars
var (
	PseudoEstimationNotAvailable               prometheus.Counter
	PseudoEstimationOutdate                    prometheus.Counter
	preparedPlanCacheHitCounter                prometheus.Counter
	nonPreparedPlanCacheHitCounter             prometheus.Counter
	preparedPlanCacheMissCounter               prometheus.Counter
	nonPreparedPlanCacheMissCounter            prometheus.Counter
	preparedPlanCacheInstancePlanNumCounter    prometheus.Gauge
	nonPreparedPlanCacheInstancePlanNumCounter prometheus.Gauge
	preparedPlanCacheInstanceMemoryUsage       prometheus.Gauge
	nonPreparedPlanCacheInstanceMemoryUsage    prometheus.Gauge
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init planner core metrics vars.
func InitMetricsVars() {
	PseudoEstimationNotAvailable = metrics.PseudoEstimation.WithLabelValues("nodata")
	PseudoEstimationOutdate = metrics.PseudoEstimation.WithLabelValues("outdate")
	// plan cache metrics
	preparedPlanCacheHitCounter = metrics.PlanCacheCounter.WithLabelValues("prepared")
	nonPreparedPlanCacheHitCounter = metrics.PlanCacheCounter.WithLabelValues("non-prepared")
	preparedPlanCacheMissCounter = metrics.PlanCacheMissCounter.WithLabelValues("prepared")
	nonPreparedPlanCacheMissCounter = metrics.PlanCacheMissCounter.WithLabelValues("non-prepared")
	preparedPlanCacheInstancePlanNumCounter = metrics.PlanCacheInstancePlanNumCounter.WithLabelValues(" prepared")
	preparedPlanCacheInstancePlanNumCounter = metrics.PlanCacheInstancePlanNumCounter.WithLabelValues(" prepared")
	nonPreparedPlanCacheInstancePlanNumCounter = metrics.PlanCacheInstancePlanNumCounter.WithLabelValues(" non-prepared")
	preparedPlanCacheInstanceMemoryUsage = metrics.PlanCacheInstanceMemoryUsage.WithLabelValues(" prepared")
	nonPreparedPlanCacheInstanceMemoryUsage = metrics.PlanCacheInstanceMemoryUsage.WithLabelValues(" non-prepared")
}

func GetPlanCacheHitCounter(isNonPrepared bool) prometheus.Counter {
	if isNonPrepared {
		return nonPreparedPlanCacheHitCounter
	}
	return preparedPlanCacheHitCounter
}

func GetPlanCacheMissCounter(isNonPrepared bool) prometheus.Counter {
	if isNonPrepared {
		return nonPreparedPlanCacheMissCounter
	}
	return preparedPlanCacheMissCounter
}

func GetPlanCacheInstanceNumCounter(isNonPrepared bool) prometheus.Gauge {
	if isNonPrepared {
		return nonPreparedPlanCacheInstancePlanNumCounter
	}
	return preparedPlanCacheInstancePlanNumCounter
}

func GetPlanCacheInstanceMemoryUsage(isNonPrepared bool) prometheus.Gauge {
	if isNonPrepared {
		return nonPreparedPlanCacheInstanceMemoryUsage
	}
	return preparedPlanCacheInstanceMemoryUsage
}

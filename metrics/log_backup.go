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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// log backup metrics.
// see the `Help` field for details.
var (
	LastCheckpoint = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb",
		Subsystem: "log_backup",
		Name:      "last_checkpoint",
		Help:      "The last global checkpoint of log backup.",
	}, []string{"task"})
	AdvancerOwner = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   "tidb",
		Subsystem:   "log_backup",
		Name:        "advancer_owner",
		Help:        "If the node is the owner of advancers, set this to `1`, otherwise `0`.",
		ConstLabels: map[string]string{},
	})
	AdvancerTickDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "log_backup",
		Name:      "advancer_tick_duration_sec",
		Help:      "The time cost of each step during advancer ticking.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 3.0, 8),
	}, []string{"step"})
	GetCheckpointBatchSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "log_backup",
		Name:      "advancer_batch_size",
		Help:      "The batch size of scanning region or get region checkpoint.",
		Buckets:   prometheus.ExponentialBuckets(1, 2.0, 12),
	}, []string{"type"})
	RegionCheckpointRequest = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "log_backup",
		Name:      "region_request",
		Help:      "The failure / success stat requesting region checkpoints.",
	}, []string{"result"})
	RegionCheckpointFailure = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "log_backup",
		Name:      "region_request_failure",
		Help:      "The failure reasons of requesting region checkpoints.",
	}, []string{"reason"})
	RegionCheckpointSubscriptionEvent = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "log_backup",
		Name:      "region_checkpoint_event",
		Help:      "The region flush event count.",
	}, []string{"store"})
)

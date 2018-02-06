// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics
var (
	NewSessionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "owner",
			Name:      "new_session",
			Help:      "Bucketed histogram of processing time (s) of new session.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 22),
		}, []string{"type", "result_state"})

	Cancelled         = "cancelled"
	Deleted           = "deleted"
	SessionDone       = "session_done"
	CtxDone           = "context_done"
	WatchOwnerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "owner",
			Name:      "watch_owner",
			Help:      "Counter of watch owner.",
		}, []string{"type", "return_reason"})

	NoLongerOwner        = "no_longer_owner"
	CampaignOwnerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "owner",
			Name:      "campaign_owner",
			Help:      "Counter of campaign owner.",
		}, []string{"type", "retry_reason"})
)

func init() {
	prometheus.MustRegister(NewSessionHistogram)
	prometheus.MustRegister(WatchOwnerCounter)
	prometheus.MustRegister(CampaignOwnerCounter)
}

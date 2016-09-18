// Copyright 2016 PingCAP, Inc.
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

package tikv

import "github.com/prometheus/client_golang/prometheus"

var (
	txnCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_action_total",
			Help:      "Counter of txn actions.",
		})

	snapshotCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "snapshot_total",
			Help:      "Counter of snapshots.",
		})

	txnCmdCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_cmd_total",
			Help:      "Counter of txn commands.",
		}, []string{"type"})

	txnCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_cmd_seconds",
			Help:      "Bucketed histogram of processing time of txn cmds.",
		}, []string{"type"})

	backoffCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "backoff_total",
			Help:      "Counter of backoff.",
		}, []string{"type"})

	backoffHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "backoff_seconds",
			Help:      "Bucketed histogram of sleep seconds of backoff.",
		}, []string{"type"})

	sendReqCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "request_total",
			Help:      "Counter of tikv-server requests.",
		}, []string{"type"})

	sendReqHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of sending request duration.",
		}, []string{"type"})

	copBuildTaskHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "cop_buildtask_seconds",
			Help:      "Coprocessor buildTask cost time.",
		})

	coprocessorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "coprocessor_actions_total",
			Help:      "Counter of coprocessor actions.",
		}, []string{"type"})

	gcWorkerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_worker_actions_total",
			Help:      "Counter of gc worker actions.",
		}, []string{"type"})

	gcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "gc_seconds",
			Help:      "Bucketed histogram of gc duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		}, []string{"stage"})

	lockResolverCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "lock_resolver_actions_total",
			Help:      "Counter of lock resolver actions.",
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(txnCounter)
	prometheus.MustRegister(snapshotCounter)
	prometheus.MustRegister(txnCmdCounter)
	prometheus.MustRegister(txnCmdHistogram)
	prometheus.MustRegister(backoffCounter)
	prometheus.MustRegister(backoffHistogram)
	prometheus.MustRegister(sendReqCounter)
	prometheus.MustRegister(sendReqHistogram)
	prometheus.MustRegister(copBuildTaskHistogram)
	prometheus.MustRegister(coprocessorCounter)
	prometheus.MustRegister(gcWorkerCounter)
	prometheus.MustRegister(gcHistogram)
	prometheus.MustRegister(lockResolverCounter)
}

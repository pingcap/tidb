// Copyright 2022 PingCAP, Inc.
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
	"context"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Phases to trace
var (
	PhaseIdle      = "idle"
	PhaseBeginTxn  = "begin_txn"
	PhaseCommitTxn = "commit_txn"
	PhaseQuery     = "query"
	PhaseCheckTTL  = "check_ttl"
	PhaseWaitRetry = "wait_retry"
	PhaseDispatch  = "dispatch"
	PhaseWaitToken = "wait_token"
	PhaseOther     = "other"
)

// TTL metrics
var (
	SelectSuccessDuration = metrics.TTLQueryDuration.With(prometheus.Labels{metrics.LblSQLType: "select", metrics.LblResult: metrics.LblOK})
	SelectErrorDuration   = metrics.TTLQueryDuration.With(prometheus.Labels{metrics.LblSQLType: "select", metrics.LblResult: metrics.LblError})
	DeleteSuccessDuration = metrics.TTLQueryDuration.With(prometheus.Labels{metrics.LblSQLType: "delete", metrics.LblResult: metrics.LblOK})
	DeleteErrorDuration   = metrics.TTLQueryDuration.With(prometheus.Labels{metrics.LblSQLType: "delete", metrics.LblResult: metrics.LblError})

	ScannedExpiredRows       = metrics.TTLProcessedExpiredRowsCounter.With(prometheus.Labels{metrics.LblSQLType: "select", metrics.LblResult: metrics.LblOK})
	DeleteSuccessExpiredRows = metrics.TTLProcessedExpiredRowsCounter.With(prometheus.Labels{metrics.LblSQLType: "delete", metrics.LblResult: metrics.LblOK})
	DeleteErrorExpiredRows   = metrics.TTLProcessedExpiredRowsCounter.With(prometheus.Labels{metrics.LblSQLType: "delete", metrics.LblResult: metrics.LblError})

	RunningJobsCnt    = metrics.TTLJobStatus.With(prometheus.Labels{metrics.LblType: "running"})
	CancellingJobsCnt = metrics.TTLJobStatus.With(prometheus.Labels{metrics.LblType: "cancelling"})

	RunningTaskCnt = metrics.TTLTaskStatus.With(prometheus.Labels{metrics.LblType: "running"})
)

func initWorkerPhases(workerType string) map[string]prometheus.Counter {
	return map[string]prometheus.Counter{
		PhaseIdle:      metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseIdle),
		PhaseBeginTxn:  metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseBeginTxn),
		PhaseCommitTxn: metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseCommitTxn),
		PhaseQuery:     metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseQuery),
		PhaseWaitRetry: metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseWaitRetry),
		PhaseDispatch:  metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseDispatch),
		PhaseCheckTTL:  metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseCheckTTL),
		PhaseWaitToken: metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseWaitToken),
		PhaseOther:     metrics.TTLPhaseTime.WithLabelValues(workerType, PhaseOther),
	}
}

var scanWorkerPhases = initWorkerPhases("scan_worker")
var deleteWorkerPhases = initWorkerPhases("delete_worker")

// PhaseTracer is used to tracer the phases duration
type PhaseTracer struct {
	getTime        func() time.Time
	recordDuration func(phase string, duration time.Duration)

	phase     string
	phaseTime time.Time
}

// NewScanWorkerPhaseTracer returns a tracer for scan worker
func NewScanWorkerPhaseTracer() *PhaseTracer {
	return newPhaseTracer(time.Now, func(status string, duration time.Duration) {
		if counter, ok := scanWorkerPhases[status]; ok {
			counter.Add(duration.Seconds())
		}
	})
}

// NewDeleteWorkerPhaseTracer returns a tracer for delete worker
func NewDeleteWorkerPhaseTracer() *PhaseTracer {
	return newPhaseTracer(time.Now, func(status string, duration time.Duration) {
		if counter, ok := deleteWorkerPhases[status]; ok {
			counter.Add(duration.Seconds())
		}
	})
}

func newPhaseTracer(getTime func() time.Time, recordDuration func(status string, duration time.Duration)) *PhaseTracer {
	return &PhaseTracer{
		getTime:        getTime,
		recordDuration: recordDuration,
		phaseTime:      getTime(),
	}
}

// Phase returns the current phase
func (t *PhaseTracer) Phase() string {
	if t == nil {
		return ""
	}
	return t.phase
}

// EnterPhase enters into a new phase
func (t *PhaseTracer) EnterPhase(phase string) {
	if t == nil {
		return
	}

	now := t.getTime()
	if t.phase != "" {
		t.recordDuration(t.phase, now.Sub(t.phaseTime))
	}

	t.phase = phase
	t.phaseTime = now
}

// EndPhase ends the current phase
func (t *PhaseTracer) EndPhase() {
	if t == nil {
		return
	}
	t.EnterPhase("")
}

type ttlPhaseTraceKey struct{}

// CtxWithPhaseTracer create a new context with tracer
func CtxWithPhaseTracer(ctx context.Context, tracer *PhaseTracer) context.Context {
	return context.WithValue(ctx, ttlPhaseTraceKey{}, tracer)
}

// PhaseTracerFromCtx returns a tracer from a given context
func PhaseTracerFromCtx(ctx context.Context) *PhaseTracer {
	if tracer, ok := ctx.Value(ttlPhaseTraceKey{}).(*PhaseTracer); ok {
		return tracer
	}
	return nil
}

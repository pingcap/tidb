// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// SPFreshBulkSortedRunDecodeDuration measures completed decoding stages per object.
var SPFreshBulkSortedRunDecodeDuration *prometheus.HistogramVec

// SPFreshBulkRaBitQFragmentDecodeDuration measures completed shard-input decoding stages.
var SPFreshBulkRaBitQFragmentDecodeDuration *prometheus.HistogramVec

// SPFreshBulkObjectReadDuration measures successful immutable object reads.
var SPFreshBulkObjectReadDuration *prometheus.HistogramVec

// SPFreshBulkProofDuration measures independent shard work and final proof wall time.
var SPFreshBulkProofDuration *prometheus.HistogramVec

// SPFreshBulkReadInFlightBytes tracks estimated reserved bytes across DDL jobs.
var SPFreshBulkReadInFlightBytes prometheus.Gauge

// SPFreshBulkReadBudgetBytes reports the current process-wide object budget.
var SPFreshBulkReadBudgetBytes prometheus.Gauge

// SPFreshBulkReadBudgetWait measures waits before allocating object bodies.
var SPFreshBulkReadBudgetWait *prometheus.HistogramVec

// SPFreshBulkReadLeaseDuration spans object allocation through drained import.
var SPFreshBulkReadLeaseDuration prometheus.Histogram

// SPFreshBulkImportDuration separates batch admission from joined import work.
var SPFreshBulkImportDuration *prometheus.HistogramVec

// SPFreshBulkImportInFlight counts admitted batches across this process's DDLs.
var SPFreshBulkImportInFlight prometheus.Gauge

// SPFreshBulkScanUploadDuration measures source scan and upload pipeline stages.
var SPFreshBulkScanUploadDuration *prometheus.HistogramVec

// SPFreshBulkSourceBufferBytes counts newly allocated and reused raw scan buffers.
var SPFreshBulkSourceBufferBytes *prometheus.CounterVec

// SPFreshBulkPipelineWorkers counts configured slots and active work across builds.
// Active readers/consumers include RPC and backpressure waits; active uploaders
// are encoding/uploading a fragment. No build IDs are used as metric labels.
var SPFreshBulkPipelineWorkers *prometheus.GaugeVec

// SPFreshBulkPendingFragments samples queued upload fragments once per second.
var SPFreshBulkPendingFragments prometheus.Gauge

// SPFreshBulkCleanupRuns counts completed runs and retryable cleanup failures.
var SPFreshBulkCleanupRuns *prometheus.CounterVec

// SPFreshBulkCleanupDuration measures successful background cleanup attempts.
var SPFreshBulkCleanupDuration prometheus.Histogram

// SPFreshBulkCleanupObjects counts acknowledged object deletions.
var SPFreshBulkCleanupObjects prometheus.Counter

// SPFreshBulkCleanupBytes counts the listed bytes of acknowledged object deletions.
var SPFreshBulkCleanupBytes prometheus.Counter

// InitSPFreshBulkMetrics initializes SPFresh offline artifact metrics.
func InitSPFreshBulkMetrics() {
	SPFreshBulkPipelineWorkers = NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "pipeline_workers",
		Help: "Configured slots and active work summed across builds; readers/consumers include waits, uploaders count fragments being encoded/uploaded.",
	}, []string{"role", "state"})
	SPFreshBulkPendingFragments = NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "pending_fragments",
		Help: "Queued upload fragments summed across builds, sampled once per second; excludes blocked producers and active uploaders.",
	})
	SPFreshBulkCleanupRuns = NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "cleanup_runs_total",
		Help: "Background cleanup successes and retryable failures; excludes foreground build work.",
	}, []string{"result"})
	SPFreshBulkCleanupDuration = NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "cleanup_duration_seconds",
		Help:    "Successful background cleanup attempt duration, excluding queue delay and storage-side reclamation.",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 9),
	})
	SPFreshBulkCleanupObjects = NewCounter(prometheus.CounterOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "cleanup_objects_total",
		Help: "Object deletions acknowledged by the temporary object store.",
	})
	SPFreshBulkCleanupBytes = NewCounter(prometheus.CounterOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "cleanup_bytes_total",
		Help: "Listed logical bytes of acknowledged deletions; not physical disk reclamation.",
	})
	SPFreshBulkSourceBufferBytes = NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "source_buffer_bytes_total",
		Help: "Raw source batch buffer capacity allocated or reused; excludes snapshot and row descriptor allocations.",
	}, []string{"action"})
	SPFreshBulkScanUploadDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "scan_upload_duration_seconds",
		Help:    "Source scan and upload work/wait stages; concurrent and nested work is not additive wall time.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 4, 12),
	}, []string{"stage"})
	SPFreshBulkReadInFlightBytes = NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "read_in_flight_estimated_bytes",
		Help: "Process-wide estimated bytes reserved by concurrent proof/memory-import object reads.",
	})
	SPFreshBulkReadBudgetBytes = NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "read_budget_bytes",
		Help: "Current process-wide object-read reservation budget; objects exceeding it are rejected. This is not an RSS limit.",
	})
	SPFreshBulkImportDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "import_batch_duration_seconds",
		Help:    "Batch slot wait and joined import execution, including errors; concurrent durations are not additive wall time.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 4, 12),
	}, []string{"stage"})
	SPFreshBulkImportInFlight = NewGauge(prometheus.GaugeOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "import_batches_in_flight",
		Help: "Active memory-import batches across DDL jobs in this process, excluding batches waiting for a slot.",
	})
	SPFreshBulkReadLeaseDuration = NewHistogram(prometheus.HistogramOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "read_lease_duration_seconds",
		Help:    "Object memory reservation duration through authentication, proof and joined memory import, including failures.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 4, 12),
	})
	SPFreshBulkReadBudgetWait = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb", Subsystem: "spfresh_bulk", Name: "read_budget_wait_seconds",
		Help:    "Time waiting for process-wide object-read memory reservations before allocation.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 4, 12),
	}, []string{"phase"})
	SPFreshBulkProofDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "spfresh_bulk",
		Name:      "proof_duration_seconds",
		Help:      "Completed proof input/output work, local SST preparation and final combination wall time; shard work overlaps build RPCs.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 4, 12),
	}, []string{"stage"})
	SPFreshBulkObjectReadDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "spfresh_bulk",
		Name:      "object_read_duration_seconds",
		Help:      "Time spent reading complete immutable objects; total includes open and read stages.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 4, 12),
	}, []string{"stage"})
	SPFreshBulkSortedRunDecodeDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "spfresh_bulk",
		Name:      "sorted_run_decode_duration_seconds",
		Help:      "Time spent in each completed SortedRun decoding stage per object.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 4, 12),
	}, []string{"stage"})
	SPFreshBulkRaBitQFragmentDecodeDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "spfresh_bulk",
		Name:      "rabitq_fragment_decode_duration_seconds",
		Help:      "Time spent in each completed RaBitQFragment decoding stage per object.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 4, 12),
	}, []string{"stage"})
}

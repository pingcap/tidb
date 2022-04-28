// Copyright 2019 PingCAP, Inc.
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

package metric

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	// states used for the TableCounter labels
	TableStatePending   = "pending"
	TableStateImported  = "imported"
	TableStateCompleted = "completed"

	BytesStateTotalRestore   = "total_restore" // total source data bytes needs to restore
	BytesStateRestored       = "restored"      // source data bytes restored during restore engine
	BytesStateRestoreWritten = "written"       // bytes written during restore engine
	BytesStateImported       = "imported"      // bytes imported during import engine

	ProgressPhaseTotal   = "total"   // total restore progress(not include post-process, like checksum and analyze)
	ProgressPhaseRestore = "restore" // restore engine progress
	ProgressPhaseImport  = "import"  // import engine progress

	// results used for the TableCounter labels
	TableResultSuccess = "success"
	TableResultFailure = "failure"

	// states used for the ChunkCounter labels
	ChunkStateEstimated = "estimated"
	ChunkStatePending   = "pending"
	ChunkStateRunning   = "running"
	ChunkStateFinished  = "finished"
	ChunkStateFailed    = "failed"

	BlockDeliverKindIndex = "index"
	BlockDeliverKindData  = "data"
)

var (
	ImporterEngineCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "importer_engine",
			Help:      "counting open and closed importer engines",
		}, []string{"type"})

	IdleWorkersGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "lightning",
			Name:      "idle_workers",
			Help:      "counting idle workers",
		}, []string{"name"})

	KvEncoderCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "kv_encoder",
			Help:      "counting kv open and closed kv encoder",
		}, []string{"type"},
	)

	TableCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "tables",
			Help:      "count number of tables processed",
		}, []string{"state", "result"})
	ProcessedEngineCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "engines",
			Help:      "count number of engines processed",
		}, []string{"state", "result"})
	ChunkCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "chunks",
			Help:      "count number of chunks processed",
		}, []string{"state"})
	BytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "lightning",
			Name:      "bytes",
			Help:      "count of total bytes",
		}, []string{"state"})
	// state can be one of:
	//  - estimated (an estimation derived from the file size)
	//  - pending
	//  - running
	//  - finished
	//  - failed

	ImportSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "import_seconds",
			Help:      "time needed to import a table",
			Buckets:   prometheus.ExponentialBuckets(0.125, 2, 6),
		},
	)
	ChunkParserReadBlockSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "chunk_parser_read_block_seconds",
			Help:      "time needed for chunk parser read a block",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
		},
	)
	ApplyWorkerSecondsHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "apply_worker_seconds",
			Help:      "time needed to apply a worker",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
		}, []string{"name"},
	)
	RowReadSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "row_read_seconds",
			Help:      "time needed to parse a row",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 7),
		},
	)
	RowReadBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "row_read_bytes",
			Help:      "number of bytes being read out from data source",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 8),
		},
	)
	RowEncodeSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "row_encode_seconds",
			Help:      "time needed to encode a row",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
		},
	)
	RowKVDeliverSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "row_kv_deliver_seconds",
			Help:      "time needed to deliver kvs of a single row",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
		},
	)
	BlockDeliverSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_deliver_seconds",
			Help:      "time needed to deliver a block",
			Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
		},
	)
	BlockDeliverBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_deliver_bytes",
			Help:      "number of bytes being sent out to importer",
			Buckets:   prometheus.ExponentialBuckets(512, 2, 10),
		}, []string{"kind"},
	)
	BlockDeliverKVPairsHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "block_deliver_kv_pairs",
			Help:      "number of KV pairs being sent out to importer",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		}, []string{"kind"},
	)
	ChecksumSecondsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "lightning",
			Name:      "checksum_seconds",
			Help:      "time needed to complete the checksum stage",
			Buckets:   prometheus.ExponentialBuckets(1, 2.2679331552660544, 10),
		},
	)

	LocalStorageUsageBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "lightning",
			Name:      "local_storage_usage_bytes",
			Help:      "disk/memory size currently occupied by intermediate files in local backend",
		}, []string{"medium"},
	)

	ProgressGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "lightning",
			Name:      "progress",
			Help:      "progress of lightning phase",
		}, []string{"phase"},
	)
)

//nolint:gochecknoinits // TODO: refactor
func init() {
	prometheus.MustRegister(IdleWorkersGauge)
	prometheus.MustRegister(ImporterEngineCounter)
	prometheus.MustRegister(KvEncoderCounter)
	prometheus.MustRegister(TableCounter)
	prometheus.MustRegister(ProcessedEngineCounter)
	prometheus.MustRegister(ChunkCounter)
	prometheus.MustRegister(BytesCounter)
	prometheus.MustRegister(ImportSecondsHistogram)
	prometheus.MustRegister(RowReadSecondsHistogram)
	prometheus.MustRegister(RowReadBytesHistogram)
	prometheus.MustRegister(RowEncodeSecondsHistogram)
	prometheus.MustRegister(RowKVDeliverSecondsHistogram)
	prometheus.MustRegister(BlockDeliverSecondsHistogram)
	prometheus.MustRegister(BlockDeliverBytesHistogram)
	prometheus.MustRegister(BlockDeliverKVPairsHistogram)
	prometheus.MustRegister(ChecksumSecondsHistogram)
	prometheus.MustRegister(ChunkParserReadBlockSecondsHistogram)
	prometheus.MustRegister(ApplyWorkerSecondsHistogram)
	prometheus.MustRegister(LocalStorageUsageBytesGauge)
	prometheus.MustRegister(ProgressGauge)
}

func RecordTableCount(status string, err error) {
	var result string
	if err != nil {
		result = TableResultFailure
	} else {
		result = TableResultSuccess
	}
	TableCounter.WithLabelValues(status, result).Inc()
}

func RecordEngineCount(status string, err error) {
	var result string
	if err != nil {
		result = TableResultFailure
	} else {
		result = TableResultSuccess
	}
	ProcessedEngineCounter.WithLabelValues(status, result).Inc()
}

// ReadCounter reports the current value of the counter.
func ReadCounter(counter prometheus.Counter) float64 {
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Counter.GetValue()
}

// ReadHistogramSum reports the sum of all observed values in the histogram.
func ReadHistogramSum(histogram prometheus.Histogram) float64 {
	var metric dto.Metric
	if err := histogram.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Histogram.GetSampleSum()
}

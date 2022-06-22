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
	"context"
	"math"

	"github.com/pingcap/tidb/util/promutil"
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

type Metrics struct {
	ImporterEngineCounter                *prometheus.CounterVec
	IdleWorkersGauge                     *prometheus.GaugeVec
	KvEncoderCounter                     *prometheus.CounterVec
	TableCounter                         *prometheus.CounterVec
	ProcessedEngineCounter               *prometheus.CounterVec
	ChunkCounter                         *prometheus.CounterVec
	BytesCounter                         *prometheus.CounterVec
	ImportSecondsHistogram               prometheus.Histogram
	ChunkParserReadBlockSecondsHistogram prometheus.Histogram
	ApplyWorkerSecondsHistogram          *prometheus.HistogramVec
	RowReadSecondsHistogram              prometheus.Histogram
	RowReadBytesHistogram                prometheus.Histogram
	RowEncodeSecondsHistogram            prometheus.Histogram
	RowKVDeliverSecondsHistogram         prometheus.Histogram
	BlockDeliverSecondsHistogram         prometheus.Histogram
	BlockDeliverBytesHistogram           *prometheus.HistogramVec
	BlockDeliverKVPairsHistogram         *prometheus.HistogramVec
	ChecksumSecondsHistogram             prometheus.Histogram
	LocalStorageUsageBytesGauge          *prometheus.GaugeVec
	ProgressGauge                        *prometheus.GaugeVec
}

// NewMetrics creates a new empty metrics.
func NewMetrics(factory promutil.Factory) *Metrics {
	return &Metrics{
		ImporterEngineCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lightning",
				Name:      "importer_engine",
				Help:      "counting open and closed importer engines",
			}, []string{"type"}),

		IdleWorkersGauge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "lightning",
				Name:      "idle_workers",
				Help:      "counting idle workers",
			}, []string{"name"}),

		KvEncoderCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lightning",
				Name:      "kv_encoder",
				Help:      "counting kv open and closed kv encoder",
			}, []string{"type"}),

		TableCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lightning",
				Name:      "tables",
				Help:      "count number of tables processed",
			}, []string{"state", "result"}),

		ProcessedEngineCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lightning",
				Name:      "engines",
				Help:      "count number of engines processed",
			}, []string{"state", "result"}),

		ChunkCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lightning",
				Name:      "chunks",
				Help:      "count number of chunks processed",
			}, []string{"state"}),

		BytesCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lightning",
				Name:      "bytes",
				Help:      "count of total bytes",
			}, []string{"state"}),
		// state can be one of:
		//  - estimated (an estimation derived from the file size)
		//  - pending
		//  - running
		//  - finished
		//  - failed

		ImportSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "import_seconds",
				Help:      "time needed to import a table",
				Buckets:   prometheus.ExponentialBuckets(0.125, 2, 6),
			}),

		ChunkParserReadBlockSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "chunk_parser_read_block_seconds",
				Help:      "time needed for chunk parser read a block",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),

		ApplyWorkerSecondsHistogram: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "apply_worker_seconds",
				Help:      "time needed to apply a worker",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}, []string{"name"}),

		RowReadSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "row_read_seconds",
				Help:      "time needed to parse a row",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 7),
			}),

		RowReadBytesHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "row_read_bytes",
				Help:      "number of bytes being read out from data source",
				Buckets:   prometheus.ExponentialBuckets(1024, 2, 8),
			}),

		RowEncodeSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "row_encode_seconds",
				Help:      "time needed to encode a row",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),
		RowKVDeliverSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "row_kv_deliver_seconds",
				Help:      "time needed to deliver kvs of a single row",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),

		BlockDeliverSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "block_deliver_seconds",
				Help:      "time needed to deliver a block",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),
		BlockDeliverBytesHistogram: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "block_deliver_bytes",
				Help:      "number of bytes being sent out to importer",
				Buckets:   prometheus.ExponentialBuckets(512, 2, 10),
			}, []string{"kind"}),
		BlockDeliverKVPairsHistogram: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "block_deliver_kv_pairs",
				Help:      "number of KV pairs being sent out to importer",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
			}, []string{"kind"}),
		ChecksumSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "lightning",
				Name:      "checksum_seconds",
				Help:      "time needed to complete the checksum stage",
				Buckets:   prometheus.ExponentialBuckets(1, 2.2679331552660544, 10),
			}),

		LocalStorageUsageBytesGauge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "lightning",
				Name:      "local_storage_usage_bytes",
				Help:      "disk/memory size currently occupied by intermediate files in local backend",
			}, []string{"medium"}),

		ProgressGauge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "lightning",
				Name:      "progress",
				Help:      "progress of lightning phase",
			}, []string{"phase"}),
	}
}

// RegisterTo registers all metrics to the given registry.
func (m *Metrics) RegisterTo(r promutil.Registry) {
	r.MustRegister(
		m.ImporterEngineCounter,
		m.IdleWorkersGauge,
		m.KvEncoderCounter,
		m.TableCounter,
		m.ProcessedEngineCounter,
		m.ChunkCounter,
		m.BytesCounter,
		m.ImportSecondsHistogram,
		m.ChunkParserReadBlockSecondsHistogram,
		m.ApplyWorkerSecondsHistogram,
		m.RowReadSecondsHistogram,
		m.RowReadBytesHistogram,
		m.RowEncodeSecondsHistogram,
		m.RowKVDeliverSecondsHistogram,
		m.BlockDeliverSecondsHistogram,
		m.BlockDeliverBytesHistogram,
		m.BlockDeliverKVPairsHistogram,
		m.ChecksumSecondsHistogram,
		m.LocalStorageUsageBytesGauge,
		m.ProgressGauge,
	)
}

// UnregisterFrom unregisters all metrics from the given registry.
func (m *Metrics) UnregisterFrom(r promutil.Registry) {
	r.Unregister(m.ImporterEngineCounter)
	r.Unregister(m.IdleWorkersGauge)
	r.Unregister(m.KvEncoderCounter)
	r.Unregister(m.TableCounter)
	r.Unregister(m.ProcessedEngineCounter)
	r.Unregister(m.ChunkCounter)
	r.Unregister(m.BytesCounter)
	r.Unregister(m.ImportSecondsHistogram)
	r.Unregister(m.ChunkParserReadBlockSecondsHistogram)
	r.Unregister(m.ApplyWorkerSecondsHistogram)
	r.Unregister(m.RowReadSecondsHistogram)
	r.Unregister(m.RowReadBytesHistogram)
	r.Unregister(m.RowEncodeSecondsHistogram)
	r.Unregister(m.RowKVDeliverSecondsHistogram)
	r.Unregister(m.BlockDeliverSecondsHistogram)
	r.Unregister(m.BlockDeliverBytesHistogram)
	r.Unregister(m.BlockDeliverKVPairsHistogram)
	r.Unregister(m.ChecksumSecondsHistogram)
	r.Unregister(m.LocalStorageUsageBytesGauge)
	r.Unregister(m.ProgressGauge)
}

func (m *Metrics) RecordTableCount(status string, err error) {
	var result string
	if err != nil {
		result = TableResultFailure
	} else {
		result = TableResultSuccess
	}
	m.TableCounter.WithLabelValues(status, result).Inc()
}

func (m *Metrics) RecordEngineCount(status string, err error) {
	var result string
	if err != nil {
		result = TableResultFailure
	} else {
		result = TableResultSuccess
	}
	m.ProcessedEngineCounter.WithLabelValues(status, result).Inc()
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

type ctxKeyType struct{}

var ctxKey ctxKeyType

// NewContext returns a new context with the provided metrics.
func NewContext(ctx context.Context, metrics *Metrics) context.Context {
	return context.WithValue(ctx, ctxKey, metrics)
}

// FromContext returns the metrics stored in the context.
func FromContext(ctx context.Context) (*Metrics, bool) {
	m, ok := ctx.Value(ctxKey).(*Metrics)
	return m, ok
}

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

	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// metric label values.
const (
	TableStatePending   = "pending" // for the TableCounter labels, below too
	TableStateImported  = "imported"
	TableStateCompleted = "completed"

	StateTotalRestore   = "total_restore" // total source data bytes needs to restore
	StateRestored       = "restored"      // source data bytes restored during restore engine
	StateRestoreWritten = "written"       // kv bytes written during restore engine
	StateImported       = "imported"      // kv bytes imported during import engine

	ProgressPhaseTotal   = "total"   // total restore progress(not include post-process, like checksum and analyze)
	ProgressPhaseRestore = "restore" // restore engine progress
	ProgressPhaseImport  = "import"  // import engine progress

	TableResultSuccess = "success" // for the TableCounter labels, below too
	TableResultFailure = "failure"

	ChunkStateEstimated = "estimated" // for the ChunkCounter labels, below too
	ChunkStatePending   = "pending"
	ChunkStateRunning   = "running"
	ChunkStateFinished  = "finished"
	ChunkStateFailed    = "failed"

	SSTProcessSplit  = "split" // for the SSTSecondsHistogram labels, below too
	SSTProcessWrite  = "write"
	SSTProcessIngest = "ingest"

	BlockDeliverKindIndex = "index"
	BlockDeliverKindData  = "data"

	lightningNamespace = "lightning"
)

// Common contains metrics shared for lightning and import-into.
// they will have different namespace.
// metrics here will have an additional task-id const-label when used for import-into.
type Common struct {
	ChunkCounter *prometheus.CounterVec
	// BytesCounter records the total bytes processed.
	// it has a state label, values includes:
	// 	- total_restore: total source file bytes needs to restore, it's constant.
	// 	- restored: source file bytes restored, i.e. encoded and sorted.
	// 	- written: kv bytes written during encode & sort.
	// 	- imported: kv bytes imported during import engine(this value don't account for replica).
	BytesCounter *prometheus.CounterVec
	RowsCounter  *prometheus.CounterVec
	// RowReadSecondsHistogram records the time spent on reading a batch of rows.
	// for each row, the time includes time spend on reading from file,
	// decompress if needed, and parsing into row data.
	// then sum up all rows in a batch, and record the time.
	RowReadSecondsHistogram prometheus.Histogram
	// RowEncodeSecondsHistogram records the time spent on encoding a batch of rows.
	RowEncodeSecondsHistogram    prometheus.Histogram
	BlockDeliverSecondsHistogram prometheus.Histogram
	BlockDeliverBytesHistogram   *prometheus.HistogramVec
	BlockDeliverKVPairsHistogram *prometheus.HistogramVec
}

// NewCommon returns common metrics instance.
func NewCommon(factory promutil.Factory, namespace, subsystem string, constLabels prometheus.Labels) *Common {
	return &Common{
		ChunkCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "chunks",
				Help:        "count number of chunks processed",
				ConstLabels: constLabels,
			}, []string{"state"}),

		BytesCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "bytes",
				Help:        "count of total bytes",
				ConstLabels: constLabels,
			}, []string{"state"}),
		// state can be one of:
		//  - estimated (an estimation derived from the file size)
		//  - pending
		//  - running
		//  - finished
		//  - failed

		RowsCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "rows",
				Help:        "count of total rows",
				ConstLabels: constLabels,
			}, []string{"state", "table"}),

		RowReadSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "row_read_seconds",
				Help:        "time needed to parse a row(include time to read and decompress file)",
				ConstLabels: constLabels,
				Buckets:     prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 7),
			}),

		RowEncodeSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "row_encode_seconds",
				Help:        "time needed to encode a row",
				ConstLabels: constLabels,
				Buckets:     prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),

		BlockDeliverSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "block_deliver_seconds",
				Help:        "time needed to deliver a block",
				ConstLabels: constLabels,
				Buckets:     prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),
		BlockDeliverBytesHistogram: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "block_deliver_bytes",
				Help:        "number of bytes being sent out to importer",
				ConstLabels: constLabels,
				Buckets:     prometheus.ExponentialBuckets(512, 2, 10),
			}, []string{"kind"}),
		BlockDeliverKVPairsHistogram: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace:   namespace,
				Subsystem:   subsystem,
				Name:        "block_deliver_kv_pairs",
				Help:        "number of KV pairs being sent out to importer",
				ConstLabels: constLabels,
				Buckets:     prometheus.ExponentialBuckets(1, 2, 10),
			}, []string{"kind"}),
	}
}

// RegisterTo registers all metrics to the given registry.
func (c *Common) RegisterTo(r promutil.Registry) {
	r.MustRegister(
		c.ChunkCounter,
		c.BytesCounter,
		c.RowsCounter,
		c.RowReadSecondsHistogram,
		c.RowEncodeSecondsHistogram,
		c.BlockDeliverSecondsHistogram,
		c.BlockDeliverBytesHistogram,
		c.BlockDeliverKVPairsHistogram,
	)
}

// UnregisterFrom unregisters all metrics from the given registry.
func (c *Common) UnregisterFrom(r promutil.Registry) {
	r.Unregister(c.ChunkCounter)
	r.Unregister(c.BytesCounter)
	r.Unregister(c.RowsCounter)
	r.Unregister(c.RowReadSecondsHistogram)
	r.Unregister(c.RowEncodeSecondsHistogram)
	r.Unregister(c.BlockDeliverSecondsHistogram)
	r.Unregister(c.BlockDeliverBytesHistogram)
	r.Unregister(c.BlockDeliverKVPairsHistogram)
}

// Metrics contains all metrics used by lightning.
type Metrics struct {
	ImporterEngineCounter                *prometheus.CounterVec
	IdleWorkersGauge                     *prometheus.GaugeVec
	KvEncoderCounter                     *prometheus.CounterVec
	TableCounter                         *prometheus.CounterVec
	ProcessedEngineCounter               *prometheus.CounterVec
	ImportSecondsHistogram               prometheus.Histogram
	ChunkParserReadBlockSecondsHistogram prometheus.Histogram
	ApplyWorkerSecondsHistogram          *prometheus.HistogramVec
	RowKVDeliverSecondsHistogram         prometheus.Histogram
	// RowReadBytesHistogram records the number of bytes read from data source
	// for a batch of rows.
	// it's a little duplicate with RowsCounter of state = "restored".
	RowReadBytesHistogram       prometheus.Histogram
	ChecksumSecondsHistogram    prometheus.Histogram
	SSTSecondsHistogram         *prometheus.HistogramVec
	LocalStorageUsageBytesGauge *prometheus.GaugeVec
	ProgressGauge               *prometheus.GaugeVec
	*Common
}

// NewMetrics creates a new empty metrics.
func NewMetrics(factory promutil.Factory) *Metrics {
	c := NewCommon(factory, lightningNamespace, "", nil)
	return &Metrics{
		Common: c,
		ImporterEngineCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: lightningNamespace,
				Name:      "importer_engine",
				Help:      "counting open and closed importer engines",
			}, []string{"type"}),

		IdleWorkersGauge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: lightningNamespace,
				Name:      "idle_workers",
				Help:      "counting idle workers",
			}, []string{"name"}),

		KvEncoderCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: lightningNamespace,
				Name:      "kv_encoder",
				Help:      "counting kv open and closed kv encoder",
			}, []string{"type"}),

		TableCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: lightningNamespace,
				Name:      "tables",
				Help:      "count number of tables processed",
			}, []string{"state", "result"}),

		ProcessedEngineCounter: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: lightningNamespace,
				Name:      "engines",
				Help:      "count number of engines processed",
			}, []string{"state", "result"}),

		ImportSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: lightningNamespace,
				Name:      "import_seconds",
				Help:      "time needed to import a table",
				Buckets:   prometheus.ExponentialBuckets(0.125, 2, 6),
			}),

		ChunkParserReadBlockSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: lightningNamespace,
				Name:      "chunk_parser_read_block_seconds",
				Help:      "time needed for chunk parser read a block",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),

		ApplyWorkerSecondsHistogram: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: lightningNamespace,
				Name:      "apply_worker_seconds",
				Help:      "time needed to apply a worker",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}, []string{"name"}),
		RowKVDeliverSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: lightningNamespace,
				Name:      "row_kv_deliver_seconds",
				Help:      "time needed to send kvs to deliver loop",
				Buckets:   prometheus.ExponentialBuckets(0.001, 3.1622776601683795, 10),
			}),

		RowReadBytesHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: lightningNamespace,
				Name:      "row_read_bytes",
				Help:      "number of bytes being read out from data source",
				Buckets:   prometheus.ExponentialBuckets(1024, 2, 8),
			}),
		ChecksumSecondsHistogram: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: lightningNamespace,
				Name:      "checksum_seconds",
				Help:      "time needed to complete the checksum stage",
				Buckets:   prometheus.ExponentialBuckets(1, 2.2679331552660544, 10),
			}),
		SSTSecondsHistogram: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: lightningNamespace,
				Name:      "sst_seconds",
				Help:      "time needed to complete the sst operations",
				Buckets:   prometheus.ExponentialBuckets(1, 2.2679331552660544, 10),
			}, []string{"kind"}),

		LocalStorageUsageBytesGauge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: lightningNamespace,
				Name:      "local_storage_usage_bytes",
				Help:      "disk/memory size currently occupied by intermediate files in local backend",
			}, []string{"medium"}),

		ProgressGauge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: lightningNamespace,
				Name:      "progress",
				Help:      "progress of lightning phase",
			}, []string{"phase"}),
	}
}

// RegisterTo registers all metrics to the given registry.
func (m *Metrics) RegisterTo(r promutil.Registry) {
	m.Common.RegisterTo(r)
	r.MustRegister(
		m.ImporterEngineCounter,
		m.IdleWorkersGauge,
		m.KvEncoderCounter,
		m.TableCounter,
		m.ProcessedEngineCounter,
		m.ImportSecondsHistogram,
		m.ChunkParserReadBlockSecondsHistogram,
		m.ApplyWorkerSecondsHistogram,
		m.RowKVDeliverSecondsHistogram,
		m.RowReadBytesHistogram,
		m.ChecksumSecondsHistogram,
		m.SSTSecondsHistogram,
		m.LocalStorageUsageBytesGauge,
		m.ProgressGauge,
	)
}

// UnregisterFrom unregisters all metrics from the given registry.
func (m *Metrics) UnregisterFrom(r promutil.Registry) {
	m.Common.UnregisterFrom(r)
	r.Unregister(m.ImporterEngineCounter)
	r.Unregister(m.IdleWorkersGauge)
	r.Unregister(m.KvEncoderCounter)
	r.Unregister(m.TableCounter)
	r.Unregister(m.ProcessedEngineCounter)
	r.Unregister(m.ImportSecondsHistogram)
	r.Unregister(m.ChunkParserReadBlockSecondsHistogram)
	r.Unregister(m.ApplyWorkerSecondsHistogram)
	r.Unregister(m.RowKVDeliverSecondsHistogram)
	r.Unregister(m.RowReadBytesHistogram)
	r.Unregister(m.ChecksumSecondsHistogram)
	r.Unregister(m.SSTSecondsHistogram)
	r.Unregister(m.LocalStorageUsageBytesGauge)
	r.Unregister(m.ProgressGauge)
}

// RecordTableCount records the number of tables processed.
func (m *Metrics) RecordTableCount(status string, err error) {
	var result string
	if err != nil {
		result = TableResultFailure
	} else {
		result = TableResultSuccess
	}
	m.TableCounter.WithLabelValues(status, result).Inc()
}

// RecordEngineCount records the number of engines processed.
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

// ReadHistogram reports the current value of the histogram.
// for test only.
func ReadHistogram(counter prometheus.Histogram) *dto.Metric {
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return nil
	}
	return &metric
}

func metricHasLabel(labelPairs []*dto.LabelPair, labels prometheus.Labels) bool {
	for _, label := range labelPairs {
		if v, ok := labels[label.GetName()]; ok && v == label.GetValue() {
			return true
		}
	}
	return false
}

// ReadAllCounters reports the summary value of the counters with given labels.
func ReadAllCounters(metricsVec *prometheus.MetricVec, labels prometheus.Labels) float64 {
	metricsChan := make(chan prometheus.Metric, 8)
	go func() {
		metricsVec.Collect(metricsChan)
		close(metricsChan)
	}()

	var sum float64
	for counter := range metricsChan {
		var metric dto.Metric
		if err := counter.Write(&metric); err != nil {
			return math.NaN()
		}
		if !metricHasLabel(metric.GetLabel(), labels) {
			continue
		}
		sum += metric.Counter.GetValue()
	}
	return sum
}

// ReadHistogramSum reports the sum of all observed values in the histogram.
func ReadHistogramSum(histogram prometheus.Histogram) float64 {
	var metric dto.Metric
	if err := histogram.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Histogram.GetSampleSum()
}

type ctxKeyType string

var (
	allMetricKey    ctxKeyType = "all-metrics"
	commonMetricKey ctxKeyType = "common-metrics"
)

// WithMetric returns a new context with the provided metrics.
func WithMetric(ctx context.Context, metrics *Metrics) context.Context {
	return context.WithValue(
		WithCommonMetric(ctx, metrics.Common),
		allMetricKey, metrics)
}

// WithCommonMetric returns a new context with the provided common metrics.
func WithCommonMetric(ctx context.Context, commonMetric *Common) context.Context {
	return context.WithValue(ctx, commonMetricKey, commonMetric)
}

// FromContext returns the metrics stored in the context.
func FromContext(ctx context.Context) (*Metrics, bool) {
	m, ok := ctx.Value(allMetricKey).(*Metrics)
	return m, ok
}

// GetCommonMetric returns the common metrics stored in the context.
func GetCommonMetric(ctx context.Context) (*Common, bool) {
	m, ok := ctx.Value(commonMetricKey).(*Common)
	return m, ok
}

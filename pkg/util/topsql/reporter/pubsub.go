// Copyright 2021 PingCAP, Inc.
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

package reporter

import (
	"context"
	"errors"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	reporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// TopSQLPubSubService implements tipb.TopSQLPubSubServer.
//
// If a client subscribes to TopSQL records, the TopSQLPubSubService is responsible
// for registering an associated DataSink to the reporter. Then the DataSink sends
// data to the client periodically.
type TopSQLPubSubService struct {
	dataSinkRegisterer DataSinkRegisterer
}

// NewTopSQLPubSubService creates a new TopSQLPubSubService.
func NewTopSQLPubSubService(dataSinkRegisterer DataSinkRegisterer) *TopSQLPubSubService {
	return &TopSQLPubSubService{dataSinkRegisterer: dataSinkRegisterer}
}

var _ tipb.TopSQLPubSubServer = &TopSQLPubSubService{}

// Subscribe registers dataSinks to the reporter and redirects data received from reporter
// to subscribers associated with those dataSinks.
func (ps *TopSQLPubSubService) Subscribe(req *tipb.TopSQLSubRequest, stream tipb.TopSQLPubSub_SubscribeServer) error {
	ds, err := newPubSubDataSink(req, stream, ps.dataSinkRegisterer)
	if err != nil {
		return err
	}
	if err := ps.dataSinkRegisterer.Register(ds); err != nil {
		return err
	}
	return ds.run()
}

type pubSubDataSink struct {
	ctx    context.Context
	cancel context.CancelFunc

	stream     tipb.TopSQLPubSub_SubscribeServer
	sendTaskCh chan sendTask

	// for deregister
	registerer DataSinkRegisterer

	// TopSQL subscription config
	enableTopSQL bool

	// TopRU subscription config
	enableTopRU  bool
	itemInterval tipb.ItemInterval
}

// parseTopSQLSubscription returns true when the request opts in to TopSQL.
// Empty collectors default to TopSQL enabled for backward compatibility.
func parseTopSQLSubscription(req *tipb.TopSQLSubRequest) bool {
	if req == nil {
		return true
	}

	collectors := req.GetCollectors()
	if len(collectors) == 0 {
		return true
	}
	for _, collector := range collectors {
		if collector == tipb.CollectorType_COLLECTOR_TYPE_TOPSQL || collector == tipb.CollectorType_COLLECTOR_TYPE_UNSPECIFIED {
			return true
		}
	}
	return false
}

// ErrTopRUConfig indicates the subscription requests TopRU but omits the Topru config.
var ErrTopRUConfig = errors.New("topru config is empty")

func parseTopRUSubscription(req *tipb.TopSQLSubRequest) (bool, tipb.ItemInterval, error) {
	if req == nil {
		return false, tipb.ItemInterval_ITEM_INTERVAL_UNSPECIFIED, nil
	}

	enabled := false
	for _, collector := range req.GetCollectors() {
		if collector == tipb.CollectorType_COLLECTOR_TYPE_TOPRU {
			enabled = true
			break
		}
	}
	if !enabled {
		return false, tipb.ItemInterval_ITEM_INTERVAL_UNSPECIFIED, nil
	}

	cfg := req.GetTopru()
	if cfg == nil {
		return false, tipb.ItemInterval_ITEM_INTERVAL_UNSPECIFIED, ErrTopRUConfig
	}

	return true, cfg.GetItemIntervalSeconds(), nil
}

// newPubSubDataSink creates a DataSink for PubSub subscription.
//
// It parses TopSQL/TopRU options from the request and stores them in the sink.
// Register enables TopRU when the sink has enableTopRU; Deregister disables it when the last such sink is removed.
// item_interval_seconds controls TopRURecordItem.timestamp_sec (15s/30s/60s).
// Requests without the TOPRU collector entry keep TopRU disabled for backward compatibility.
// It returns an error when the request includes TOPRU but omits the Topru config.
func newPubSubDataSink(req *tipb.TopSQLSubRequest, stream tipb.TopSQLPubSub_SubscribeServer, registerer DataSinkRegisterer) (*pubSubDataSink, error) {
	ctx, cancel := context.WithCancel(stream.Context())
	enableTopSQL := parseTopSQLSubscription(req)
	enableTopRU, itemInterval, err := parseTopRUSubscription(req)
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] pubsub datasink failed to parse top-ru config", zap.Error(err))
		cancel()
		return nil, err
	}

	ds := &pubSubDataSink{
		ctx:    ctx,
		cancel: cancel,

		stream:     stream,
		sendTaskCh: make(chan sendTask, 1),

		registerer: registerer,

		enableTopSQL: enableTopSQL,
		enableTopRU:  enableTopRU,
		itemInterval: itemInterval,
	}

	return ds, nil
}

var _ DataSink = &pubSubDataSink{}

func (ds *pubSubDataSink) TrySend(data *ReportData, deadline time.Time) error {
	select {
	case ds.sendTaskCh <- sendTask{data: data, deadline: deadline}:
		return nil
	case <-ds.ctx.Done():
		return ds.ctx.Err()
	default:
		reporter_metrics.IgnoreReportChannelFullCounter.Inc()
		return errors.New("the channel of pubsub dataSink is full")
	}
}

func (ds *pubSubDataSink) OnReporterClosing() {
	ds.cancel()
}

func (ds *pubSubDataSink) run() error {
	defer func() {
		if r := recover(); r != nil {
			// To catch panic when log grpc error. https://github.com/pingcap/tidb/issues/51301.
			logutil.BgLogger().Error("[top-sql] got panic in pub sub data sink, just ignore", zap.Error(util.GetRecoverError(r)))
		}
		ds.registerer.Deregister(ds)
		ds.cancel()
	}()

	for {
		select {
		case task := <-ds.sendTaskCh:
			ctx, rcancel := context.WithDeadline(ds.ctx, task.deadline)
			// use a cancel cause context to return error safely
			var cancel context.CancelCauseFunc
			ctx, cancel = context.WithCancelCause(ctx)

			start := time.Now()
			go util.WithRecovery(func() {
				var err error
				defer cancel(err)
				err = ds.doSend(ctx, task.data)
				if err != nil {
					reporter_metrics.ReportAllDurationFailedHistogram.Observe(time.Since(start).Seconds())
				} else {
					reporter_metrics.ReportAllDurationSuccHistogram.Observe(time.Since(start).Seconds())
				}
			}, nil)

			// When the deadline is exceeded, the closure inside `go util.WithRecovery` above may not notice that
			// immediately because it can be blocked by `stream.Send`.
			// In order to clean up resources as quickly as possible, we let that closure run in an individual goroutine,
			// and wait for timeout here.
			<-ctx.Done()
			// useless, it is called to prevent linter error
			rcancel()

			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				logutil.BgLogger().Warn(
					"[top-sql] pubsub datasink failed to send data to subscriber due to deadline exceeded",
					zap.Time("deadline", task.deadline),
				)
				return ctx.Err()
			}

			failpoint.Inject("mockGrpcLogPanic", nil)
			if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logutil.BgLogger().Warn(
					"[top-sql] pubsub datasink failed to send data to subscriber",
					zap.Error(err),
				)
				return err
			}
		case <-ds.ctx.Done():
			return ds.ctx.Err()
		}
	}
}

func (ds *pubSubDataSink) doSend(ctx context.Context, data *ReportData) error {
	if err := ds.sendTopSQLRecords(ctx, data.DataRecords); err != nil {
		return err
	}
	if err := ds.sendTopRURecords(ctx, data.RURecords); err != nil {
		return err
	}
	if err := ds.sendSQLMeta(ctx, data.SQLMetas); err != nil {
		return err
	}
	return ds.sendPlanMeta(ctx, data.PlanMetas)
}

func (ds *pubSubDataSink) sendTopSQLRecords(ctx context.Context, records []tipb.TopSQLRecord) (err error) {
	if !ds.enableTopSQL {
		return nil
	}
	if len(records) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		reporter_metrics.TopSQLReportRecordCounterHistogram.Observe(float64(sentCount))
		if err != nil {
			reporter_metrics.ReportRecordDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reporter_metrics.ReportRecordDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}()

	topSQLRecord := &tipb.TopSQLSubResponse_Record{}
	r := &tipb.TopSQLSubResponse{RespOneof: topSQLRecord}

	for i := range records {
		topSQLRecord.Record = &records[i]
		if err = ds.stream.Send(r); err != nil {
			return
		}
		sentCount++

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
	}

	return
}

// sendTopRURecords sends TopRU records to subscriber via PubSub stream.
//
// It returns early if there is no record or TopRU is disabled.
// It uses TopSQLSubResponse_RuRecord (protocol field 4).
func (ds *pubSubDataSink) sendTopRURecords(ctx context.Context, records []tipb.TopRURecord) (err error) {
	if len(records) == 0 {
		return
	}

	// Defense in depth: only send RU records when TopRU is enabled.
	if !ds.enableTopRU || !topsqlstate.TopRUEnabled() {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		reporter_metrics.TopSQLReportRURecordCounterHistogram.Observe(float64(sentCount))
		if err != nil {
			reporter_metrics.ReportRURecordDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reporter_metrics.ReportRURecordDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}()

	topRURecord := &tipb.TopSQLSubResponse_RuRecord{}
	r := &tipb.TopSQLSubResponse{RespOneof: topRURecord}

	for i := range records {
		topRURecord.RuRecord = &records[i]
		if err = ds.stream.Send(r); err != nil {
			return
		}
		sentCount++

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
	}

	return
}

func (ds *pubSubDataSink) sendSQLMeta(ctx context.Context, sqlMetas []tipb.SQLMeta) (err error) {
	if len(sqlMetas) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		reporter_metrics.TopSQLReportSQLCountHistogram.Observe(float64(sentCount))
		if err != nil {
			reporter_metrics.ReportSQLDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reporter_metrics.ReportSQLDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}()

	sqlMeta := &tipb.TopSQLSubResponse_SqlMeta{}
	r := &tipb.TopSQLSubResponse{RespOneof: sqlMeta}

	for i := range sqlMetas {
		sqlMeta.SqlMeta = &sqlMetas[i]
		if err = ds.stream.Send(r); err != nil {
			return
		}
		sentCount++

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
	}

	return
}

func (ds *pubSubDataSink) sendPlanMeta(ctx context.Context, planMetas []tipb.PlanMeta) (err error) {
	if len(planMetas) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		reporter_metrics.TopSQLReportPlanCountHistogram.Observe(float64(sentCount))
		if err != nil {
			reporter_metrics.ReportPlanDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reporter_metrics.ReportPlanDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}()

	planMeta := &tipb.TopSQLSubResponse_PlanMeta{}
	r := &tipb.TopSQLSubResponse{RespOneof: planMeta}

	for i := range planMetas {
		planMeta.PlanMeta = &planMetas[i]
		if err = ds.stream.Send(r); err != nil {
			return
		}
		sentCount++

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
	}

	return
}

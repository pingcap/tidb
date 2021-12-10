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
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// TopSQLPubSubService implements tipb.TopSQLPubSubServer.
//
// If a client subscribes to TopSQL records, the TopSQLPubSubService is responsible
// for registering an associated DataSink to the reporter. Then the reporter sends
// data to the client periodically.
type TopSQLPubSubService struct {
	dataSinkRegHandle DataSinkRegHandle
}

// NewTopSQLPubSubService creates a new TopSQLPubSubService.
func NewTopSQLPubSubService(
	dataSinkRegHandle DataSinkRegHandle,
) *TopSQLPubSubService {
	return &TopSQLPubSubService{
		dataSinkRegHandle: dataSinkRegHandle,
	}
}

var _ tipb.TopSQLPubSubServer = &TopSQLPubSubService{}

// Subscribe registers dataSinks to the reporter and redirects data received from reporter
// to subscribers associated with those dataSinks.
func (t *TopSQLPubSubService) Subscribe(
	_ *tipb.TopSQLSubRequest,
	stream tipb.TopSQLPubSub_SubscribeServer,
) error {
	ds := newPubSubDataSink(stream)

	if err := t.dataSinkRegHandle.Register(ds); err != nil {
		return err
	}

	ds.run()
	return nil
}

type pubSubDataSink struct {
	stream     tipb.TopSQLPubSub_SubscribeServer
	sendTaskCh chan sendTask
	isDown     *atomic.Bool
}

func newPubSubDataSink(
	stream tipb.TopSQLPubSub_SubscribeServer,
) *pubSubDataSink {
	return &pubSubDataSink{
		stream:     stream,
		sendTaskCh: make(chan sendTask),
		isDown:     atomic.NewBool(false),
	}
}

var _ DataSink = &pubSubDataSink{}

func (s *pubSubDataSink) Send(data ReportData, deadline time.Time) {
	if s.IsDown() {
		return
	}

	select {
	case s.sendTaskCh <- sendTask{data: data, deadline: deadline}:
		// sent successfully
	default:
		ignoreReportChannelFullCounter.Inc()
		logutil.BgLogger().Warn("[top-sql] report channel is full")
	}
}

func (s *pubSubDataSink) IsPaused() bool {
	return false
}

func (s *pubSubDataSink) IsDown() bool {
	return s.isDown.Load()
}

func (s *pubSubDataSink) Close() {
	close(s.sendTaskCh)
}

func (s *pubSubDataSink) run() {
	defer s.isDown.Store(true)

	for task := range s.sendTaskCh {
		ctx, cancel := context.WithDeadline(context.Background(), task.deadline)
		start := time.Now()

		var err error
		doneCh := make(chan struct{})
		go util.WithRecovery(func() {
			defer func() {
				doneCh <- struct{}{}
				cancel()
			}()
			err = s.doSend(ctx, task.data)
			if err != nil {
				reportAllDurationFailedHistogram.Observe(time.Since(start).Seconds())
			} else {
				reportAllDurationSuccHistogram.Observe(time.Since(start).Seconds())
			}
		}, nil)

		select {
		case <-doneCh:
			if err != nil {
				logutil.BgLogger().Warn(
					"[top-sql] pubsub datasink failed to send data to subscriber",
					zap.Error(err),
				)
				return
			}
		case <-ctx.Done():
			logutil.BgLogger().Warn(
				"[top-sql] pubsub datasink failed to send data to subscriber due to timeout",
				zap.Time("deadline", task.deadline),
			)
			return
		}
	}
}

func (s *pubSubDataSink) doSend(ctx context.Context, data ReportData) error {
	if err := s.sendCPUTime(ctx, data.CPUTimeRecords); err != nil {
		return err
	}
	if err := s.sendSQLMeta(ctx, data.SQLMetas); err != nil {
		return err
	}
	return s.sendPlanMeta(ctx, data.PlanMetas)
}

func (s *pubSubDataSink) sendCPUTime(ctx context.Context, records []*tipb.CPUTimeRecord) (err error) {
	if len(records) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		topSQLReportRecordCounterHistogram.Observe(float64(sentCount))
		if err != nil {
			reportRecordDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reportRecordDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}()

	cpuRecord := &tipb.TopSQLSubResponse_Record{}
	r := &tipb.TopSQLSubResponse{RespOneof: cpuRecord}

	for i := range records {
		cpuRecord.Record = records[i]
		if err = s.stream.Send(r); err != nil {
			return
		}
		sentCount += 1

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
	}

	return
}

func (s *pubSubDataSink) sendSQLMeta(ctx context.Context, sqlMetas []*tipb.SQLMeta) (err error) {
	if len(sqlMetas) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		topSQLReportSQLCountHistogram.Observe(float64(sentCount))
		if err != nil {
			reportSQLDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reportSQLDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}()

	sqlMeta := &tipb.TopSQLSubResponse_SqlMeta{}
	r := &tipb.TopSQLSubResponse{RespOneof: sqlMeta}

	for i := range sqlMetas {
		sqlMeta.SqlMeta = sqlMetas[i]
		if err = s.stream.Send(r); err != nil {
			return
		}
		sentCount += 1

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
	}

	return
}

func (s *pubSubDataSink) sendPlanMeta(ctx context.Context, planMetas []*tipb.PlanMeta) (err error) {
	if len(planMetas) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		topSQLReportPlanCountHistogram.Observe(float64(sentCount))
		if err != nil {
			reportPlanDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reportPlanDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}()

	planMeta := &tipb.TopSQLSubResponse_PlanMeta{}
	r := &tipb.TopSQLSubResponse{RespOneof: planMeta}

	for i := range planMetas {
		planMeta.PlanMeta = planMetas[i]
		if err = s.stream.Send(r); err != nil {
			return
		}
		sentCount += 1

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		default:
		}
	}

	return
}

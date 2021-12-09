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
	"sync"
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
	decodePlan             planBinaryDecodeFunc
	dataSinkRegisterHandle DataSinkRegisterHandle
}

// NewTopSQLPubSubService creates a new TopSQLPubSubService.
func NewTopSQLPubSubService(
	decodePlan planBinaryDecodeFunc,
	dataSinkRegisterHandle DataSinkRegisterHandle,
) *TopSQLPubSubService {
	return &TopSQLPubSubService{
		decodePlan:             decodePlan,
		dataSinkRegisterHandle: dataSinkRegisterHandle,
	}
}

var _ tipb.TopSQLPubSubServer = &TopSQLPubSubService{}

// Subscribe registers dataSinks to the reporter and redirects data received from reporter
// to subscribers associated with those dataSinks.
func (t *TopSQLPubSubService) Subscribe(
	_ *tipb.TopSQLSubRequest,
	stream tipb.TopSQLPubSub_SubscribeServer,
) error {
	sc := newPubSubDataSink(stream, t.decodePlan)

	if err := t.dataSinkRegisterHandle.Register(sc); err != nil {
		return err
	}

	sc.run()
	return nil
}

type pubSubDataSink struct {
	stream     tipb.TopSQLPubSub_SubscribeServer
	sendTaskCh chan sendTask
	isDown     *atomic.Bool

	decodePlan planBinaryDecodeFunc
}

func newPubSubDataSink(
	stream tipb.TopSQLPubSub_SubscribeServer,
	decodePlan planBinaryDecodeFunc,
) *pubSubDataSink {
	return &pubSubDataSink{
		stream:     stream,
		sendTaskCh: make(chan sendTask),
		isDown:     atomic.NewBool(false),

		decodePlan: decodePlan,
	}
}

var _ DataSink = &pubSubDataSink{}

func (s *pubSubDataSink) Send(data reportData, timeout time.Duration) {
	if s.IsDown() {
		return
	}

	select {
	case s.sendTaskCh <- sendTask{data: data, timeout: timeout}:
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
		ctx, cancel := context.WithTimeout(context.Background(), task.timeout)
		start := time.Now()

		var err error
		doneCh := make(chan struct{})
		go func(task sendTask) {
			util.WithRecovery(func() {
				defer func() {
					doneCh <- struct{}{}
				}()
				err = s.doSend(ctx, task.data)
				if err != nil {
					reportAllDurationFailedHistogram.Observe(time.Since(start).Seconds())
				} else {
					reportAllDurationSuccHistogram.Observe(time.Since(start).Seconds())
				}
			}, nil)
		}(task)

		select {
		case <-doneCh:
			cancel()
			if err != nil {
				logutil.BgLogger().Warn(
					"[top-sql] pubsub data sink failed to send data to subscriber",
					zap.Error(err),
				)
				return
			}
		case <-ctx.Done():
			cancel()
			logutil.BgLogger().Warn(
				"[top-sql] pubsub data sink failed to send data to subscriber due to timeout",
				zap.Duration("timeout", task.timeout),
			)
			return
		}
	}
}

func (s *pubSubDataSink) doSend(ctx context.Context, data reportData) error {
	if err := s.sendCPUTime(ctx, data.collectedData); err != nil {
		return err
	}
	if err := s.sendSQLMeta(ctx, data.normalizedSQLMap); err != nil {
		return err
	}
	return s.sendPlanMeta(ctx, data.normalizedPlanMap)
}

func (s *pubSubDataSink) sendCPUTime(ctx context.Context, data []*dataPoints) (err error) {
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

	r := &tipb.TopSQLSubResponse{}
	record := &tipb.CPUTimeRecord{}
	r.RespOneof = &tipb.TopSQLSubResponse_Record{Record: record}

	for i := range data {
		point := data[i]
		record.SqlDigest = point.SQLDigest
		record.PlanDigest = point.PlanDigest
		record.RecordListCpuTimeMs = point.CPUTimeMsList
		record.RecordListTimestampSec = point.TimestampList
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

func (s *pubSubDataSink) sendSQLMeta(ctx context.Context, sqlMetaMap *sync.Map) (err error) {
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

	r := &tipb.TopSQLSubResponse{}
	sqlMeta := &tipb.SQLMeta{}
	r.RespOneof = &tipb.TopSQLSubResponse_SqlMeta{SqlMeta: sqlMeta}

	sqlMetaMap.Range(func(key, value interface{}) bool {
		meta := value.(SQLMeta)
		sqlMeta.SqlDigest = []byte(key.(string))
		sqlMeta.NormalizedSql = meta.normalizedSQL
		sqlMeta.IsInternalSql = meta.isInternal
		if err = s.stream.Send(r); err != nil {
			return false
		}
		sentCount += 1

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return false
		default:
		}

		return true
	})
	if err != nil {
		return err
	}

	return
}

func (s *pubSubDataSink) sendPlanMeta(ctx context.Context, planMetaMap *sync.Map) (err error) {
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

	r := &tipb.TopSQLSubResponse{}
	planMeta := &tipb.PlanMeta{}
	r.RespOneof = &tipb.TopSQLSubResponse_PlanMeta{PlanMeta: planMeta}
	planMetaMap.Range(func(key, value interface{}) bool {
		planDecoded, err1 := s.decodePlan(value.(string))
		if err1 != nil {
			logutil.BgLogger().Warn("[top-sql] decode plan failed", zap.Error(err1))
			return true
		}

		planMeta.PlanDigest = []byte(key.(string))
		planMeta.NormalizedPlan = planDecoded
		if err = s.stream.Send(r); err != nil {
			return false
		}
		sentCount += 1

		select {
		case <-ctx.Done():
			err = ctx.Err()
			return false
		default:
		}

		return true
	})
	if err != nil {
		return err
	}

	return
}

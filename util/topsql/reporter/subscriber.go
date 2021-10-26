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

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// TopSQLPublisher implements TopSQLPublisher.
//
// If a client subscribes to TopSQL records, the TopSQLPublisher is responsible for registering them to the reporter.
// Then the reporter sends data to the client periodically.
type TopSQLPublisher struct {
	decodePlan     planBinaryDecodeFunc
	clientRegistry *ReportClientRegistry
}

// NewTopSQLPublisher creates a new TopSQLPublisher.
func NewTopSQLPublisher(
	decodePlan planBinaryDecodeFunc,
	clientRegistry *ReportClientRegistry,
) *TopSQLPublisher {
	return &TopSQLPublisher{
		decodePlan:     decodePlan,
		clientRegistry: clientRegistry,
	}
}

var _ tipb.TopSQLPubSubServer = &TopSQLPublisher{}

// Subscribe registers clients to the reporter and redirects data received from reporter
// to subscribers associated with those clients.
func (t *TopSQLPublisher) Subscribe(
	_ *tipb.TopSQLSubRequest,
	stream tipb.TopSQLPubSub_SubscribeServer,
) error {
	sc := newSubClient(stream, t.decodePlan)

	var wg sync.WaitGroup
	wg.Add(1)
	go sc.run(&wg)

	t.clientRegistry.register(sc)

	wg.Wait()
	return nil
}

type subClient struct {
	stream   tipb.TopSQLPubSub_SubscribeServer
	sendTask chan struct {
		data    reportData
		timeout time.Duration
	}
	isDown *atomic.Bool

	decodePlan planBinaryDecodeFunc
}

func newSubClient(
	stream tipb.TopSQLPubSub_SubscribeServer,
	decodePlan planBinaryDecodeFunc,
) *subClient {
	sendTask := make(chan struct {
		data    reportData
		timeout time.Duration
	})
	return &subClient{
		stream:   stream,
		sendTask: sendTask,
		isDown:   atomic.NewBool(false),

		decodePlan: decodePlan,
	}
}

func (s *subClient) run(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		s.isDown.Store(true)
	}()

	for task := range s.sendTask {
		ctx, cancel := context.WithTimeout(context.Background(), task.timeout)
		start := time.Now()
		err := s.doSend(ctx, task.data)
		cancel()
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] client failed to send data to subscriber", zap.Error(err))
			reportAllDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reportAllDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}
}

func (s *subClient) doSend(ctx context.Context, data reportData) error {
	if err := s.sendCPUTime(ctx, data.collectedData); err != nil {
		return err
	}
	if err := s.sendSQLMeta(ctx, data.normalizedSQLMap); err != nil {
		return err
	}
	return s.sendPlanMeta(ctx, data.normalizedPlanMap)
}

func (s *subClient) sendCPUTime(ctx context.Context, data []*dataPoints) (err error) {
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

func (s *subClient) sendSQLMeta(ctx context.Context, sqlMetaMap *sync.Map) (err error) {
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

func (s *subClient) sendPlanMeta(ctx context.Context, planMetaMap *sync.Map) (err error) {
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

var _ ReportClient = &subClient{}

func (s *subClient) Send(data reportData, timeout time.Duration) {
	if s.IsDown() {
		return
	}

	select {
	case s.sendTask <- struct {
		data    reportData
		timeout time.Duration
	}{data: data, timeout: timeout}:
		// sent successfully
	default:
		ignoreReportChannelFullCounter.Inc()
		logutil.BgLogger().Warn("[top-sql] report channel is full")
	}
}

func (s *subClient) IsPending() bool {
	return false
}

func (s *subClient) IsDown() bool {
	return s.isDown.Load()
}

func (s *subClient) Close() {
	close(s.sendTask)
}

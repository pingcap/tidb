package reporter

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type TopSQLPublisher struct {
	decodePlan     planBinaryDecodeFunc
	clientRegistry *ReportClientRegistry
}

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
	stream tipb.TopSQLPubSub_SubscribeServer
	dataCh chan reportData
	isDown *atomic.Bool

	decodePlan planBinaryDecodeFunc
}

func newSubClient(
	stream tipb.TopSQLPubSub_SubscribeServer,
	decodePlan planBinaryDecodeFunc,
) *subClient {
	dataCh := make(chan reportData)
	return &subClient{
		stream: stream,
		dataCh: dataCh,
		isDown: atomic.NewBool(false),

		decodePlan: decodePlan,
	}
}

func (s *subClient) run(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		s.isDown.Store(true)
	}()

	for data := range s.dataCh {
		var err error
		r := &tipb.TopSQLSubResponse{}

		record := &tipb.CPUTimeRecord{}
		r.RespOneof = &tipb.TopSQLSubResponse_Record{Record: record}
		for i := range data.collectedData {
			point := data.collectedData[i]
			record.SqlDigest = point.SQLDigest
			record.PlanDigest = point.PlanDigest
			record.RecordListCpuTimeMs = point.CPUTimeMsList
			record.RecordListTimestampSec = point.TimestampList
			if err = s.stream.Send(r); err != nil {
				logutil.BgLogger().Warn("[top-sql] failed to send record to the subscriber", zap.Error(err))
				return
			}
		}

		sqlMeta := &tipb.SQLMeta{}
		r.RespOneof = &tipb.TopSQLSubResponse_SqlMeta{SqlMeta: sqlMeta}
		data.normalizedSQLMap.Range(func(key, value interface{}) bool {
			meta := value.(SQLMeta)
			sqlMeta.SqlDigest = []byte(key.(string))
			sqlMeta.NormalizedSql = meta.normalizedSQL
			sqlMeta.IsInternalSql = meta.isInternal
			if err = s.stream.Send(r); err != nil {
				return false
			}
			return true
		})
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] failed to send SQL meta to the subscriber", zap.Error(err))
			return
		}

		planMeta := &tipb.PlanMeta{}
		r.RespOneof = &tipb.TopSQLSubResponse_PlanMeta{PlanMeta: planMeta}
		data.normalizedPlanMap.Range(func(key, value interface{}) bool {
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
			return true
		})
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] failed to send plan meta to the subscriber", zap.Error(err))
			return
		}
	}
}

var _ ReportClient = &subClient{}

func (s *subClient) Send(_ context.Context, data reportData) error {
	select {
	case s.dataCh <- data:
		// sent successfully
	default:
		logutil.BgLogger().Warn("[top-sql] data channel is full")
	}
	return nil
}

func (s *subClient) IsPending() bool {
	return false
}

func (s *subClient) IsDown() bool {
	return s.isDown.Load()
}

func (s *subClient) Close() {
	close(s.dataCh)
}

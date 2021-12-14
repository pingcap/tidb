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
	"math"
	"sync"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// SingleTargetDataSink reports data to grpc servers.
type SingleTargetDataSink struct {
	ctx    context.Context
	cancel context.CancelFunc

	curRPCAddr string
	conn       *grpc.ClientConn
	sendTaskCh chan sendTask

	// calling decodePlan this can take a while, so should not block critical paths
	decodePlan planBinaryDecodeFunc
}

// NewSingleTargetDataSink returns a new SingleTargetDataSink
func NewSingleTargetDataSink(decodePlan planBinaryDecodeFunc) *SingleTargetDataSink {
	ctx, cancel := context.WithCancel(context.Background())
	dataSink := &SingleTargetDataSink{
		ctx:    ctx,
		cancel: cancel,

		curRPCAddr: "",
		conn:       nil,
		sendTaskCh: make(chan sendTask, 1),

		decodePlan: decodePlan,
	}
	go util.WithRecovery(dataSink.run, nil)
	return dataSink
}

// run will return when SingleTargetDataSink is closed.
func (ds *SingleTargetDataSink) run() {
	for {
		var task sendTask
		select {
		case <-ds.ctx.Done():
			return
		case task = <-ds.sendTaskCh:
		}

		targetRPCAddr := config.GetGlobalConfig().TopSQL.ReceiverAddress
		if targetRPCAddr == "" {
			continue
		}

		ctx, cancel := context.WithDeadline(context.Background(), task.deadline)
		start := time.Now()
		err := ds.doSend(ctx, targetRPCAddr, task.data)
		cancel()
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] single target data sink failed to send data to receiver", zap.Error(err))
			reportAllDurationFailedHistogram.Observe(time.Since(start).Seconds())
		} else {
			reportAllDurationSuccHistogram.Observe(time.Since(start).Seconds())
		}
	}
}

var _ DataSink = &SingleTargetDataSink{}

// TrySend implements the DataSink interface.
// Currently the implementation will establish a new connection every time,
// which is suitable for a per-minute sending period
func (ds *SingleTargetDataSink) TrySend(data ReportData, deadline time.Time) error {
	select {
	case ds.sendTaskCh <- sendTask{data: data, deadline: deadline}:
		return nil
	case <-ds.ctx.Done():
		return ds.ctx.Err()
	default:
		ignoreReportChannelFullCounter.Inc()
		return errors.New("the channel of single target dataSink is full")
	}
}

// IsPaused implements the DataSink interface.
func (ds *SingleTargetDataSink) IsPaused() bool {
	return len(config.GetGlobalConfig().TopSQL.ReceiverAddress) == 0
}

// IsDown implements the DataSink interface.
func (ds *SingleTargetDataSink) IsDown() bool {
	select {
	case <-ds.ctx.Done():
		return true
	default:
		return false
	}
}

// Close uses to close grpc connection.
func (ds *SingleTargetDataSink) Close() {
	ds.cancel()
	if ds.conn == nil {
		return
	}
	err := ds.conn.Close()
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] single target dataSink close connection failed", zap.Error(err))
	}
	ds.conn = nil
}

func (ds *SingleTargetDataSink) doSend(ctx context.Context, addr string, data ReportData) error {
	err := ds.tryEstablishConnection(ctx, addr)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	wg.Add(3)

	go func() {
		defer wg.Done()
		errCh <- ds.sendBatchSQLMeta(ctx, data.normalizedSQLMap)
	}()
	go func() {
		defer wg.Done()
		errCh <- ds.sendBatchPlanMeta(ctx, data.normalizedPlanMap)
	}()
	go func() {
		defer wg.Done()
		errCh <- ds.sendBatchCPUTimeRecord(ctx, data.collectedData)
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// sendBatchCPUTimeRecord sends a batch of TopSQL records by stream.
func (ds *SingleTargetDataSink) sendBatchCPUTimeRecord(ctx context.Context, records []*dataPoints) error {
	if len(records) == 0 {
		return nil
	}
	start := time.Now()
	client := tipb.NewTopSQLAgentClient(ds.conn)
	stream, err := client.ReportCPUTimeRecords(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		record := &tipb.CPUTimeRecord{
			RecordListTimestampSec: record.TimestampList,
			RecordListCpuTimeMs:    record.CPUTimeMsList,
			SqlDigest:              record.SQLDigest,
			PlanDigest:             record.PlanDigest,
		}
		if err := stream.Send(record); err != nil {
			return err
		}
	}
	topSQLReportRecordCounterHistogram.Observe(float64(len(records)))
	// See https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream for how to avoid leaking the stream
	_, err = stream.CloseAndRecv()
	if err != nil {
		reportRecordDurationFailedHistogram.Observe(time.Since(start).Seconds())
		return err
	}
	reportRecordDurationSuccHistogram.Observe(time.Since(start).Seconds())
	return nil
}

// sendBatchSQLMeta sends a batch of SQL metas by stream.
func (ds *SingleTargetDataSink) sendBatchSQLMeta(ctx context.Context, sqlMap *sync.Map) error {
	start := time.Now()
	client := tipb.NewTopSQLAgentClient(ds.conn)
	stream, err := client.ReportSQLMeta(ctx)
	if err != nil {
		return err
	}
	cnt := 0
	sqlMap.Range(func(key, value interface{}) bool {
		cnt++
		meta := value.(SQLMeta)
		sqlMeta := &tipb.SQLMeta{
			SqlDigest:     []byte(key.(string)),
			NormalizedSql: meta.normalizedSQL,
			IsInternalSql: meta.isInternal,
		}
		if err = stream.Send(sqlMeta); err != nil {
			return false
		}
		return true
	})
	// stream.Send return error
	if err != nil {
		return err
	}
	topSQLReportSQLCountHistogram.Observe(float64(cnt))
	_, err = stream.CloseAndRecv()
	if err != nil {
		reportSQLDurationFailedHistogram.Observe(time.Since(start).Seconds())
		return err
	}
	reportSQLDurationSuccHistogram.Observe(time.Since(start).Seconds())
	return nil
}

// sendBatchPlanMeta sends a batch of SQL metas by stream.
func (ds *SingleTargetDataSink) sendBatchPlanMeta(ctx context.Context, planMap *sync.Map) error {
	start := time.Now()
	client := tipb.NewTopSQLAgentClient(ds.conn)
	stream, err := client.ReportPlanMeta(ctx)
	if err != nil {
		return err
	}
	cnt := 0
	planMap.Range(func(key, value interface{}) bool {
		planDecoded, errDecode := ds.decodePlan(value.(string))
		if errDecode != nil {
			logutil.BgLogger().Warn("[top-sql] decode plan failed", zap.Error(errDecode))
			return true
		}
		cnt++
		planMeta := &tipb.PlanMeta{
			PlanDigest:     []byte(key.(string)),
			NormalizedPlan: planDecoded,
		}
		if err = stream.Send(planMeta); err != nil {
			return false
		}
		return true
	})
	// stream.Send return error
	if err != nil {
		return err
	}
	topSQLReportPlanCountHistogram.Observe(float64(cnt))
	_, err = stream.CloseAndRecv()
	if err != nil {
		reportPlanDurationFailedHistogram.Observe(time.Since(start).Seconds())
		return err
	}
	reportPlanDurationSuccHistogram.Observe(time.Since(start).Seconds())
	return err
}

// tryEstablishConnection establishes the gRPC connection if connection is not established.
func (ds *SingleTargetDataSink) tryEstablishConnection(ctx context.Context, targetRPCAddr string) (err error) {
	if ds.curRPCAddr == targetRPCAddr && ds.conn != nil {
		// Address is not changed, skip.
		return nil
	}

	if ds.conn != nil {
		err := ds.conn.Close()
		logutil.BgLogger().Warn("[top-sql] grpc dataSink close connection failed", zap.Error(err))
	}

	ds.conn, err = ds.dial(ctx, targetRPCAddr)
	if err != nil {
		return err
	}
	ds.curRPCAddr = targetRPCAddr
	return nil
}

func (ds *SingleTargetDataSink) dial(ctx context.Context, targetRPCAddr string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	return grpc.DialContext(
		dialCtx,
		targetRPCAddr,
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond, // Default was 1s.
				Multiplier: 1.6,                    // Default
				Jitter:     0.2,                    // Default
				MaxDelay:   3 * time.Second,        // Default was 120s.
			},
		}),
	)
}

type sendTask struct {
	data     ReportData
	deadline time.Time
}

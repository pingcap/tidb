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
	curRPCAddr string
	conn       *grpc.ClientConn
	sendTaskCh chan sendTask
}

type sendTask struct {
	data     ReportData
	deadline time.Time
}

// NewSingleTargetDataSink returns a new SingleTargetDataSink
//
func NewSingleTargetDataSink() *SingleTargetDataSink {
	dataSink := &SingleTargetDataSink{
		sendTaskCh: make(chan sendTask),
	}

	go util.WithRecovery(dataSink.run, nil)
	return dataSink
}

func (r *SingleTargetDataSink) run() {
	for task := range r.sendTaskCh {
		targetRPCAddr := config.GetGlobalConfig().TopSQL.ReceiverAddress
		if targetRPCAddr == "" {
			continue
		}

		ctx, cancel := context.WithDeadline(context.Background(), task.deadline)
		start := time.Now()
		err := r.doSend(ctx, targetRPCAddr, task.data)
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

// Send implements the DataSink interface.
func (r *SingleTargetDataSink) Send(data ReportData, deadline time.Time) {
	select {
	case r.sendTaskCh <- sendTask{data: data, deadline: deadline}:
		// sent successfully
	default:
		ignoreReportChannelFullCounter.Inc()
		logutil.BgLogger().Warn("[top-sql] report channel is full")
	}
}

// Currently the doSend will establish a new connection every time, which is suitable for a per-minute sending period
func (r *SingleTargetDataSink) doSend(ctx context.Context, addr string, data ReportData) (err error) {
	err = r.tryEstablishConnection(ctx, addr)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	wg.Add(3)

	go func() {
		defer wg.Done()
		errCh <- r.sendBatchSQLMeta(ctx, data.SQLMetas)
	}()
	go func() {
		defer wg.Done()
		errCh <- r.sendBatchPlanMeta(ctx, data.PlanMetas)
	}()
	go func() {
		defer wg.Done()
		errCh <- r.sendBatchCPUTimeRecord(ctx, data.CPUTimeRecords)
	}()
	wg.Wait()
	close(errCh)
	for err = range errCh {
		if err != nil {
			return
		}
	}

	return
}

// IsPaused implements DataSink interface.
func (r *SingleTargetDataSink) IsPaused() bool {
	return len(config.GetGlobalConfig().TopSQL.ReceiverAddress) == 0
}

// IsDown implements DataSink interface.
func (r *SingleTargetDataSink) IsDown() bool {
	return false
}

// Close uses to close grpc connection.
func (r *SingleTargetDataSink) Close() {
	close(r.sendTaskCh)
	if r.conn == nil {
		return
	}
	err := r.conn.Close()
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] single target data sink failed to close connection", zap.Error(err))
	}
	r.conn = nil
}

// sendBatchCPUTimeRecord sends a batch of TopSQL records by stream.
func (r *SingleTargetDataSink) sendBatchCPUTimeRecord(ctx context.Context, records []*tipb.CPUTimeRecord) (err error) {
	if len(records) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		topSQLReportRecordCounterHistogram.Observe(float64(sentCount))
		if err != nil {
			reportRecordDurationFailedHistogram.Observe(time.Since(start).Seconds())
		}
		reportRecordDurationSuccHistogram.Observe(time.Since(start).Seconds())
	}()

	client := tipb.NewTopSQLAgentClient(r.conn)
	stream, err := client.ReportCPUTimeRecords(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err = stream.Send(record); err != nil {
			return
		}
		sentCount += 1
	}

	// See https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream for how to avoid leaking the stream
	_, err = stream.CloseAndRecv()
	return
}

// sendBatchSQLMeta sends a batch of SQL metas by stream.
func (r *SingleTargetDataSink) sendBatchSQLMeta(ctx context.Context, sqlMetas []*tipb.SQLMeta) (err error) {
	if len(sqlMetas) == 0 {
		return
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		topSQLReportSQLCountHistogram.Observe(float64(sentCount))

		if err != nil {
			reportSQLDurationFailedHistogram.Observe(time.Since(start).Seconds())
		}
		reportSQLDurationSuccHistogram.Observe(time.Since(start).Seconds())
	}()

	client := tipb.NewTopSQLAgentClient(r.conn)
	stream, err := client.ReportSQLMeta(ctx)
	if err != nil {
		return err
	}

	for _, meta := range sqlMetas {
		if err = stream.Send(meta); err != nil {
			return
		}
		sentCount += 1
	}

	// See https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream for how to avoid leaking the stream
	_, err = stream.CloseAndRecv()
	return
}

// sendBatchPlanMeta sends a batch of SQL metas by stream.
func (r *SingleTargetDataSink) sendBatchPlanMeta(ctx context.Context, planMetas []*tipb.PlanMeta) (err error) {
	if len(planMetas) == 0 {
		return nil
	}

	start := time.Now()
	sentCount := 0
	defer func() {
		topSQLReportPlanCountHistogram.Observe(float64(sentCount))
		if err != nil {
			reportPlanDurationFailedHistogram.Observe(time.Since(start).Seconds())
		}
		reportPlanDurationSuccHistogram.Observe(time.Since(start).Seconds())
	}()

	client := tipb.NewTopSQLAgentClient(r.conn)
	stream, err := client.ReportPlanMeta(ctx)
	if err != nil {
		return err
	}

	for _, meta := range planMetas {
		if err = stream.Send(meta); err != nil {
			return err
		}
		sentCount += 1
	}

	// See https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream for how to avoid leaking the stream
	_, err = stream.CloseAndRecv()
	return
}

// tryEstablishConnection establishes the gRPC connection if connection is not established.
func (r *SingleTargetDataSink) tryEstablishConnection(ctx context.Context, targetRPCAddr string) (err error) {
	if r.curRPCAddr == targetRPCAddr && r.conn != nil {
		// Address is not changed, skip.
		return nil
	}

	if r.conn != nil {
		err := r.conn.Close()
		logutil.BgLogger().Warn("[top-sql] single target data sink failed to close connection", zap.Error(err))
	}

	r.conn, err = r.dial(ctx, targetRPCAddr)
	if err != nil {
		return err
	}
	r.curRPCAddr = targetRPCAddr
	return nil
}

func (r *SingleTargetDataSink) dial(ctx context.Context, targetRPCAddr string) (*grpc.ClientConn, error) {
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

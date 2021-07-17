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
// See the License for the specific language governing permissions and
// limitations under the License.

package reporter

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// ReportClient send data to the target server.
type ReportClient interface {
	Send(ctx context.Context, addr string, data reportData) error
	Close()
}

// GRPCReportClient reports data to grpc servers.
type GRPCReportClient struct {
	curRPCAddr string
	conn       *grpc.ClientConn
	// calling decodePlan this can take a while, so should not block critical paths
	decodePlan planBinaryDecodeFunc
}

// NewGRPCReportClient returns a new GRPCReportClient
func NewGRPCReportClient(decodePlan planBinaryDecodeFunc) *GRPCReportClient {
	return &GRPCReportClient{
		decodePlan: decodePlan,
	}
}

var _ ReportClient = &GRPCReportClient{}

// Send implements the ReportClient interface.
// Currently the implementation will establish a new connection every time, which is suitable for a per-minute sending period
func (r *GRPCReportClient) Send(ctx context.Context, targetRPCAddr string, data reportData) error {
	if targetRPCAddr == "" {
		return nil
	}
	err := r.tryEstablishConnection(ctx, targetRPCAddr)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	wg.Add(3)

	go func() {
		defer wg.Done()
		errCh <- r.sendBatchSQLMeta(ctx, data.normalizedSQLMap)
	}()
	go func() {
		defer wg.Done()
		errCh <- r.sendBatchPlanMeta(ctx, data.normalizedPlanMap)
	}()
	go func() {
		defer wg.Done()
		errCh <- r.sendBatchCPUTimeRecord(ctx, data.collectedData)
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

// Close uses to close grpc connection.
func (r *GRPCReportClient) Close() {
	if r.conn == nil {
		return
	}
	err := r.conn.Close()
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] grpc client close connection failed", zap.Error(err))
	}
	r.conn = nil
}

// sendBatchCPUTimeRecord sends a batch of TopSQL records by stream.
func (r *GRPCReportClient) sendBatchCPUTimeRecord(ctx context.Context, records []*dataPoints) error {
	if len(records) == 0 {
		return nil
	}
	start := time.Now()
	client := tipb.NewTopSQLAgentClient(r.conn)
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
func (r *GRPCReportClient) sendBatchSQLMeta(ctx context.Context, sqlMap *sync.Map) error {
	start := time.Now()
	client := tipb.NewTopSQLAgentClient(r.conn)
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
func (r *GRPCReportClient) sendBatchPlanMeta(ctx context.Context, planMap *sync.Map) error {
	start := time.Now()
	client := tipb.NewTopSQLAgentClient(r.conn)
	stream, err := client.ReportPlanMeta(ctx)
	if err != nil {
		return err
	}
	cnt := 0
	planMap.Range(func(key, value interface{}) bool {
		planDecoded, errDecode := r.decodePlan(value.(string))
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
func (r *GRPCReportClient) tryEstablishConnection(ctx context.Context, targetRPCAddr string) (err error) {
	if r.curRPCAddr == targetRPCAddr && r.conn != nil {
		// Address is not changed, skip.
		return nil
	}

	if r.conn != nil {
		err := r.conn.Close()
		logutil.BgLogger().Warn("[top-sql] grpc client close connection failed", zap.Error(err))
	}

	r.conn, err = r.dial(ctx, targetRPCAddr)
	if err != nil {
		return err
	}
	r.curRPCAddr = targetRPCAddr
	return nil
}

func (r *GRPCReportClient) dial(ctx context.Context, targetRPCAddr string) (*grpc.ClientConn, error) {
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

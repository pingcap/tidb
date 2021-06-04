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
	Send(ctx context.Context, addr string, sqlMetas []*tipb.SQLMeta, planMetas []*tipb.PlanMeta, records []*tipb.CPUTimeRecord) error
	Close()
}

// GRPCReportClient reports data to grpc servers.
type GRPCReportClient struct {
	curRPCAddr string
	conn       *grpc.ClientConn
}

// NewGRPCReportClient returns a new GRPCReportClient
func NewGRPCReportClient() *GRPCReportClient {
	return &GRPCReportClient{}
}

// Send implements the ReportClient interface.
func (r *GRPCReportClient) Send(
	ctx context.Context, targetRPCAddr string,
	sqlMetas []*tipb.SQLMeta, planMetas []*tipb.PlanMeta, records []*tipb.CPUTimeRecord) error {
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
		errCh <- r.sendBatchSQLMeta(ctx, sqlMetas)
	}()
	go func() {
		defer wg.Done()
		errCh <- r.sendBatchPlanMeta(ctx, planMetas)
	}()
	go func() {
		defer wg.Done()
		errCh <- r.sendBatchCPUTimeRecord(ctx, records)
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

func (r *GRPCReportClient) Close() {
	if r.conn == nil {
		return
	}
	err := r.conn.Close()
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] grpc client close connection failed", zap.Error(err))
	}
}

// sendBatchCPUTimeRecord sends a batch of TopSQL records by stream.
func (r *GRPCReportClient) sendBatchCPUTimeRecord(ctx context.Context, records []*tipb.CPUTimeRecord) error {
	if len(records) == 0 {
		return nil
	}
	client := tipb.NewTopSQLAgentClient(r.conn)
	stream, err := client.ReportCPUTimeRecords(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := stream.Send(record); err != nil {
			break
		}
	}
	// See https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream for how to avoid leaking the stream
	_, err = stream.CloseAndRecv()
	return err
}

// sendBatchSQLMeta sends a batch of SQL metas by stream.
func (r *GRPCReportClient) sendBatchSQLMeta(ctx context.Context, metas []*tipb.SQLMeta) error {
	if len(metas) == 0 {
		return nil
	}
	client := tipb.NewTopSQLAgentClient(r.conn)
	stream, err := client.ReportSQLMeta(ctx)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		if err := stream.Send(meta); err != nil {
			break
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// sendBatchSQLMeta sends a batch of SQL metas by stream.
func (r *GRPCReportClient) sendBatchPlanMeta(ctx context.Context, metas []*tipb.PlanMeta) error {
	if len(metas) == 0 {
		return nil
	}
	client := tipb.NewTopSQLAgentClient(r.conn)
	stream, err := client.ReportPlanMeta(ctx)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		if err := stream.Send(meta); err != nil {
			break
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// tryEstablishConnection establishes the gRPC connection if connection is not established.
func (r *GRPCReportClient) tryEstablishConnection(ctx context.Context, targetRPCAddr string) (err error) {
	if r.curRPCAddr == targetRPCAddr && r.conn != nil {
		// Address is not changed, skip.
		return nil
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

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
}

// ReportGRPCClient reports data to grpc servers.
type ReportGRPCClient struct {
	addr   string
	conn   *grpc.ClientConn
	client tipb.TopSQLAgentClient
}

// NewReportGRPCClient returns a new ReportGRPCClient
func NewReportGRPCClient() *ReportGRPCClient {
	return &ReportGRPCClient{}
}

// Send implements the ReportClient interface.
func (r *ReportGRPCClient) Send(ctx context.Context, addr string, sqlMetas []*tipb.SQLMeta, planMetas []*tipb.PlanMeta, records []*tipb.CPUTimeRecord) error {
	if addr == "" {
		return nil
	}
	err := r.initialize(ctx, addr)
	if err != nil {
		return err
	}
	if err := r.sendBatchSQLMeta(ctx, sqlMetas); err != nil {
		return r.resetClientWhenSendError(err)
	}
	if err := r.sendBatchPlanMeta(ctx, planMetas); err != nil {
		return r.resetClientWhenSendError(err)
	}
	if err := r.sendBatchCPUTimeRecord(ctx, records); err != nil {
		return r.resetClientWhenSendError(err)
	}
	return nil
}

// sendBatchCPUTimeRecord sends a batch of TopSQL records by stream.
func (r *ReportGRPCClient) sendBatchCPUTimeRecord(ctx context.Context, records []*tipb.CPUTimeRecord) error {
	stream, err := r.client.ReportCPUTimeRecords(ctx)
	if err != nil {
		return r.resetClientWhenSendError(err)
	}
	for _, record := range records {
		if err := stream.Send(record); err != nil {
			return err
		}
	}
	// response is Empty, drop it for now
	_, err = stream.CloseAndRecv()
	return err
}

// sendBatchSQLMeta sends a batch of SQL metas by stream.
func (r *ReportGRPCClient) sendBatchSQLMeta(ctx context.Context, metas []*tipb.SQLMeta) error {
	stream, err := r.client.ReportSQLMeta(ctx)
	if err != nil {
		return r.resetClientWhenSendError(err)
	}
	for _, meta := range metas {
		if err := stream.Send(meta); err != nil {
			return err
		}
	}
	// response is Empty, drop it for now
	_, err = stream.CloseAndRecv()
	return err
}

// sendBatchSQLMeta sends a batch of SQL metas by stream.
func (r *ReportGRPCClient) sendBatchPlanMeta(ctx context.Context, metas []*tipb.PlanMeta) error {
	stream, err := r.client.ReportPlanMeta(ctx)
	if err != nil {
		return r.resetClientWhenSendError(err)
	}
	for _, meta := range metas {
		if err := stream.Send(meta); err != nil {
			return err
		}
	}
	// response is Empty, drop it for now
	_, err = stream.CloseAndRecv()
	return err
}

func (r *ReportGRPCClient) initialize(ctx context.Context, addr string) (err error) {
	if r.addr == addr {
		return nil
	}
	r.conn, err = r.newAgentConn(ctx, addr)
	if err != nil {
		return err
	}
	r.addr = addr
	r.client = tipb.NewTopSQLAgentClient(r.conn)
	return nil
}

func (r *ReportGRPCClient) newAgentConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	return grpc.DialContext(
		dialCtx,
		addr,
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

func (r *ReportGRPCClient) resetClientWhenSendError(err error) error {
	if err == nil {
		return nil
	}
	r.addr = ""
	if r.conn != nil {
		err1 := r.conn.Close()
		if err1 != nil {
			logutil.BgLogger().Warn("[top-sql] close grpc conn failed", zap.Error(err1))
		}
	}
	return err
}

// Copyright 2026 PingCAP, Inc.
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

// Package client is the gRPC client to the external workload controller. Every
// request carries the keyspace identity in externalworkloadpb.RequestHeader so
// the controller can route to the right pool.
package client

import (
	"context"
	"crypto/tls"
	"net/url"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/externalworkloadpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrControllerPaused is returned when the controller responds with
// ErrorType_PAUSED to a request that addresses a paused handler. Callers
// may match with errors.Is.
var ErrControllerPaused = errors.New("external workload controller: handler paused")

// Option configures the gRPC client.
type Option struct {
	KeyspaceID     uint32
	KeyspaceName   string
	TiDBPool       string
	ControllerAddr string
	TLSConfig      *tls.Config
}

// Client is the gRPC client to the external workload controller.
type Client interface {
	Close() error
	Ping(ctx context.Context) error

	GCClient
	GCV2Client
	BgTaskClient
	RemoteQueryClient
	TTLClient
	AutoAnalyzeClient
}

// GCClient covers the GC v1 (delete-range) RPCs.
type GCClient interface {
	RegisterGC(ctx context.Context, deletionTs uint64) error
	RecycleGC(ctx context.Context, safePoint uint64) error
}

// GCV2Client covers the keyspace-level GC RPCs.
type GCV2Client interface {
	RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error
	RecycleGCV2(ctx context.Context, safePoint uint64) error
	UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error
}

// BgTaskClient covers the distributed background task RPCs.
type BgTaskClient interface {
	GetBgTaskConfig(ctx context.Context, workerType string) (workerCount int, autoScaleEnabled bool, err error)
	RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error
	RecycleBgTask(ctx context.Context, gTaskID, subTaskID int64) error
	UpdateBgTaskExecID(ctx context.Context, gTaskID int64, assignments []*pb.SubtaskExecIDAssignment) error
}

// RemoteQueryClient covers the remote-query RPC.
type RemoteQueryClient interface {
	RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error
}

// TTLClient covers the TTL RPCs.
type TTLClient interface {
	RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error
	DeleteTTLTableInfo(ctx context.Context, tableID int64) error
	RecycleTTLTask(ctx context.Context, completedJobCreateTime uint64) error
	UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error
}

// AutoAnalyzeClient covers the auto-analyze RPCs.
type AutoAnalyzeClient interface {
	RegisterAutoAnalyze(ctx context.Context, taskID uint64) error
	RecycleAutoAnalyze(ctx context.Context, taskID uint64) error
}

// New dials the controller at opt.ControllerAddr and returns a Client. Callers
// must Close it when finished.
func New(ctx context.Context, opt *Option, extraDialOpts ...grpc.DialOption) (Client, error) {
	if opt == nil {
		return nil, errors.New("external workload client: nil option")
	}
	host, err := parseHost(opt.ControllerAddr)
	if err != nil {
		return nil, err
	}
	dialOpts := make([]grpc.DialOption, 0, len(extraDialOpts)+1)
	if opt.TLSConfig != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(opt.TLSConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, extraDialOpts...)

	conn, err := grpc.DialContext(ctx, host, dialOpts...) //nolint:staticcheck // grpc.NewClient lacks ctx-aware dial.
	if err != nil {
		return nil, errors.Annotate(err, "dial external workload controller")
	}
	return &grpcClient{
		opt:  *opt,
		conn: conn,
		stub: pb.NewExternalWorkloadControllerClient(conn),
	}, nil
}

func parseHost(addr string) (string, error) {
	if addr == "" {
		return "", errors.New("external workload client: empty controller address")
	}
	u, err := url.Parse(addr)
	if err != nil {
		return "", errors.Annotatef(err, "parse controller address %q", addr)
	}
	if u.Host != "" {
		return u.Host, nil
	}
	// Accept bare host:port (no scheme).
	return addr, nil
}

type grpcClient struct {
	opt  Option
	conn *grpc.ClientConn
	stub pb.ExternalWorkloadControllerClient
}

func (c *grpcClient) Close() error { return c.conn.Close() }

func (c *grpcClient) header() *pb.RequestHeader {
	return &pb.RequestHeader{
		KeyspaceId:   c.opt.KeyspaceID,
		KeyspaceName: c.opt.KeyspaceName,
		TidbPool:     c.opt.TiDBPool,
	}
}

func (c *grpcClient) Ping(ctx context.Context) error {
	resp, err := c.stub.Ping(ctx, &pb.PingRequest{})
	return mapResponse("Ping", resp, err)
}

func (c *grpcClient) RegisterGC(ctx context.Context, deletionTs uint64) error {
	resp, err := c.stub.RegisterGC(ctx, &pb.RegisterGCRequest{
		Header:     c.header(),
		DeletionTs: deletionTs,
	})
	return mapResponse("RegisterGC", resp, err)
}

func (c *grpcClient) RecycleGC(ctx context.Context, safePoint uint64) error {
	resp, err := c.stub.RecycleGC(ctx, &pb.RecycleGCRequest{
		Header:    c.header(),
		SafePoint: safePoint,
	})
	return mapResponse("RecycleGC", resp, err)
}

func (c *grpcClient) RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error {
	resp, err := c.stub.RegisterGCV2(ctx, &pb.RegisterGCV2Request{
		Header:     c.header(),
		SafePoint:  safePoint,
		GcLifeTime: gcLifeTime,
	})
	return mapResponse("RegisterGCV2", resp, err)
}

func (c *grpcClient) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	resp, err := c.stub.RecycleGCV2(ctx, &pb.RecycleGCV2Request{
		Header:    c.header(),
		SafePoint: safePoint,
	})
	return mapResponse("RecycleGCV2", resp, err)
}

func (c *grpcClient) UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error {
	resp, err := c.stub.UpdateGCLifeTime(ctx, &pb.UpdateGCLifeTimeRequest{
		Header:     c.header(),
		GcLifeTime: gcLifeTime,
	})
	return mapResponse("UpdateGCLifeTime", resp, err)
}

func (c *grpcClient) GetBgTaskConfig(ctx context.Context, workerType string) (int, bool, error) {
	resp, err := c.stub.GetBgTaskConfig(ctx, &pb.GetBgTaskConfigRequest{
		Header:     c.header(),
		WorkerType: workerType,
	})
	if err != nil {
		return 0, false, errors.Annotate(err, "external workload rpc GetBgTaskConfig")
	}
	if e := resp.GetError(); e != nil && e.GetType() != pb.ErrorType_OK {
		if e.GetType() == pb.ErrorType_PAUSED {
			return 0, false, ErrControllerPaused
		}
		return 0, false, errors.Errorf("external workload rpc GetBgTaskConfig: %s", e.GetMessage())
	}
	return int(resp.GetWorkerCount()), resp.GetAutoScaleEnabled(), nil
}

func (c *grpcClient) RegisterBgTask(ctx context.Context, taskType, taskKey string, gTaskID, subTaskID int64, execID string) error {
	resp, err := c.stub.RegisterBgTask(ctx, &pb.RegisterBgTaskRequest{
		Header:       c.header(),
		TaskType:     taskType,
		TaskKey:      taskKey,
		GlobalTaskId: gTaskID,
		SubTaskId:    subTaskID,
		ExecId:       execID,
	})
	return mapResponse("RegisterBgTask", resp, err)
}

func (c *grpcClient) RecycleBgTask(ctx context.Context, gTaskID, subTaskID int64) error {
	resp, err := c.stub.RecycleBgTask(ctx, &pb.RecycleBgTaskRequest{
		Header:       c.header(),
		GlobalTaskId: gTaskID,
		SubTaskId:    subTaskID,
	})
	return mapResponse("RecycleBgTask", resp, err)
}

func (c *grpcClient) UpdateBgTaskExecID(ctx context.Context, gTaskID int64, assignments []*pb.SubtaskExecIDAssignment) error {
	resp, err := c.stub.UpdateBgTaskExecID(ctx, &pb.UpdateBgTaskExecIDRequest{
		Header:       c.header(),
		GlobalTaskId: gTaskID,
		Assignments:  assignments,
	})
	return mapResponse("UpdateBgTaskExecID", resp, err)
}

func (c *grpcClient) RegisterRemoteQuery(ctx context.Context, queryID, queryAddr string) error {
	resp, err := c.stub.RegisterRemoteQuery(ctx, &pb.RegisterRemoteQueryRequest{
		Header:       c.header(),
		QueryId:      queryID,
		QueryAddress: queryAddr,
	})
	return mapResponse("RegisterRemoteQuery", resp, err)
}

func (c *grpcClient) RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error {
	resp, err := c.stub.RegisterTTLTask(ctx, &pb.RegisterTTLTaskRequest{
		Header:       c.header(),
		TableId:      tableID,
		TtlJobEnable: ttlJobEnable,
	})
	return mapResponse("RegisterTTLTask", resp, err)
}

func (c *grpcClient) DeleteTTLTableInfo(ctx context.Context, tableID int64) error {
	resp, err := c.stub.DeleteTTLTableInfo(ctx, &pb.DeleteTTLTableInfoRequest{
		Header:  c.header(),
		TableId: tableID,
	})
	return mapResponse("DeleteTTLTableInfo", resp, err)
}

func (c *grpcClient) RecycleTTLTask(ctx context.Context, completedJobCreateTime uint64) error {
	resp, err := c.stub.RecycleTTLTask(ctx, &pb.RecycleTTLTaskRequest{
		Header:                 c.header(),
		CompletedJobCreateTime: completedJobCreateTime,
	})
	return mapResponse("RecycleTTLTask", resp, err)
}

func (c *grpcClient) UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error {
	resp, err := c.stub.UpdateTTLJobEnable(ctx, &pb.UpdateTTLJobEnableRequest{
		Header:       c.header(),
		TtlJobEnable: ttlJobEnable,
	})
	return mapResponse("UpdateTTLJobEnable", resp, err)
}

func (c *grpcClient) RegisterAutoAnalyze(ctx context.Context, taskID uint64) error {
	resp, err := c.stub.RegisterAutoAnalyze(ctx, &pb.RegisterAutoAnalyzeRequest{
		Header: c.header(),
		TaskId: taskID,
	})
	return mapResponse("RegisterAutoAnalyze", resp, err)
}

func (c *grpcClient) RecycleAutoAnalyze(ctx context.Context, taskID uint64) error {
	resp, err := c.stub.RecycleAutoAnalyze(ctx, &pb.RecycleAutoAnalyzeRequest{
		Header: c.header(),
		TaskId: taskID,
	})
	return mapResponse("RecycleAutoAnalyze", resp, err)
}

// respWithError is implemented by every controller response that surfaces a
// top-level *pb.Error (i.e. anything returning pb.Response and the
// GetBgTaskConfigResponse).
type respWithError interface {
	GetError() *pb.Error
}

// mapResponse converts a gRPC (response, err) pair into a single error.
//
// It is used by every Register / Recycle / Update RPC that returns
// pb.Response. GetBgTaskConfig has a non-standard response and is mapped
// inline at the call site.
func mapResponse(method string, resp respWithError, err error) error {
	if err != nil {
		return errors.Annotatef(err, "external workload rpc %s", method)
	}
	e := resp.GetError()
	if e == nil || e.GetType() == pb.ErrorType_OK {
		return nil
	}
	if e.GetType() == pb.ErrorType_PAUSED {
		return ErrControllerPaused
	}
	return errors.Errorf("external workload rpc %s: %s", method, e.GetMessage())
}

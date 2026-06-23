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

package extworkload

import (
	"context"
	"errors"
	"math"
	"net"
	"testing"

	pb "github.com/pingcap/kvproto/pkg/externalworkloadpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/extworkload/client"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type managerStubServer struct {
	pb.UnimplementedExternalWorkloadControllerServer
	pingErr *pb.Error
}

func (s *managerStubServer) Ping(_ context.Context, _ *pb.PingRequest) (*pb.Response, error) {
	return &pb.Response{Error: s.pingErr}, nil
}

func startManagerStub(t *testing.T, stub *managerStubServer) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	pb.RegisterExternalWorkloadControllerServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()

	return "http://" + ln.Addr().String(), func() {
		srv.GracefulStop()
		_ = ln.Close()
	}
}

func TestNewManagerLifecycle(t *testing.T) {
	meta := &keyspacepb.KeyspaceMeta{Id: 42, Name: "starter-ks"}
	cfg := config.ExternalWorkload{
		Enable:         true,
		Role:           config.RoleMaster,
		TidbPool:       "vip-tidb-pool",
		ControllerAddr: "",
	}

	mgr, err := NewManager(context.Background(), nil, config.ExternalWorkload{})
	require.NoError(t, err)
	require.Nil(t, mgr)

	_, err = NewManager(context.Background(), nil, cfg)
	require.ErrorContains(t, err, "non-nil keyspace meta")

	addr, cleanup := startManagerStub(t, &managerStubServer{})
	defer cleanup()
	cfg.ControllerAddr = addr
	mgr, err = NewManager(context.Background(), meta, cfg)
	require.NoError(t, err)
	require.NotNil(t, mgr)
	require.Equal(t, config.RoleMaster, mgr.Role())
	require.Same(t, meta, mgr.Meta())
	require.NoError(t, mgr.Close())
}

func TestNewManagerPingFailure(t *testing.T) {
	addr, cleanup := startManagerStub(t, &managerStubServer{
		pingErr: &pb.Error{Type: pb.ErrorType_UNKNOWN, Message: "boom"},
	})
	defer cleanup()

	mgr, err := NewManager(context.Background(), &keyspacepb.KeyspaceMeta{Id: 1, Name: "ks"}, config.ExternalWorkload{
		Enable:         true,
		Role:           config.RoleMaster,
		TidbPool:       "super-vip-tidb-pool",
		ControllerAddr: addr,
	})
	require.ErrorContains(t, err, "ping external workload controller")
	require.Nil(t, mgr)
}

type fakeClient struct {
	call                   string
	safePoint              uint64
	gcLifeTime             int64
	tableID                int64
	ttlJobEnable           bool
	completedJobCreateTime uint64
	taskID                 uint64
	deadlineSet            bool
	labels                 metricLabels
	hasLabels              bool
	err                    error
}

func (f *fakeClient) Close() error               { return nil }
func (f *fakeClient) Ping(context.Context) error { return nil }

func (f *fakeClient) RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error {
	f.record(ctx, "RegisterGCV2")
	f.safePoint = safePoint
	f.gcLifeTime = gcLifeTime
	return f.err
}

func (f *fakeClient) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	f.record(ctx, "RecycleGCV2")
	f.safePoint = safePoint
	return f.err
}

func (f *fakeClient) UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error {
	f.record(ctx, "UpdateGCLifeTime")
	f.gcLifeTime = gcLifeTime
	return f.err
}

func (f *fakeClient) RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error {
	f.record(ctx, "RegisterTTLTask")
	f.tableID = tableID
	f.ttlJobEnable = ttlJobEnable
	return f.err
}

func (f *fakeClient) DeleteTTLTableInfo(ctx context.Context, tableID int64) error {
	f.record(ctx, "DeleteTTLTableInfo")
	f.tableID = tableID
	return f.err
}

func (f *fakeClient) RecycleTTLTask(ctx context.Context, completedJobCreateTime uint64) error {
	f.record(ctx, "RecycleTTLTask")
	f.completedJobCreateTime = completedJobCreateTime
	return f.err
}

func (f *fakeClient) UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error {
	f.record(ctx, "UpdateTTLJobEnable")
	f.ttlJobEnable = ttlJobEnable
	return f.err
}

func (f *fakeClient) RegisterAutoAnalyze(ctx context.Context, taskID uint64) error {
	f.record(ctx, "RegisterAutoAnalyze")
	f.taskID = taskID
	return f.err
}

func (f *fakeClient) RecycleAutoAnalyze(ctx context.Context, taskID uint64) error {
	f.record(ctx, "RecycleAutoAnalyze")
	f.taskID = taskID
	return f.err
}

func (f *fakeClient) record(ctx context.Context, call string) {
	f.call = call
	_, f.deadlineSet = ctx.Deadline()
	f.labels, f.hasLabels = ctx.Value(metricLabelsKey{}).(metricLabels)
}

func TestManagerMethodsSetDeadlineAndMetrics(t *testing.T) {
	cases := []struct {
		name  string
		call  func(*manager) error
		check func(*fakeClient)
	}{
		{
			name: "InitializeGCV2",
			call: func(m *manager) error { return m.InitializeGCV2(context.Background()) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RegisterGCV2", cli.call)
				require.Equal(t, uint64(0), cli.safePoint)
				require.Equal(t, int64(defGCLifeTimeSec), cli.gcLifeTime)
				requireLabels(t, cli, string(config.RoleGCV2Worker), metrics.WorkerActionInit)
			},
		},
		{
			name: "AbortGCV2",
			call: func(m *manager) error { return m.AbortGCV2(context.Background()) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RecycleGCV2", cli.call)
				require.Equal(t, uint64(math.MaxUint64), cli.safePoint)
				requireLabels(t, cli, string(config.RoleGCV2Worker), metrics.WorkerActionAbort)
			},
		},
		{
			name: "RegisterGCV2",
			call: func(m *manager) error { return m.RegisterGCV2(context.Background(), 10, 600) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RegisterGCV2", cli.call)
				require.Equal(t, uint64(10), cli.safePoint)
				require.Equal(t, int64(600), cli.gcLifeTime)
				requireLabels(t, cli, string(config.RoleGCV2Worker), metrics.WorkerActionRegister)
			},
		},
		{
			name: "RecycleGCV2",
			call: func(m *manager) error { return m.RecycleGCV2(context.Background(), 20) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RecycleGCV2", cli.call)
				require.Equal(t, uint64(20), cli.safePoint)
				requireLabels(t, cli, string(config.RoleGCV2Worker), metrics.WorkerActionRecycle)
			},
		},
		{
			name: "UpdateGCLifeTime",
			call: func(m *manager) error { return m.UpdateGCLifeTime(context.Background(), 60) },
			check: func(cli *fakeClient) {
				require.Equal(t, "UpdateGCLifeTime", cli.call)
				require.Equal(t, int64(60), cli.gcLifeTime)
				require.False(t, cli.hasLabels)
			},
		},
		{
			name: "RegisterTTLTask",
			call: func(m *manager) error { return m.RegisterTTLTask(context.Background(), 11, true) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RegisterTTLTask", cli.call)
				require.Equal(t, int64(11), cli.tableID)
				require.True(t, cli.ttlJobEnable)
				requireLabels(t, cli, string(config.RoleTTLTaskWorker), metrics.WorkerActionRegister)
			},
		},
		{
			name: "DeleteTTLTableInfo",
			call: func(m *manager) error { return m.DeleteTTLTableInfo(context.Background(), 12) },
			check: func(cli *fakeClient) {
				require.Equal(t, "DeleteTTLTableInfo", cli.call)
				require.Equal(t, int64(12), cli.tableID)
				require.False(t, cli.hasLabels)
			},
		},
		{
			name: "RecycleTTLTask",
			call: func(m *manager) error { return m.RecycleTTLTask(context.Background(), 99) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RecycleTTLTask", cli.call)
				require.Equal(t, uint64(99), cli.completedJobCreateTime)
				requireLabels(t, cli, string(config.RoleTTLTaskWorker), metrics.WorkerActionRecycle)
			},
		},
		{
			name: "UpdateTTLJobEnable",
			call: func(m *manager) error { return m.UpdateTTLJobEnable(context.Background(), true) },
			check: func(cli *fakeClient) {
				require.Equal(t, "UpdateTTLJobEnable", cli.call)
				require.True(t, cli.ttlJobEnable)
				require.False(t, cli.hasLabels)
			},
		},
		{
			name: "RegisterAutoAnalyze",
			call: func(m *manager) error { return m.RegisterAutoAnalyze(context.Background(), 7) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RegisterAutoAnalyze", cli.call)
				require.Equal(t, uint64(7), cli.taskID)
				requireLabels(t, cli, string(config.RoleAutoAnalyzeWorker), metrics.WorkerActionRegister)
			},
		},
		{
			name: "RecycleAutoAnalyze",
			call: func(m *manager) error { return m.RecycleAutoAnalyze(context.Background(), 8) },
			check: func(cli *fakeClient) {
				require.Equal(t, "RecycleAutoAnalyze", cli.call)
				require.Equal(t, uint64(8), cli.taskID)
				requireLabels(t, cli, string(config.RoleAutoAnalyzeWorker), metrics.WorkerActionRecycle)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cli := &fakeClient{}
			mgr := &manager{cli: cli}
			require.NoError(t, c.call(mgr))
			require.True(t, cli.deadlineSet)
			c.check(cli)
		})
	}
}

func TestManagerMethodErrorPropagation(t *testing.T) {
	boom := errors.New("boom")
	cli := &fakeClient{err: boom}
	mgr := &manager{cli: cli}
	require.ErrorIs(t, mgr.RegisterTTLTask(context.Background(), 1, true), boom)
}

func requireLabels(t *testing.T, cli *fakeClient, workerType, action string) {
	t.Helper()
	require.True(t, cli.hasLabels)
	require.Equal(t, workerType, cli.labels.workerType)
	require.Equal(t, action, cli.labels.action)
}

var _ client.Client = (*fakeClient)(nil)

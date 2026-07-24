// Copyright 2025 PingCAP, Inc.
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

package autoid

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// mockAutoIDClient implements autoid.AutoIDAllocClient for testing.
type mockAutoIDClient struct {
	autoid.AutoIDAllocClient // embed to satisfy interface without implementing all methods
	allocCallCount           atomic.Int64
	rebaseCallCount          atomic.Int64
	allocErr                 error
	rebaseErr                error
}

func (m *mockAutoIDClient) AllocAutoID(_ context.Context, _ *autoid.AutoIDRequest, _ ...grpc.CallOption) (*autoid.AutoIDResponse, error) {
	m.allocCallCount.Add(1)
	return nil, m.allocErr
}

func (m *mockAutoIDClient) Rebase(_ context.Context, _ *autoid.RebaseRequest, _ ...grpc.CallOption) (*autoid.RebaseResponse, error) {
	m.rebaseCallCount.Add(1)
	return nil, m.rebaseErr
}

type scriptedAutoIDServer struct {
	autoid.UnimplementedAutoIDAllocServer
	allocCallCount  atomic.Int64
	rebaseCallCount atomic.Int64
	alloc           func(int64) (*autoid.AutoIDResponse, error)
	rebase          func(int64) (*autoid.RebaseResponse, error)
}

func (s *scriptedAutoIDServer) AllocAutoID(_ context.Context, _ *autoid.AutoIDRequest) (*autoid.AutoIDResponse, error) {
	call := s.allocCallCount.Add(1)
	return s.alloc(call)
}

func (s *scriptedAutoIDServer) Rebase(_ context.Context, _ *autoid.RebaseRequest) (*autoid.RebaseResponse, error) {
	call := s.rebaseCallCount.Add(1)
	return s.rebase(call)
}

func startScriptedAutoIDServer(t *testing.T, service *scriptedAutoIDServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	autoid.RegisterAutoIDAllocServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func newAutoIDTestEtcdClient(t *testing.T) *clientv3.Client {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 creates filenames that are invalid on Windows")
	}
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() {
		cluster.Terminate(t)
	})
	return cluster.RandClient()
}

func putAutoIDServiceEndpoint(t *testing.T, cli *clientv3.Client, address string) {
	t.Helper()
	_, err := cli.Put(autoIDTestContext(t), AutoIDLeaderPath+"/candidate", address)
	require.NoError(t, err)
}

func autoIDTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func observeAutoIDLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.InfoLevel)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{
		Core:  core,
		Level: zap.NewAtomicLevelAt(zap.InfoLevel),
	})
	t.Cleanup(restore)
	return logs
}

func newRPCRetryTestAllocator(t *testing.T, etcdCli *clientv3.Client) *singlePointAlloc {
	t.Helper()
	allocator := &singlePointAlloc{
		dbID:           11,
		tblID:          22,
		ClientDiscover: NewClientDiscover(etcdCli),
		keyspaceID:     uint32(tikv.NullspaceID),
		rpcRetryPolicy: autoIDRPCRetryPolicy{
			minErrors:   3,
			minDuration: 0,
		},
	}
	t.Cleanup(func() {
		allocator.mu.RLock()
		grpcConn := allocator.mu.ClientConn
		allocator.mu.RUnlock()
		allocator.ResetConn(nil)
		if grpcConn != nil {
			require.Eventually(t, func() bool {
				return grpcConn.GetState() == connectivity.Shutdown
			}, 2*time.Second, 10*time.Millisecond)
		}
	})
	return allocator
}

func runAutoIDTestOperation(ctx context.Context, operation string, allocator *singlePointAlloc) error {
	if operation == "alloc" {
		_, _, err := allocator.Alloc(ctx, 1, 1, 1)
		return err
	}
	return allocator.rebase(ctx, 100, false)
}

func autoIDOperationCallCount(operation string, service *scriptedAutoIDServer) int64 {
	if operation == "alloc" {
		return service.allocCallCount.Load()
	}
	return service.rebaseCallCount.Load()
}

func successfulAutoIDResponse() (*autoid.AutoIDResponse, error) {
	return &autoid.AutoIDResponse{Min: 100, Max: 101}, nil
}

func successfulRebaseResponse() (*autoid.RebaseResponse, error) {
	return &autoid.RebaseResponse{}, nil
}

// newTestSinglePointAlloc creates a singlePointAlloc with a mock client.
// The mock client is set directly on ClientDiscover so GetClient returns it.
// After resetConn clears the client, GetClient will fail without etcd,
// but that's OK for the canceled-context tests since they return before retrying.
func newTestSinglePointAlloc(mockCli *mockAutoIDClient) *singlePointAlloc {
	cd := &ClientDiscover{}
	cd.mu.AutoIDAllocClient = mockCli
	return &singlePointAlloc{
		dbID:           1,
		tblID:          1,
		isUnsigned:     false,
		ClientDiscover: cd,
		keyspaceID:     0,
	}
}

// TestAllocCanceledRPCReturnsQuickly verifies that when AllocAutoID returns an RPC error
// and the context is already canceled, Alloc returns immediately without retrying.
// This is the core fix for the KILL QUERY taking 20 minutes issue.
func TestAllocCanceledRPCReturnsQuickly(t *testing.T) {
	// Simulate the gRPC error seen in production: rpc error with Canceled code.
	rpcErr := status.Error(codes.Canceled, "rpc error: code = Canceled desc = context canceled")

	mockCli := &mockAutoIDClient{
		allocErr: rpcErr,
	}
	sp := newTestSinglePointAlloc(mockCli)

	// Use an already-canceled context (simulating KILL QUERY).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, _, err := sp.Alloc(ctx, 1, 1, 1)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	// Should return within 1 second (not 20 minutes as in the bug).
	require.Less(t, elapsed, time.Second, "Alloc should return quickly when context is canceled, took %v", elapsed)
	// Should only call the RPC once — no retries, no resetConn.
	require.Equal(t, int64(1), mockCli.allocCallCount.Load(), "Alloc should not retry on canceled context")
}

// TestRebaseCanceledRPCReturnsQuickly verifies that rebase also checks ctx.Err()
// before resetting connection and retrying on RPC errors.
func TestRebaseCanceledRPCReturnsQuickly(t *testing.T) {
	rpcErr := status.Error(codes.Canceled, "rpc error: code = Canceled desc = context canceled")

	mockCli := &mockAutoIDClient{
		rebaseErr: rpcErr,
	}
	sp := newTestSinglePointAlloc(mockCli)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := sp.rebase(ctx, 100, false)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, time.Second, "rebase should return quickly when context is canceled")
	require.Equal(t, int64(1), mockCli.rebaseCallCount.Load(), "rebase should not retry on canceled context")
}

// TestBackoffCtxAware verifies that backoffer.Backoff respects context cancellation.
func TestBackoffCtxAware(t *testing.T) {
	var bo backoffer

	// Without ctx, Backoff should behave as before (blocking sleep).
	start := time.Now()
	err := bo.Backoff()
	require.NoError(t, err)
	require.WithinDuration(t, start.Add(backoffMin), time.Now(), 50*time.Millisecond)

	// With a canceled ctx, Backoff should return immediately.
	bo.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start = time.Now()
	err = bo.Backoff(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 10*time.Millisecond, "Backoff should return immediately on canceled ctx")

	// With a valid ctx that gets canceled during sleep, Backoff should return early.
	bo.Reset()
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	start = time.Now()
	err = bo.Backoff(ctx)
	// Backoff may or may not return an error depending on timing,
	// but it should not block for the full duration (100ms at max).
	require.Less(t, time.Since(start), 50*time.Millisecond, "Backoff should return early when ctx is canceled during sleep")
	_ = err
}

func TestAutoIDRPCRetryPolicy(t *testing.T) {
	t.Run("production default", func(t *testing.T) {
		policy := (&singlePointAlloc{}).effectiveRPCRetryPolicy()
		require.Equal(t, 10, policy.minErrors)
		require.Equal(t, 15*time.Second, policy.minDuration)
	})

	policy := autoIDRPCRetryPolicy{minErrors: 3, minDuration: 2 * time.Second}
	start := time.Unix(100, 0)
	var state autoIDRPCRetryState

	require.False(t, state.observe(start, policy))
	require.False(t, state.observe(start.Add(time.Second), policy))
	require.True(t, state.observe(start.Add(2*time.Second), policy))
	require.Equal(t, 3, state.errorCount)
	require.Equal(t, start, state.firstError)

	t.Run("count and duration use AND semantics", func(t *testing.T) {
		var countOnly autoIDRPCRetryState
		require.False(t, countOnly.observe(start, policy))
		require.False(t, countOnly.observe(start, policy))
		require.False(t, countOnly.observe(start, policy))

		var durationOnly autoIDRPCRetryState
		require.False(t, durationOnly.observe(start, policy))
		require.False(t, durationOnly.observe(start.Add(3*time.Second), policy))
	})
}

func TestAutoIDRPCRetry(t *testing.T) {
	t.Run("reaches the common limit", func(t *testing.T) {
		tests := []struct {
			name      string
			operation string
			rpcErrors []error
		}{
			{
				name:      "alloc repeated original RPC error",
				operation: "alloc",
				rpcErrors: []error{
					status.Error(codes.Unknown, "not leader"),
					status.Error(codes.Unknown, "not leader"),
					status.Error(codes.Unknown, "not leader"),
				},
			},
			{
				name:      "rebase mixed RPC errors",
				operation: "rebase",
				rpcErrors: []error{
					status.Error(codes.Unknown, "not leader"),
					status.Error(codes.Unavailable, "temporary connection failure"),
					status.Error(codes.Internal, "final retry failure at 100%"),
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				logs := observeAutoIDLogs(t)
				etcdCli := newAutoIDTestEtcdClient(t)
				service := &scriptedAutoIDServer{
					alloc:  func(int64) (*autoid.AutoIDResponse, error) { return successfulAutoIDResponse() },
					rebase: func(int64) (*autoid.RebaseResponse, error) { return successfulRebaseResponse() },
				}
				nextError := func(call int64) error {
					return test.rpcErrors[call-1]
				}
				if test.operation == "alloc" {
					service.alloc = func(call int64) (*autoid.AutoIDResponse, error) {
						return nil, nextError(call)
					}
				} else {
					service.rebase = func(call int64) (*autoid.RebaseResponse, error) {
						return nil, nextError(call)
					}
				}

				address := startScriptedAutoIDServer(t, service)
				putAutoIDServiceEndpoint(t, etcdCli, address)
				allocator := newRPCRetryTestAllocator(t, etcdCli)

				err := runAutoIDTestOperation(autoIDTestContext(t), test.operation, allocator)
				require.Error(t, err)
				require.True(t, ErrAutoincReadFailed.Equal(err))
				require.True(t, IsRPCRetryLimitError(err))
				require.Contains(t, err.Error(), "3 RPC errors")
				require.Contains(t, err.Error(), test.rpcErrors[2].Error())
				require.Contains(t, err.Error(), autoIDRPCRetryAction)
				require.Equal(t, int64(3), autoIDOperationCallCount(test.operation, service))

				starts := logs.FilterMessage("autoid request entered RPC retry").All()
				terminals := logs.FilterMessage("autoid request stopped after reaching RPC retry limit").All()
				require.Len(t, starts, 1)
				require.Len(t, terminals, 1)
				require.Equal(t, starts[0].ContextMap()["autoid-request-id"], terminals[0].ContextMap()["autoid-request-id"])
				require.Equal(t, test.operation, terminals[0].ContextMap()["operation"])
				require.Equal(t, int64(3), terminals[0].ContextMap()["rpc-error-count"])
				require.Equal(t, "fast-failed", terminals[0].ContextMap()["outcome"])
				require.Empty(t, logs.FilterMessage("autoid request completed after RPC retry").All())
			})
		}
	})

	t.Run("recovers before the limit", func(t *testing.T) {
		for _, operation := range []string{"alloc", "rebase"} {
			t.Run(operation, func(t *testing.T) {
				logs := observeAutoIDLogs(t)
				etcdCli := newAutoIDTestEtcdClient(t)
				retryErr := status.Error(codes.Unavailable, "temporary connection failure")
				service := &scriptedAutoIDServer{
					alloc: func(call int64) (*autoid.AutoIDResponse, error) {
						if call < 3 {
							return nil, retryErr
						}
						return successfulAutoIDResponse()
					},
					rebase: func(call int64) (*autoid.RebaseResponse, error) {
						if call < 3 {
							return nil, retryErr
						}
						return successfulRebaseResponse()
					},
				}

				address := startScriptedAutoIDServer(t, service)
				putAutoIDServiceEndpoint(t, etcdCli, address)
				allocator := newRPCRetryTestAllocator(t, etcdCli)

				require.NoError(t, runAutoIDTestOperation(autoIDTestContext(t), operation, allocator))
				require.Equal(t, int64(3), autoIDOperationCallCount(operation, service))

				starts := logs.FilterMessage("autoid request entered RPC retry").All()
				completions := logs.FilterMessage("autoid request completed after RPC retry").All()
				require.Len(t, starts, 1)
				require.Len(t, completions, 1)
				require.Equal(t, starts[0].ContextMap()["autoid-request-id"], completions[0].ContextMap()["autoid-request-id"])
				require.Equal(t, int64(2), completions[0].ContextMap()["rpc-error-count"])
				require.Equal(t, "recovered", completions[0].ContextMap()["outcome"])
				require.Empty(t, logs.FilterMessage("autoid request stopped after reaching RPC retry limit").All())
			})
		}
	})

	t.Run("non RPC errors are not retried", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		nonRPCErr := errors.New("local validation failed")
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		tests := []struct {
			name      string
			operation string
			ctx       context.Context
		}{
			{name: "alloc", operation: "alloc", ctx: context.Background()},
			{name: "rebase", operation: "rebase", ctx: context.Background()},
			{name: "alloc with canceled context", operation: "alloc", ctx: canceledCtx},
			{name: "rebase with canceled context", operation: "rebase", ctx: canceledCtx},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				mockCli := &mockAutoIDClient{
					allocErr:  nonRPCErr,
					rebaseErr: nonRPCErr,
				}
				allocator := newTestSinglePointAlloc(mockCli)
				err := runAutoIDTestOperation(test.ctx, test.operation, allocator)
				require.ErrorIs(t, err, nonRPCErr)
				require.False(t, IsRPCRetryLimitError(err))
				if test.operation == "alloc" {
					require.Equal(t, int64(1), mockCli.allocCallCount.Load())
				} else {
					require.Equal(t, int64(1), mockCli.rebaseCallCount.Load())
				}
			})
		}
		require.Empty(t, logs.FilterMessage("autoid request entered RPC retry").All())
	})

	t.Run("normal success adds no retry logs", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		service := &scriptedAutoIDServer{
			alloc:  func(int64) (*autoid.AutoIDResponse, error) { return successfulAutoIDResponse() },
			rebase: func(int64) (*autoid.RebaseResponse, error) { return successfulRebaseResponse() },
		}
		address := startScriptedAutoIDServer(t, service)
		putAutoIDServiceEndpoint(t, etcdCli, address)
		allocator := newRPCRetryTestAllocator(t, etcdCli)

		require.NoError(t, runAutoIDTestOperation(autoIDTestContext(t), "alloc", allocator))
		require.NoError(t, runAutoIDTestOperation(autoIDTestContext(t), "rebase", allocator))
		require.Empty(t, logs.FilterMessage("autoid request entered RPC retry").All())
		require.Empty(t, logs.FilterMessage("autoid request completed after RPC retry").All())
		require.Empty(t, logs.FilterMessage("autoid request stopped after reaching RPC retry limit").All())
	})
}

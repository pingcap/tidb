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
	"fmt"
	"net"
	"runtime"
	"sync"
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
	t.Cleanup(func() {
		server.Stop()
	})
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

func putAutoIDLeader(t *testing.T, cli *clientv3.Client, candidate, address string) autoIDLeaderSnapshot {
	t.Helper()
	key := AutoIDLeaderPath + "/" + candidate
	_, err := cli.Put(context.Background(), key, address)
	require.NoError(t, err)
	resp, err := cli.Get(context.Background(), key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	return autoIDLeaderSnapshot{
		address:        address,
		electionKey:    key,
		createRevision: resp.Kvs[0].CreateRevision,
	}
}

func replaceAutoIDLeader(cli *clientv3.Client, candidate, address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Delete(ctx, AutoIDLeaderPath+"/", clientv3.WithPrefix()); err != nil {
		return err
	}
	_, err := cli.Put(ctx, AutoIDLeaderPath+"/"+candidate, address)
	return err
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

func newNotLeaderTestAllocator(t *testing.T, etcdCli *clientv3.Client) *singlePointAlloc {
	t.Helper()
	allocator := &singlePointAlloc{
		dbID:           11,
		tblID:          22,
		ClientDiscover: NewClientDiscover(etcdCli),
		keyspaceID:     uint32(tikv.NullspaceID),
		notLeaderPolicy: notLeaderRetryPolicy{
			minConsecutive: 3,
			minDuration:    0,
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

// TestAutoIDNotLeaderHelpers covers the narrow error classifier, request-local
// count-and-duration policy, and terminal decision without real retry delays.
func TestAutoIDNotLeaderHelpers(t *testing.T) {
	t.Run("production default", func(t *testing.T) {
		policy := (&singlePointAlloc{}).effectiveNotLeaderPolicy()
		require.Equal(t, 10, policy.minConsecutive)
		require.Equal(t, 15*time.Second, policy.minDuration)
	})

	t.Run("handler converts revalidation failure", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints:          []string{"127.0.0.1:1"},
			MaxUnaryRetries:    1,
			BackoffWaitBetween: time.Millisecond,
			Logger:             zap.NewNop(),
		})
		require.NoError(t, err)
		require.NoError(t, etcdCli.Close())

		allocator := newNotLeaderTestAllocator(t, etcdCli)
		allocator.notLeaderPolicy.minConsecutive = 1
		leader := autoIDLeaderSnapshot{
			address:        "127.0.0.1:10080",
			electionKey:    "leader/1",
			createRevision: 10,
		}
		var state notLeaderRetryState
		requestLog := newAutoIDRequestLogState("alloc", allocator.keyspaceID, allocator.dbID, allocator.tblID, time.Now())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		handled, retryImmediately, terminalErr := allocator.handleNotLeaderError(
			ctx, "alloc", 0, leader, status.Error(codes.Unknown, "not leader"), &state, &requestLog,
		)
		require.NoError(t, ctx.Err())
		require.True(t, handled)
		require.False(t, retryImmediately)
		require.True(t, ErrAutoincReadFailed.Equal(terminalErr))
		require.True(t, IsNotLeaderFastFailError(terminalErr))
		require.Regexp(t, `could not be revalidated: .+; check etcd connectivity`, terminalErr.Error())
		require.Contains(t, terminalErr.Error(), autoIDNotLeaderAction)
		requestLog.complete(terminalErr)

		starts := logs.FilterMessage("autoid request entered not-leader retry").All()
		terminals := logs.FilterMessage("autoid request stopped after repeated not leader responses").All()
		require.Len(t, starts, 1)
		require.Len(t, terminals, 1)
		require.Equal(t, starts[0].ContextMap()["autoid-request-id"], terminals[0].ContextMap()["autoid-request-id"])
		require.Equal(t, "fast-failed", terminals[0].ContextMap()["outcome"])
		require.Contains(t, terminals[0].ContextMap()["error"], "could not be revalidated")
		require.Empty(t, logs.FilterMessage("autoid request completed after not-leader retry").All())
	})

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "exact status", err: status.Error(codes.Unknown, "not leader"), want: true},
		{name: "wrapped exact status", err: fmt.Errorf("wrapped: %w", status.Error(codes.Unknown, "not leader"))},
		{name: "unavailable", err: status.Error(codes.Unavailable, "not leader")},
		{name: "message suffix", err: status.Error(codes.Unknown, "not leader: retry")},
		{name: "canceled", err: status.Error(codes.Canceled, "not leader")},
		{name: "plain error", err: errors.New("not leader")},
		{name: "nil"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, isAutoIDNotLeaderError(test.err))
		})
	}

	policy := notLeaderRetryPolicy{minConsecutive: 3, minDuration: 2 * time.Second}
	start := time.Unix(100, 0)
	leader := autoIDLeaderSnapshot{address: "127.0.0.1:10080", electionKey: "leader/1", createRevision: 10}
	var state notLeaderRetryState

	require.False(t, state.observe(leader, start, policy))
	require.False(t, state.observe(leader, start.Add(time.Second), policy))
	require.True(t, state.observe(leader, start.Add(2*time.Second), policy))
	require.Equal(t, 3, state.count)
	require.Equal(t, start, state.firstSeen)

	t.Run("count and duration use AND semantics", func(t *testing.T) {
		var countOnly notLeaderRetryState
		require.False(t, countOnly.observe(leader, start, policy))
		require.False(t, countOnly.observe(leader, start, policy))
		require.False(t, countOnly.observe(leader, start, policy))

		var durationOnly notLeaderRetryState
		require.False(t, durationOnly.observe(leader, start, policy))
		require.False(t, durationOnly.observe(leader, start.Add(3*time.Second), policy))
	})

	t.Run("leader generation changes restart the sequence", func(t *testing.T) {
		changes := []autoIDLeaderSnapshot{
			{address: "127.0.0.2:10080", electionKey: leader.electionKey, createRevision: leader.createRevision},
			{address: leader.address, electionKey: "leader/2", createRevision: leader.createRevision},
			{address: leader.address, electionKey: leader.electionKey, createRevision: leader.createRevision + 1},
		}
		for _, changed := range changes {
			var current notLeaderRetryState
			require.False(t, current.observe(leader, start, policy))
			require.False(t, current.observe(leader, start.Add(time.Second), policy))
			require.False(t, current.observe(changed, start.Add(3*time.Second), policy))
			require.Equal(t, 1, current.count)
			require.Equal(t, changed, current.leader)
			require.Equal(t, start.Add(3*time.Second), current.firstSeen)
		}
	})

	state.reset()
	require.Zero(t, state.count)
	require.True(t, state.firstSeen.IsZero())
	require.Equal(t, autoIDLeaderSnapshot{}, state.leader)
}

// TestAutoIDLeaderSnapshotCache verifies that a cached client, its election
// metadata, and its local generation are read and reset as one unit.
func TestAutoIDLeaderSnapshotCache(t *testing.T) {
	discover := &ClientDiscover{}
	firstClient := &mockAutoIDClient{}
	firstLeader := autoIDLeaderSnapshot{address: "127.0.0.1:10080", electionKey: "leader/1", createRevision: 10}
	discover.mu.AutoIDAllocClient = firstClient
	discover.mu.leaderSnapshot = firstLeader
	discover.mu.version = 7

	client, version, leader, err := discover.getClientWithLeader(context.Background(), 0)
	require.NoError(t, err)
	require.Same(t, firstClient, client)
	require.Equal(t, uint64(7), version)
	require.Equal(t, firstLeader, leader)

	discover.resetConn(6, nil)
	client, version, leader, err = discover.getClientWithLeader(context.Background(), 0)
	require.NoError(t, err)
	require.Same(t, firstClient, client)
	require.Equal(t, uint64(7), version)
	require.Equal(t, firstLeader, leader)

	var resets sync.WaitGroup
	resets.Add(2)
	go func() {
		defer resets.Done()
		discover.resetConn(7, nil)
	}()
	go func() {
		defer resets.Done()
		discover.resetConn(7, nil)
	}()
	resets.Wait()

	discover.mu.RLock()
	require.Nil(t, discover.mu.AutoIDAllocClient)
	require.Equal(t, autoIDLeaderSnapshot{}, discover.mu.leaderSnapshot)
	require.Equal(t, uint64(8), discover.mu.version)
	discover.mu.RUnlock()

	secondClient := &mockAutoIDClient{}
	secondLeader := autoIDLeaderSnapshot{address: "127.0.0.2:10080", electionKey: "leader/2", createRevision: 20}
	discover.mu.Lock()
	discover.mu.AutoIDAllocClient = secondClient
	discover.mu.leaderSnapshot = secondLeader
	discover.mu.Unlock()
	discover.resetConn(7, nil)

	client, version, leader, err = discover.getClientWithLeader(context.Background(), 0)
	require.NoError(t, err)
	require.Same(t, secondClient, client)
	require.Equal(t, uint64(8), version)
	require.Equal(t, secondLeader, leader)

	discover.ResetConn(nil)
	discover.mu.RLock()
	require.Nil(t, discover.mu.AutoIDAllocClient)
	require.Equal(t, autoIDLeaderSnapshot{}, discover.mu.leaderSnapshot)
	require.Equal(t, uint64(9), discover.mu.version)
	discover.mu.RUnlock()
}

// TestAutoIDNotLeaderRetry exercises the request-local fast-fail boundary,
// leader-generation revalidation, existing retry behavior, and bounded logs.
func TestAutoIDNotLeaderRetry(t *testing.T) {
	notLeader := status.Error(codes.Unknown, "not leader")

	t.Run("persistent response fast-fails alloc and rebase", func(t *testing.T) {
		for _, operation := range []string{"alloc", "rebase"} {
			t.Run(operation, func(t *testing.T) {
				logs := observeAutoIDLogs(t)
				etcdCli := newAutoIDTestEtcdClient(t)
				service := &scriptedAutoIDServer{
					alloc:  func(int64) (*autoid.AutoIDResponse, error) { return nil, notLeader },
					rebase: func(int64) (*autoid.RebaseResponse, error) { return nil, notLeader },
				}
				address := startScriptedAutoIDServer(t, service)
				putAutoIDLeader(t, etcdCli, "candidate-1", address)
				allocator := newNotLeaderTestAllocator(t, etcdCli)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := runAutoIDTestOperation(ctx, operation, allocator)
				require.Error(t, err)
				require.False(t, errors.Is(err, context.DeadlineExceeded))
				require.True(t, ErrAutoincReadFailed.Equal(err))
				require.True(t, IsNotLeaderFastFailError(err))
				require.True(t, IsNotLeaderFastFailError(fmt.Errorf("wrapped: %w", err)))
				require.Contains(t, fmt.Sprintf("%+v", err), "repeatedNotLeaderError")
				require.Equal(t, int64(3), autoIDOperationCallCount(operation, service))
				require.Contains(t, err.Error(), "autoid "+operation+" failed after 3 consecutive")
				require.Contains(t, err.Error(), `"not leader"`)
				require.Contains(t, err.Error(), " over ")
				require.Contains(t, err.Error(), address)
				require.Contains(t, err.Error(), fmt.Sprintf("keyspace_id=%d, db_id=11, table_id=22", allocator.keyspaceID))
				require.Contains(t, err.Error(), autoIDNotLeaderAction)

				starts := logs.FilterMessage("autoid request entered not-leader retry").All()
				terminals := logs.FilterMessage("autoid request stopped after repeated not leader responses").All()
				require.Len(t, starts, 1)
				require.Len(t, terminals, 1)
				require.Equal(t, starts[0].ContextMap()["autoid-request-id"], terminals[0].ContextMap()["autoid-request-id"])
				require.Equal(t, zap.WarnLevel, terminals[0].Level)
				terminalFields := terminals[0].ContextMap()
				require.Equal(t, operation, terminalFields["operation"])
				require.Equal(t, allocator.keyspaceID, terminalFields["keyspace-id"])
				require.Equal(t, int64(11), terminalFields["db-id"])
				require.Equal(t, int64(22), terminalFields["table-id"])
				require.Equal(t, address, terminalFields["leader-address"])
				require.IsType(t, time.Duration(0), terminalFields["request-elapsed"])
				require.IsType(t, time.Duration(0), terminalFields["not-leader-elapsed"])
				require.Equal(t, int64(3), terminalFields["consecutive-not-leader-count"])
				require.Equal(t, "fast-failed", terminalFields["outcome"])
				require.Equal(t, autoIDNotLeaderAction, terminalFields["action"])
				require.Empty(t, logs.FilterMessage("autoid request completed after not-leader retry").All())
			})
		}
	})

	t.Run("nonzero duration gates alloc and rebase", func(t *testing.T) {
		const minDuration = 300 * time.Millisecond
		for _, operation := range []string{"alloc", "rebase"} {
			t.Run(operation, func(t *testing.T) {
				etcdCli := newAutoIDTestEtcdClient(t)
				service := &scriptedAutoIDServer{
					alloc:  func(int64) (*autoid.AutoIDResponse, error) { return nil, notLeader },
					rebase: func(int64) (*autoid.RebaseResponse, error) { return nil, notLeader },
				}
				address := startScriptedAutoIDServer(t, service)
				putAutoIDLeader(t, etcdCli, "candidate-1", address)
				allocator := newNotLeaderTestAllocator(t, etcdCli)
				allocator.notLeaderPolicy.minDuration = minDuration

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				start := time.Now()
				err := runAutoIDTestOperation(ctx, operation, allocator)
				elapsed := time.Since(start)
				require.True(t, IsNotLeaderFastFailError(err))
				require.GreaterOrEqual(t, elapsed, minDuration)
			})
		}
	})

	t.Run("transient responses recover before threshold", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		service := &scriptedAutoIDServer{
			alloc: func(call int64) (*autoid.AutoIDResponse, error) {
				if call < 3 {
					return nil, notLeader
				}
				return successfulAutoIDResponse()
			},
		}
		address := startScriptedAutoIDServer(t, service)
		putAutoIDLeader(t, etcdCli, "candidate-1", address)
		allocator := newNotLeaderTestAllocator(t, etcdCli)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		minID, maxID, err := allocator.Alloc(ctx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, int64(100), minID)
		require.Equal(t, int64(101), maxID)
		require.Equal(t, int64(3), service.allocCallCount.Load())
		starts := logs.FilterMessage("autoid request entered not-leader retry").All()
		require.Len(t, starts, 1)
		completions := logs.FilterMessage("autoid request completed after not-leader retry").All()
		require.Len(t, completions, 1)
		require.Equal(t, starts[0].ContextMap()["autoid-request-id"], completions[0].ContextMap()["autoid-request-id"])
		require.Equal(t, "recovered", completions[0].ContextMap()["outcome"])
		require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())
	})

	t.Run("leader change before threshold records the final address", func(t *testing.T) {
		for _, operation := range []string{"alloc", "rebase"} {
			t.Run(operation, func(t *testing.T) {
				logs := observeAutoIDLogs(t)
				etcdCli := newAutoIDTestEtcdClient(t)
				serving := &scriptedAutoIDServer{
					alloc:  func(int64) (*autoid.AutoIDResponse, error) { return successfulAutoIDResponse() },
					rebase: func(int64) (*autoid.RebaseResponse, error) { return successfulRebaseResponse() },
				}
				servingAddress := startScriptedAutoIDServer(t, serving)
				stale := &scriptedAutoIDServer{
					alloc: func(int64) (*autoid.AutoIDResponse, error) {
						if err := replaceAutoIDLeader(etcdCli, "candidate-2", servingAddress); err != nil {
							return nil, err
						}
						return nil, notLeader
					},
					rebase: func(int64) (*autoid.RebaseResponse, error) {
						if err := replaceAutoIDLeader(etcdCli, "candidate-2", servingAddress); err != nil {
							return nil, err
						}
						return nil, notLeader
					},
				}
				staleAddress := startScriptedAutoIDServer(t, stale)
				putAutoIDLeader(t, etcdCli, "candidate-1", staleAddress)
				allocator := newNotLeaderTestAllocator(t, etcdCli)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := runAutoIDTestOperation(ctx, operation, allocator)
				require.NoError(t, err)
				require.Equal(t, int64(1), autoIDOperationCallCount(operation, stale))
				require.Equal(t, int64(1), autoIDOperationCallCount(operation, serving))
				require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())

				starts := logs.FilterMessage("autoid request entered not-leader retry").All()
				completions := logs.FilterMessage("autoid request completed after not-leader retry").All()
				require.Len(t, starts, 1)
				require.Len(t, completions, 1)
				require.Equal(t, starts[0].ContextMap()["autoid-request-id"], completions[0].ContextMap()["autoid-request-id"])
				require.Equal(t, staleAddress, starts[0].ContextMap()["leader-address"])
				require.Equal(t, servingAddress, completions[0].ContextMap()["leader-address"])
				require.Equal(t, "recovered", completions[0].ContextMap()["outcome"])
			})
		}
	})

	t.Run("terminal revalidation follows a new leader address", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		serving := &scriptedAutoIDServer{alloc: func(int64) (*autoid.AutoIDResponse, error) {
			return successfulAutoIDResponse()
		}}
		servingAddress := startScriptedAutoIDServer(t, serving)
		stale := &scriptedAutoIDServer{}
		stale.alloc = func(call int64) (*autoid.AutoIDResponse, error) {
			if call == 3 {
				if err := replaceAutoIDLeader(etcdCli, "candidate-2", servingAddress); err != nil {
					return nil, err
				}
			}
			return nil, notLeader
		}
		staleAddress := startScriptedAutoIDServer(t, stale)
		putAutoIDLeader(t, etcdCli, "candidate-1", staleAddress)
		allocator := newNotLeaderTestAllocator(t, etcdCli)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := allocator.Alloc(ctx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, int64(3), stale.allocCallCount.Load())
		require.Equal(t, int64(1), serving.allocCallCount.Load())
		require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())
		completions := logs.FilterMessage("autoid request completed after not-leader retry").All()
		require.Len(t, completions, 1)
		require.Equal(t, servingAddress, completions[0].ContextMap()["leader-address"])
	})

	t.Run("same address with a new campaign gets a fresh attempt", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		service := &scriptedAutoIDServer{}
		address := startScriptedAutoIDServer(t, service)
		service.alloc = func(call int64) (*autoid.AutoIDResponse, error) {
			if call == 3 {
				if err := replaceAutoIDLeader(etcdCli, "candidate-1", address); err != nil {
					return nil, err
				}
			}
			if call <= 3 {
				return nil, notLeader
			}
			return successfulAutoIDResponse()
		}
		first := putAutoIDLeader(t, etcdCli, "candidate-1", address)
		allocator := newNotLeaderTestAllocator(t, etcdCli)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := allocator.Alloc(ctx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, int64(4), service.allocCallCount.Load())
		_, _, current, err := allocator.getClientWithLeader(ctx, allocator.keyspaceID)
		require.NoError(t, err)
		require.Equal(t, first.address, current.address)
		require.Equal(t, first.electionKey, current.electionKey)
		require.NotEqual(t, first.createRevision, current.createRevision)
		require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())
		require.Len(t, logs.FilterMessage("autoid request completed after not-leader retry").All(), 1)
	})

	t.Run("an unrelated rpc error breaks the consecutive sequence", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		service := &scriptedAutoIDServer{
			alloc: func(call int64) (*autoid.AutoIDResponse, error) {
				switch call {
				case 1, 2, 4, 5:
					return nil, notLeader
				case 3:
					return nil, status.Error(codes.Unavailable, "temporary transport failure")
				default:
					return successfulAutoIDResponse()
				}
			},
		}
		address := startScriptedAutoIDServer(t, service)
		putAutoIDLeader(t, etcdCli, "candidate-1", address)
		allocator := newNotLeaderTestAllocator(t, etcdCli)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := allocator.Alloc(ctx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, int64(6), service.allocCallCount.Load())
		require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())
		starts := logs.FilterMessage("autoid request entered not-leader retry").All()
		completions := logs.FilterMessage("autoid request completed after not-leader retry").All()
		require.Len(t, starts, 1)
		require.Len(t, completions, 1)
		require.Equal(t, int64(4), completions[0].ContextMap()["not-leader-count"])
		require.Equal(t, "recovered", completions[0].ContextMap()["outcome"])
	})

	t.Run("response errmsg keeps its existing return path", func(t *testing.T) {
		for _, operation := range []string{"alloc", "rebase"} {
			t.Run(operation, func(t *testing.T) {
				logs := observeAutoIDLogs(t)
				etcdCli := newAutoIDTestEtcdClient(t)
				service := &scriptedAutoIDServer{
					alloc: func(int64) (*autoid.AutoIDResponse, error) {
						return &autoid.AutoIDResponse{Errmsg: []byte("not leader")}, nil
					},
					rebase: func(int64) (*autoid.RebaseResponse, error) {
						return &autoid.RebaseResponse{Errmsg: []byte("not leader")}, nil
					},
				}
				address := startScriptedAutoIDServer(t, service)
				putAutoIDLeader(t, etcdCli, "candidate-1", address)
				allocator := newNotLeaderTestAllocator(t, etcdCli)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := runAutoIDTestOperation(ctx, operation, allocator)
				require.EqualError(t, err, "not leader")
				require.Equal(t, int64(1), autoIDOperationCallCount(operation, service))
				require.Empty(t, logs.FilterMessage("autoid request entered not-leader retry").All())
				require.Empty(t, logs.FilterMessage("autoid request completed after not-leader retry").All())
				require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())
			})
		}
	})

	t.Run("response errmsg after not leader completes as failed", func(t *testing.T) {
		for _, operation := range []string{"alloc", "rebase"} {
			t.Run(operation, func(t *testing.T) {
				logs := observeAutoIDLogs(t)
				etcdCli := newAutoIDTestEtcdClient(t)
				service := &scriptedAutoIDServer{
					alloc: func(call int64) (*autoid.AutoIDResponse, error) {
						if call == 1 {
							return nil, notLeader
						}
						return &autoid.AutoIDResponse{Errmsg: []byte("service rejected request")}, nil
					},
					rebase: func(call int64) (*autoid.RebaseResponse, error) {
						if call == 1 {
							return nil, notLeader
						}
						return &autoid.RebaseResponse{Errmsg: []byte("service rejected request")}, nil
					},
				}
				address := startScriptedAutoIDServer(t, service)
				putAutoIDLeader(t, etcdCli, "candidate-1", address)
				allocator := newNotLeaderTestAllocator(t, etcdCli)

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := runAutoIDTestOperation(ctx, operation, allocator)
				require.EqualError(t, err, "service rejected request")
				require.Equal(t, int64(2), autoIDOperationCallCount(operation, service))
				require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())

				starts := logs.FilterMessage("autoid request entered not-leader retry").All()
				completions := logs.FilterMessage("autoid request completed after not-leader retry").All()
				require.Len(t, starts, 1)
				require.Len(t, completions, 1)
				require.Equal(t, starts[0].ContextMap()["autoid-request-id"], completions[0].ContextMap()["autoid-request-id"])
				require.Equal(t, "failed", completions[0].ContextMap()["outcome"])
			})
		}
	})

	t.Run("context cancellation wins at the threshold boundary", func(t *testing.T) {
		for _, operation := range []string{"alloc", "rebase"} {
			t.Run(operation, func(t *testing.T) {
				logs := observeAutoIDLogs(t)
				etcdCli := newAutoIDTestEtcdClient(t)
				ctx, cancel := context.WithCancel(context.Background())
				service := &scriptedAutoIDServer{
					alloc: func(call int64) (*autoid.AutoIDResponse, error) {
						if call == 3 {
							cancel()
						}
						return nil, notLeader
					},
					rebase: func(call int64) (*autoid.RebaseResponse, error) {
						if call == 3 {
							cancel()
						}
						return nil, notLeader
					},
				}
				address := startScriptedAutoIDServer(t, service)
				putAutoIDLeader(t, etcdCli, "candidate-1", address)
				allocator := newNotLeaderTestAllocator(t, etcdCli)

				err := runAutoIDTestOperation(ctx, operation, allocator)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, int64(3), autoIDOperationCallCount(operation, service))
				require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())
				starts := logs.FilterMessage("autoid request entered not-leader retry").All()
				completions := logs.FilterMessage("autoid request completed after not-leader retry").All()
				require.Len(t, starts, 1)
				require.Len(t, completions, 1)
				require.Equal(t, starts[0].ContextMap()["autoid-request-id"], completions[0].ContextMap()["autoid-request-id"])
				require.Equal(t, "context-canceled", completions[0].ContextMap()["outcome"])
			})
		}
	})

	t.Run("a later request succeeds after remediation", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		var remediated atomic.Bool
		service := &scriptedAutoIDServer{alloc: func(int64) (*autoid.AutoIDResponse, error) {
			if remediated.Load() {
				return successfulAutoIDResponse()
			}
			return nil, notLeader
		}}
		address := startScriptedAutoIDServer(t, service)
		putAutoIDLeader(t, etcdCli, "candidate-1", address)
		allocator := newNotLeaderTestAllocator(t, etcdCli)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := allocator.Alloc(ctx, 1, 1, 1)
		require.Error(t, err)
		remediated.Store(true)
		_, _, err = allocator.Alloc(ctx, 1, 1, 1)
		require.NoError(t, err)
		require.Equal(t, int64(4), service.allocCallCount.Load())
		require.Len(t, logs.FilterMessage("autoid request entered not-leader retry").All(), 1)
		require.Len(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All(), 1)
		require.Empty(t, logs.FilterMessage("autoid request completed after not-leader retry").All())
	})

	t.Run("concurrent callers keep independent counts and log pairs", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		service := &scriptedAutoIDServer{alloc: func(int64) (*autoid.AutoIDResponse, error) {
			return nil, notLeader
		}}
		address := startScriptedAutoIDServer(t, service)
		putAutoIDLeader(t, etcdCli, "candidate-1", address)
		allocator := newNotLeaderTestAllocator(t, etcdCli)

		const requestCount = 4
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errs := make(chan error, requestCount)
		var requests sync.WaitGroup
		for range requestCount {
			requests.Add(1)
			go func() {
				defer requests.Done()
				_, _, err := allocator.Alloc(ctx, 1, 1, 1)
				errs <- err
			}()
		}
		requests.Wait()
		close(errs)
		for err := range errs {
			require.Error(t, err)
			require.Contains(t, err.Error(), "after 3 consecutive")
		}
		require.Equal(t, int64(requestCount*3), service.allocCallCount.Load())

		starts := logs.FilterMessage("autoid request entered not-leader retry").All()
		terminals := logs.FilterMessage("autoid request stopped after repeated not leader responses").All()
		require.Len(t, starts, requestCount)
		require.Len(t, terminals, requestCount)
		require.Empty(t, logs.FilterMessage("autoid request completed after not-leader retry").All())
		requestIDs := make(map[any]struct{}, requestCount)
		for _, entry := range starts {
			requestIDs[entry.ContextMap()["autoid-request-id"]] = struct{}{}
		}
		require.Len(t, requestIDs, requestCount)
		for _, entry := range terminals {
			_, ok := requestIDs[entry.ContextMap()["autoid-request-id"]]
			require.True(t, ok)
			require.Equal(t, int64(3), entry.ContextMap()["consecutive-not-leader-count"])
		}
	})

	t.Run("normal success adds no abnormal request logs", func(t *testing.T) {
		logs := observeAutoIDLogs(t)
		etcdCli := newAutoIDTestEtcdClient(t)
		service := &scriptedAutoIDServer{
			alloc:  func(int64) (*autoid.AutoIDResponse, error) { return successfulAutoIDResponse() },
			rebase: func(int64) (*autoid.RebaseResponse, error) { return successfulRebaseResponse() },
		}
		address := startScriptedAutoIDServer(t, service)
		putAutoIDLeader(t, etcdCli, "candidate-1", address)
		allocator := newNotLeaderTestAllocator(t, etcdCli)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, err := allocator.Alloc(ctx, 1, 1, 1)
		require.NoError(t, err)
		require.NoError(t, allocator.rebase(ctx, 100, false))
		require.Empty(t, logs.FilterMessage("autoid request entered not-leader retry").All())
		require.Empty(t, logs.FilterMessage("autoid request completed after not-leader retry").All())
		require.Empty(t, logs.FilterMessage("autoid request stopped after repeated not leader responses").All())
	})
}

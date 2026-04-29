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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
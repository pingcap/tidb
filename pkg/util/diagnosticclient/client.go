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

// Package diagnosticclient provides diagnostic-mode guards for PD and TiKV RPCs.
package diagnosticclient

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"github.com/tikv/pd/client/opt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PDClientOption installs the placeholder PD RPC allowlist; callers must enable it only in diagnostic mode.
func PDClientOption() opt.ClientOption {
	return opt.WithGRPCDialOptions(grpc.WithChainUnaryInterceptor(pdUnary), grpc.WithChainStreamInterceptor(pdStream))
}

func pdUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	switch method {
	case "/pdpb.PD/GetMembers",
		"/pdpb.PD/GetClusterInfo",
		"/pdpb.PD/GetStore",
		"/routerpb.Router/GetStore",
		"/pdpb.PD/GetAllStores",
		"/routerpb.Router/GetAllStores",
		"/pdpb.PD/GetRegion",
		"/routerpb.Router/GetRegion",
		"/pdpb.PD/GetPrevRegion",
		"/routerpb.Router/GetPrevRegion",
		"/pdpb.PD/GetRegionByID",
		"/routerpb.Router/GetRegionByID",
		"/pdpb.PD/ScanRegions",
		"/pdpb.PD/BatchScanRegions",
		"/routerpb.Router/BatchScanRegions",
		"/pdpb.PD/GetOperator",
		"/pdpb.PD/LoadGlobalConfig",
		"/pdpb.PD/GetExternalTimestamp",
		"/keyspacepb.Keyspace/LoadKeyspace",
		"/keyspacepb.Keyspace/LoadKeyspaceByID",
		"/keyspacepb.Keyspace/GetAllKeyspaces",
		"/meta_storagepb.MetaStorage/Get",
		"/resource_manager.ResourceManager/GetResourceGroup",
		"/pdpb.PD/GetGCState",
		"/pdpb.PD/GetAllKeyspacesGCStates",
		"/tsopb.TSO/FindGroupByKeyspaceID",
		"/grpc.health.v1.Health/Check":
		return inv(ctx, method, req, reply, cc, opts...)
	}
	return status.Errorf(codes.PermissionDenied, "diagnostic mode: blocked PD RPC %s", method)
}
func pdStream(ctx context.Context, d *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	switch method {
	// TSO is required for store initialization and MVCC reads.
	case "/pdpb.PD/Tso",
		"/tsopb.TSO/Tso",
		"/pdpb.PD/WatchGlobalConfig",
		"/pdpb.PD/QueryRegion",
		"/routerpb.Router/QueryRegion",
		"/meta_storagepb.MetaStorage/Watch":
		return streamer(ctx, d, cc, method, opts...)
	}
	return nil, status.Errorf(codes.PermissionDenied, "diagnostic mode: blocked PD stream %s", method)
}

// KVClient forwards only the placeholder allowlist of TiKV read commands.
type KVClient struct{ tikv.Client }

// WrapKV adds the KV guard in diagnostic mode and otherwise returns the original client.
func WrapKV(c tikv.Client) tikv.Client {
	if !diagnosticmode.Enabled() {
		return c
	}
	return &KVClient{Client: c}
}
func (c *KVClient) allowed(r *tikvrpc.Request) bool {
	if r == nil {
		return false
	}
	switch r.Type {
	case tikvrpc.CmdGet,
		tikvrpc.CmdBatchGet,
		tikvrpc.CmdScan,
		tikvrpc.CmdRawGet,
		tikvrpc.CmdRawBatchGet,
		tikvrpc.CmdRawScan,
		tikvrpc.CmdRawGetKeyTTL,
		tikvrpc.CmdRawChecksum,
		tikvrpc.CmdCop,
		tikvrpc.CmdCopStream,
		tikvrpc.CmdBatchCop,
		tikvrpc.CmdMvccGetByKey,
		tikvrpc.CmdMvccGetByStartTs,
		tikvrpc.CmdStoreSafeTS,
		tikvrpc.CmdLockWaitInfo,
		tikvrpc.CmdDebugGetRegionProperties,
		tikvrpc.CmdGetHealthFeedback,
		tikvrpc.CmdGetTiFlashSystemTable,
		tikvrpc.CmdMPPAlive,
		tikvrpc.CmdScanLock,
		tikvrpc.CmdPhysicalScanLock,
		tikvrpc.CmdBufferBatchGet:
		return true
	}
	return false
}

// SendRequest rejects disallowed commands before invoking the underlying client.
func (c *KVClient) SendRequest(ctx context.Context, addr string, r *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if !c.allowed(r) {
		return nil, status.Error(codes.PermissionDenied, "diagnostic mode: blocked KV RPC")
	}
	return c.Client.SendRequest(ctx, addr, r, timeout)
}

// SendRequestAsync completes rejected requests with an error through the callback.
func (c *KVClient) SendRequestAsync(ctx context.Context, addr string, r *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	if !c.allowed(r) {
		cb.Invoke(nil, status.Error(codes.PermissionDenied, "diagnostic mode: blocked KV RPC"))
		return
	}
	c.Client.SendRequestAsync(ctx, addr, r, cb)
}

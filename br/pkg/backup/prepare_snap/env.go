// Copyright 2024 PingCAP, Inc.
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

package preparesnap

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// default max gRPC message size is 10MiB.
	// split requests to chunks of 1MiB will reduce the possibility of being rejected
	// due to max gRPC message size.
	maxRequestSize = units.MiB
)

type Env interface {
	ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error)
	GetAllLiveStores(ctx context.Context) ([]*metapb.Store, error)

	LoadRegionsInKeyRange(ctx context.Context, startKey, endKey []byte) (regions []Region, err error)
}

type PrepareClient interface {
	Send(*brpb.PrepareSnapshotBackupRequest) error
	Recv() (*brpb.PrepareSnapshotBackupResponse, error)
}

type SplitRequestClient struct {
	PrepareClient
	MaxRequestSize int
}

func (s SplitRequestClient) Send(req *brpb.PrepareSnapshotBackupRequest) error {
	// Try best to keeping the request untouched.
	if req.Ty == brpb.PrepareSnapshotBackupRequestType_WaitApply && req.Size() > s.MaxRequestSize {
		rs := req.Regions
		findSplitIndex := func() int {
			if len(rs) == 0 {
				return -1
			}

			// Select at least one request.
			// So we won't get sutck if there were a really huge (!) request.
			collected := 0
			lastI := 1
			for i := 2; i < len(rs) && collected+rs[i].Size() < s.MaxRequestSize; i++ {
				lastI = i
				collected += rs[i].Size()
			}
			return lastI
		}
		for splitIdx := findSplitIndex(); splitIdx > 0; splitIdx = findSplitIndex() {
			split := &brpb.PrepareSnapshotBackupRequest{
				Ty:      brpb.PrepareSnapshotBackupRequestType_WaitApply,
				Regions: rs[:splitIdx],
			}
			rs = rs[splitIdx:]
			if err := s.PrepareClient.Send(split); err != nil {
				return err
			}
		}
		return nil
	}
	return s.PrepareClient.Send(req)
}

type Region interface {
	GetMeta() *metapb.Region
	GetLeaderStoreID() uint64
}

type CliEnv struct {
	Cache *tikv.RegionCache
	Mgr   *utils.StoreManager
}

func (c CliEnv) GetAllLiveStores(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := c.Cache.PDClient().GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	withoutTiFlash := slices.DeleteFunc(stores, engine.IsTiFlash)
	return withoutTiFlash, err
}

func AdaptForGRPCInTest(p PrepareClient) PrepareClient {
	return &gRPCGoAdapter{
		inner: p,
	}
}

// GrpcGoAdapter makes the `Send` call synchronous.
// grpc-go doesn't guarantee concurrency call to `Send` or `Recv` is safe.
// But concurrency call to `send` and `recv` is safe.
// This type is exported for testing.
type gRPCGoAdapter struct {
	inner  PrepareClient
	sendMu sync.Mutex
	recvMu sync.Mutex
}

func (s *gRPCGoAdapter) Send(req *brpb.PrepareSnapshotBackupRequest) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.inner.Send(req)
}

func (s *gRPCGoAdapter) Recv() (*brpb.PrepareSnapshotBackupResponse, error) {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	return s.inner.Recv()
}

func (c CliEnv) ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error) {
	var cli brpb.Backup_PrepareSnapshotBackupClient
	err := c.Mgr.TryWithConn(ctx, storeID, func(cc *grpc.ClientConn) error {
		bcli := brpb.NewBackupClient(cc)
		c, err := bcli.PrepareSnapshotBackup(ctx)
		if err != nil {
			return errors.Annotatef(err, "failed to create prepare backup stream")
		}
		cli = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gRPCGoAdapter{inner: cli}, nil
}

func (c CliEnv) LoadRegionsInKeyRange(ctx context.Context, startKey []byte, endKey []byte) (regions []Region, err error) {
	bo := tikv.NewBackoffer(ctx, regionCacheMaxBackoffMs)
	if len(endKey) == 0 {
		// This is encoded [0xff; 8].
		// Workaround for https://github.com/tikv/client-go/issues/1051.
		endKey = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	}
	rs, err := c.Cache.LoadRegionsInKeyRange(bo, startKey, endKey)
	if err != nil {
		return nil, err
	}
	rrs := make([]Region, 0, len(rs))
	for _, r := range rs {
		rrs = append(rrs, r)
	}
	return rrs, nil
}

type RetryAndSplitRequestEnv struct {
	Env
	GetBackoffer func() utils.Backoffer
}

func (r RetryAndSplitRequestEnv) ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error) {
	rs := utils.ConstantBackoff(10 * time.Second)
	bo := utils.Backoffer(rs)
	if r.GetBackoffer != nil {
		bo = r.GetBackoffer()
	}
	cli, err := utils.WithRetryV2(ctx, bo, func(ctx context.Context) (PrepareClient, error) {
		cli, err := r.Env.ConnectToStore(ctx, storeID)
		if err != nil {
			log.Warn("Failed to connect to store, will retry.", zap.Uint64("store", storeID), logutil.ShortError(err))
			return nil, err
		}
		return cli, nil
	})
	if err != nil {
		return nil, err
	}
	return SplitRequestClient{PrepareClient: cli, MaxRequestSize: maxRequestSize}, nil
}

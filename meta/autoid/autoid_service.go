// Copyright 2022 PingCAP, Inc.
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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/tracing"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var _ Allocator = &singlePointAlloc{}

type singlePointAlloc struct {
	dbID          int64
	tblID         int64
	lastAllocated int64
	isUnsigned    bool
	clientDiscover
}

type clientDiscover struct {
	// This the etcd client for service discover
	etcdCli *clientv3.Client
	// This is the real client for the AutoIDAlloc service
	mu struct {
		sync.RWMutex
		autoid.AutoIDAllocClient
		// Release the client conn to avoid resource leak!
		// See https://github.com/grpc/grpc-go/issues/5321
		*grpc.ClientConn
	}
}

const (
	autoIDLeaderPath = "tidb/autoid/leader"
)

func (d *clientDiscover) GetClient(ctx context.Context) (autoid.AutoIDAllocClient, error) {
	d.mu.RLock()
	cli := d.mu.AutoIDAllocClient
	if cli != nil {
		d.mu.RUnlock()
		return cli, nil
	}
	d.mu.RUnlock()

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mu.AutoIDAllocClient != nil {
		return d.mu.AutoIDAllocClient, nil
	}

	resp, err := d.etcdCli.Get(ctx, autoIDLeaderPath, clientv3.WithFirstCreate()...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("autoid service leader not found")
	}

	addr := string(resp.Kvs[0].Value)
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		clusterSecurity := security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	logutil.BgLogger().Info("[autoid client] connect to leader", zap.String("addr", addr))
	grpcConn, err := grpc.Dial(addr, opt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli = autoid.NewAutoIDAllocClient(grpcConn)
	d.mu.AutoIDAllocClient = cli
	d.mu.ClientConn = grpcConn
	return cli, nil
}

// Alloc allocs N consecutive autoID for table with tableID, returning (min, max] of the allocated autoID batch.
// The consecutive feature is used to insert multiple rows in a statement.
// increment & offset is used to validate the start position (the allocator's base is not always the last allocated id).
// The returned range is (min, max]:
// case increment=1 & offset=1: you can derive the ids like min+1, min+2... max.
// case increment=x & offset=y: you firstly need to seek to firstID by `SeekToFirstAutoIDXXX`, then derive the IDs like firstID, firstID + increment * 2... in the caller.
func (sp *singlePointAlloc) Alloc(ctx context.Context, n uint64, increment, offset int64) (min int64, max int64, _ error) {
	r, ctx := tracing.StartRegionEx(ctx, "autoid.Alloc")
	defer r.End()

	if !validIncrementAndOffset(increment, offset) {
		return 0, 0, errInvalidIncrementAndOffset.GenWithStackByArgs(increment, offset)
	}

retry:
	cli, err := sp.GetClient(ctx)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	start := time.Now()
	resp, err := cli.AllocAutoID(ctx, &autoid.AutoIDRequest{
		DbID:       sp.dbID,
		TblID:      sp.tblID,
		N:          n,
		Increment:  increment,
		Offset:     offset,
		IsUnsigned: sp.isUnsigned,
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(start).Seconds())
	if err != nil {
		if strings.Contains(err.Error(), "rpc error") {
			time.Sleep(backoffDuration)
			sp.resetConn(err)
			goto retry
		}
		return 0, 0, errors.Trace(err)
	}
	if len(resp.Errmsg) != 0 {
		return 0, 0, errors.Trace(errors.New(string(resp.Errmsg)))
	}

	du := time.Since(start)
	metrics.AutoIDReqDuration.Observe(du.Seconds())
	sp.lastAllocated = resp.Min
	return resp.Min, resp.Max, err
}

const backoffDuration = 200 * time.Millisecond

func (sp *singlePointAlloc) resetConn(reason error) {
	logutil.BgLogger().Info("[autoid client] reset grpc connection",
		zap.String("reason", reason.Error()))
	var grpcConn *grpc.ClientConn
	sp.mu.Lock()
	grpcConn = sp.mu.ClientConn
	sp.mu.AutoIDAllocClient = nil
	sp.mu.ClientConn = nil
	sp.mu.Unlock()
	// Close grpc.ClientConn to release resource.
	if grpcConn != nil {
		err := grpcConn.Close()
		if err != nil {
			logutil.BgLogger().Warn("[autoid client] close grpc connection error", zap.Error(err))
		}
	}
}

// AllocSeqCache allocs sequence batch value cached in table level（rather than in alloc), the returned range covering
// the size of sequence cache with it's increment. The returned round indicates the sequence cycle times if it is with
// cycle option.
func (*singlePointAlloc) AllocSeqCache() (a int64, b int64, c int64, err error) {
	return 0, 0, 0, errors.New("AllocSeqCache not implemented")
}

// Rebase rebases the autoID base for table with tableID and the new base value.
// If allocIDs is true, it will allocate some IDs and save to the cache.
// If allocIDs is false, it will not allocate IDs.
func (sp *singlePointAlloc) Rebase(ctx context.Context, newBase int64, _ bool) error {
	r, ctx := tracing.StartRegionEx(ctx, "autoid.Rebase")
	defer r.End()

	start := time.Now()
	err := sp.rebase(ctx, newBase, false)
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(start).Seconds())
	return err
}

func (sp *singlePointAlloc) rebase(ctx context.Context, newBase int64, force bool) error {
retry:
	cli, err := sp.GetClient(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	var resp *autoid.RebaseResponse
	resp, err = cli.Rebase(ctx, &autoid.RebaseRequest{
		DbID:       sp.dbID,
		TblID:      sp.tblID,
		Base:       newBase,
		Force:      force,
		IsUnsigned: sp.isUnsigned,
	})
	if err != nil {
		if strings.Contains(err.Error(), "rpc error") {
			time.Sleep(backoffDuration)
			sp.resetConn(err)
			goto retry
		}
		return errors.Trace(err)
	}
	if len(resp.Errmsg) != 0 {
		return errors.Trace(errors.New(string(resp.Errmsg)))
	}
	sp.lastAllocated = newBase
	return nil
}

// ForceRebase set the next global auto ID to newBase.
func (sp *singlePointAlloc) ForceRebase(newBase int64) error {
	if newBase == -1 {
		return ErrAutoincReadFailed.GenWithStack("Cannot force rebase the next global ID to '0'")
	}
	return sp.rebase(context.Background(), newBase, true)
}

// RebaseSeq rebases the sequence value in number axis with tableID and the new base value.
func (*singlePointAlloc) RebaseSeq(_ int64) (int64, bool, error) {
	return 0, false, errors.New("RebaseSeq not implemented")
}

// Base return the current base of Allocator.
func (sp *singlePointAlloc) Base() int64 {
	return sp.lastAllocated
}

// End is only used for test.
func (sp *singlePointAlloc) End() int64 {
	return sp.lastAllocated
}

// NextGlobalAutoID returns the next global autoID.
// Used by 'show create table', 'alter table auto_increment = xxx'
func (sp *singlePointAlloc) NextGlobalAutoID() (int64, error) {
	_, max, err := sp.Alloc(context.Background(), 0, 1, 1)
	return max + 1, err
}

func (*singlePointAlloc) GetType() AllocatorType {
	return AutoIncrementType
}

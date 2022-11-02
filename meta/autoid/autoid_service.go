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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/metrics"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
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

	resp, err := d.etcdCli.Get(ctx, autoIDLeaderPath, clientv3.WithFirstCreate()...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("autoid service leader not found")
	}

	addr := string(resp.Kvs[0].Value)
	grpcConn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli = autoid.NewAutoIDAllocClient(grpcConn)
	d.mu.Lock()
	d.mu.AutoIDAllocClient = cli
	d.mu.Unlock()
	return cli, nil
}

// Alloc allocs N consecutive autoID for table with tableID, returning (min, max] of the allocated autoID batch.
// The consecutive feature is used to insert multiple rows in a statement.
// increment & offset is used to validate the start position (the allocator's base is not always the last allocated id).
// The returned range is (min, max]:
// case increment=1 & offset=1: you can derive the ids like min+1, min+2... max.
// case increment=x & offset=y: you firstly need to seek to firstID by `SeekToFirstAutoIDXXX`, then derive the IDs like firstID, firstID + increment * 2... in the caller.
func (sp *singlePointAlloc) Alloc(ctx context.Context, n uint64, increment, offset int64) (int64, int64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("autoid.Alloc", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

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
	if err != nil {
		if strings.Contains(err.Error(), "rpc error") {
			sp.mu.Lock()
			sp.mu.AutoIDAllocClient = nil
			sp.mu.Unlock()
			goto retry
		}
		return 0, 0, errors.Trace(err)
	}

	du := time.Since(start)
	metrics.AutoIDReqDuration.Observe(du.Seconds())
	if err == nil {
		sp.lastAllocated = resp.Min
	}
	return resp.Min, resp.Max, err
}

// AllocSeqCache allocs sequence batch value cached in table level（rather than in alloc), the returned range covering
// the size of sequence cache with it's increment. The returned round indicates the sequence cycle times if it is with
// cycle option.
func (sp *singlePointAlloc) AllocSeqCache() (min int64, max int64, round int64, err error) {
	return 0, 0, 0, errors.New("AllocSeqCache not implemented")
}

// Rebase rebases the autoID base for table with tableID and the new base value.
// If allocIDs is true, it will allocate some IDs and save to the cache.
// If allocIDs is false, it will not allocate IDs.
func (sp *singlePointAlloc) Rebase(ctx context.Context, newBase int64, allocIDs bool) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("autoid.Rebase", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	return sp.rebase(ctx, newBase, allocIDs, false)
}

func (sp *singlePointAlloc) rebase(ctx context.Context, newBase int64, allocIDs bool, force bool) error {
	cli, err := sp.GetClient(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = cli.Rebase(ctx, &autoid.RebaseRequest{
		DbID:       sp.dbID,
		TblID:      sp.tblID,
		Base:       newBase,
		Force:      force,
		IsUnsigned: sp.isUnsigned,
	})
	if err == nil {
		sp.lastAllocated = newBase
	}
	return err
}

// ForceRebase set the next global auto ID to newBase.
func (sp *singlePointAlloc) ForceRebase(newBase int64) error {
	if newBase == -1 {
		return ErrAutoincReadFailed.GenWithStack("Cannot force rebase the next global ID to '0'")
	}
	return sp.rebase(context.Background(), newBase, false, true)
}

// RebaseSeq rebases the sequence value in number axis with tableID and the new base value.
func (sp *singlePointAlloc) RebaseSeq(newBase int64) (int64, bool, error) {
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
	// return sp.lastAllocated + 1, nil
	_, max, err := sp.Alloc(context.Background(), 0, 1, 1)
	return max + 1, err
}

func (sp *singlePointAlloc) GetType() AllocatorType {
	return RowIDAllocType
}

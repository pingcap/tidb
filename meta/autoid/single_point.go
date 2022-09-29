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
	// "fmt"
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/metrics"
)

var _ Allocator = &singlePointAlloc{}

type singlePointAlloc struct {
	dbID int64
	tblID int64
	pdpb.PDClient

	lastAllocated int64
}


// Alloc allocs N consecutive autoID for table with tableID, returning (min, max] of the allocated autoID batch.
// It gets a batch of autoIDs at a time. So it does not need to access storage for each call.
// The consecutive feature is used to insert multiple rows in a statement.
// increment & offset is used to validate the start position (the allocator's base is not always the last allocated id).
// The returned range is (min, max]:
// case increment=1 & offset=1: you can derive the ids like min+1, min+2... max.
// case increment=x & offset=y: you firstly need to seek to firstID by `SeekToFirstAutoIDXXX`, then derive the IDs like firstID, firstID + increment * 2... in the caller.
func (sp *singlePointAlloc) Alloc(ctx context.Context, n uint64, increment, offset int64) (int64, int64, error) {
	if increment != 1 || offset != 1 || n != 1 {
		panic("increment and offset is not implemented!")
	}

	start := time.Now()
	resp, err := sp.PDClient.AllocAutoID(ctx, &pdpb.AutoIDRequest{
		DbID: sp.dbID,
		TblID: sp.tblID,
	})
	du := time.Since(start)
	metrics.AutoIDReqDuration.Observe(du.Seconds())
	// fmt.Println("du == ", du)

	if err == nil {
		sp.lastAllocated = resp.Id
	}
	return resp.Id, resp.Id, err
}

// AllocSeqCache allocs sequence batch value cached in table level（rather than in alloc), the returned range covering
// the size of sequence cache with it's increment. The returned round indicates the sequence cycle times if it is with
// cycle option.
func (sp *singlePointAlloc) AllocSeqCache() (min int64, max int64, round int64, err error) {
	panic("not implement AllocSeqCache")
}

// Rebase rebases the autoID base for table with tableID and the new base value.
// If allocIDs is true, it will allocate some IDs and save to the cache.
// If allocIDs is false, it will not allocate IDs.
func (sp *singlePointAlloc) Rebase(ctx context.Context, newBase int64, allocIDs bool) error {
	panic("not implement Rebase")
}

// ForceRebase set the next global auto ID to newBase.
func (sp *singlePointAlloc) ForceRebase(newBase int64) error {
	panic("not implement ForceRebase")
}

// RebaseSeq rebases the sequence value in number axis with tableID and the new base value.
func (sp *singlePointAlloc) RebaseSeq(newBase int64) (int64, bool, error) {
	panic("not implement RebaseSeq")
}

// Base return the current base of Allocator.
func (sp *singlePointAlloc) Base() int64 {
	return sp.lastAllocated
}
// End is only used for test.
func (sp *singlePointAlloc) End() int64 {
	panic("not implement end")
}

// NextGlobalAutoID returns the next global autoID.
func (sp *singlePointAlloc) NextGlobalAutoID() (int64, error) {
	panic("not implement NextGlobalAutoID")
}

func (sp *singlePointAlloc) GetType() AllocatorType {
	return RowIDAllocType
}

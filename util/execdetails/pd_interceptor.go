// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package execdetails

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/v4/client"
)

var (
	_ pd.Client   = &InterceptedPDClient{}
	_ pd.TSFuture = &interceptedTsFuture{}
)

func recordPDWaitTime(ctx context.Context, start time.Time) {
	stmtExec := ctx.Value(StmtExecDetailKey)
	if stmtExec != nil {
		detail := stmtExec.(*StmtExecDetails)
		atomic.AddInt64(&detail.WaitPDRespDuration, int64(time.Since(start)))
	}
}

// InterceptedPDClient is a PD's wrapper client to record stmt detail.
type InterceptedPDClient struct {
	pd.Client
}

// interceptedTsFuture is a PD's wrapper future to record stmt detail.
type interceptedTsFuture struct {
	pd.TSFuture
	ctx context.Context
}

// Wait implements pd.Client#Wait.
func (m interceptedTsFuture) Wait() (int64, int64, error) {
	start := time.Now()
	physical, logical, err := m.TSFuture.Wait()
	recordPDWaitTime(m.ctx, start)
	return physical, logical, err
}

// GetTS implements pd.Client#GetTS.
func (m InterceptedPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	start := time.Now()
	physical, logical, err := m.Client.GetTS(ctx)
	recordPDWaitTime(ctx, start)
	return physical, logical, err
}

// GetTSAsync implements pd.Client#GetTSAsync.
func (m InterceptedPDClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	start := time.Now()
	f := m.Client.GetTSAsync(ctx)
	recordPDWaitTime(ctx, start)
	return interceptedTsFuture{
		ctx:      ctx,
		TSFuture: f,
	}
}

// GetRegion implements pd.Client#GetRegion.
func (m InterceptedPDClient) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	start := time.Now()
	r, err := m.Client.GetRegion(ctx, key)
	recordPDWaitTime(ctx, start)
	return r, err
}

// GetPrevRegion implements pd.Client#GetPrevRegion.
func (m InterceptedPDClient) GetPrevRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	start := time.Now()
	r, err := m.Client.GetRegion(ctx, key)
	recordPDWaitTime(ctx, start)
	return r, err
}

// GetRegionByID implements pd.Client#GetRegionByID.
func (m InterceptedPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	start := time.Now()
	r, err := m.Client.GetRegionByID(ctx, regionID)
	recordPDWaitTime(ctx, start)
	return r, err
}

// ScanRegions implements pd.Client#ScanRegions.
func (m InterceptedPDClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*metapb.Region, []*metapb.Peer, error) {
	start := time.Now()
	r, p, err := m.Client.ScanRegions(ctx, key, endKey, limit)
	recordPDWaitTime(ctx, start)
	return r, p, err
}

// GetStore implements pd.Client#GetStore.
func (m InterceptedPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	start := time.Now()
	s, err := m.Client.GetStore(ctx, storeID)
	recordPDWaitTime(ctx, start)
	return s, err
}

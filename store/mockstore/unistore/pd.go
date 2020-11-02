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

package unistore

import (
	"errors"
	"math"
	"sync"

	us "github.com/ngaut/unistore/tikv"
	"github.com/pingcap/kvproto/pkg/pdpb"
	pd "github.com/tikv/pd/client"
	"golang.org/x/net/context"
)

var _ pd.Client = new(pdClient)

type pdClient struct {
	*us.MockPD

	serviceSafePoints map[string]uint64
	gcSafePointMu     sync.Mutex
}

func newPDClient(pd *us.MockPD) *pdClient {
	return &pdClient{
		MockPD:            pd,
		serviceSafePoints: make(map[string]uint64),
	}
}

func (c *pdClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

type mockTSFuture struct {
	pdc  *pdClient
	ctx  context.Context
	used bool
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	if m.used {
		return 0, 0, errors.New("cannot wait tso twice")
	}
	m.used = true
	return m.pdc.GetTS(m.ctx)
}

func (c *pdClient) GetLeaderAddr() string { return "mockpd" }

func (c *pdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	c.gcSafePointMu.Lock()
	defer c.gcSafePointMu.Unlock()

	if ttl == 0 {
		delete(c.serviceSafePoints, serviceID)
	} else {
		var minSafePoint uint64 = math.MaxUint64
		for _, ssp := range c.serviceSafePoints {
			if ssp < minSafePoint {
				minSafePoint = ssp
			}
		}

		if len(c.serviceSafePoints) == 0 || minSafePoint <= safePoint {
			c.serviceSafePoints[serviceID] = safePoint
		}
	}

	// The minSafePoint may have changed. Reload it.
	var minSafePoint uint64 = math.MaxUint64
	for _, ssp := range c.serviceSafePoints {
		if ssp < minSafePoint {
			minSafePoint = ssp
		}
	}
	return minSafePoint, nil
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

func (c *pdClient) ScatterRegionWithOption(ctx context.Context, regionID uint64, opts ...pd.ScatterRegionOption) error {
	return nil
}

func (c *pdClient) GetMemberInfo(ctx context.Context) ([]*pdpb.Member, error) {
	return nil, nil
}

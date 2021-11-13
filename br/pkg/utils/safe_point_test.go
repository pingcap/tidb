// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/testleak"
	pd "github.com/tikv/pd/client"
)

var _ = Suite(&testSafePointSuite{})

type testSafePointSuite struct{}

func (s *testSafePointSuite) SetUpSuite(c *C) {}

func (s *testSafePointSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testSafePointSuite) TestCheckGCSafepoint(c *C) {
	ctx := context.Background()
	pdClient := &mockSafePoint{safepoint: 2333}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333+1)
		c.Assert(err, IsNil)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333)
		c.Assert(err, NotNil)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333-1)
		c.Assert(err, NotNil)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 0)
		c.Assert(err, ErrorMatches, ".*GC safepoint 2333 exceed TS 0.*")
	}
}

type mockSafePoint struct {
	sync.Mutex
	pd.Client
	safepoint           uint64
	minServiceSafepoint uint64
}

func (m *mockSafePoint) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	if m.safepoint > safePoint {
		return m.safepoint, nil
	}
	if m.minServiceSafepoint == 0 || m.minServiceSafepoint > safePoint {
		m.minServiceSafepoint = safePoint
	}
	return m.minServiceSafepoint, nil
}

func (m *mockSafePoint) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	if m.safepoint < safePoint && safePoint < m.minServiceSafepoint {
		m.safepoint = safePoint
	}
	return m.safepoint, nil
}

func (s *testSafePointSuite) TestStartServiceSafePointKeeper(c *C) {
	pdClient := &mockSafePoint{safepoint: 2333}

	cases := []struct {
		sp utils.BRServiceSafePoint
		ok bool
	}{
		{
			utils.BRServiceSafePoint{
				ID:       "br",
				TTL:      10,
				BackupTS: 2333 + 1,
			},
			true,
		},

		// Invalid TTL.
		{
			utils.BRServiceSafePoint{
				ID:       "br",
				TTL:      0,
				BackupTS: 2333 + 1,
			}, false,
		},

		// Invalid ID.
		{
			utils.BRServiceSafePoint{
				ID:       "",
				TTL:      0,
				BackupTS: 2333 + 1,
			},
			false,
		},

		// BackupTS is too small.
		{
			utils.BRServiceSafePoint{
				ID:       "br",
				TTL:      10,
				BackupTS: 2333,
			}, false,
		},
		{
			utils.BRServiceSafePoint{
				ID:       "br",
				TTL:      10,
				BackupTS: 2333 - 1,
			},
			false,
		},
	}
	for i, cs := range cases {
		ctx, cancel := context.WithCancel(context.Background())
		err := utils.StartServiceSafePointKeeper(ctx, pdClient, cs.sp)
		checker := IsNil
		if !cs.ok {
			checker = NotNil
		}
		c.Assert(err, checker, Commentf("case #%d, %v", i, cs))
		cancel()
	}
}

// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestCheckGCSafepoint(t *testing.T) {
	ctx := context.Background()
	pdClient := &mockSafePoint{safepoint: 2333, services: make(map[string]uint64)}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333+1)
		require.NoError(t, err)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333)
		require.Error(t, err)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 2333-1)
		require.Error(t, err)
	}
	{
		err := utils.CheckGCSafePoint(ctx, pdClient, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "GC safepoint 2333 exceed TS 0")
	}
}

func TestCheckUpdateServiceSafepoint(t *testing.T) {
	ctx := context.Background()
	pdClient := &mockSafePoint{safepoint: 2333, services: make(map[string]uint64)}
	{
		// nothing happened, because current safepoint is large than servicee safepoint.
		err := utils.UpdateServiceSafePoint(ctx, pdClient, utils.BRServiceSafePoint{
			"BR_SERVICE",
			1,
			1,
		})
		require.NoError(t, err)
		curSafePoint, err := pdClient.UpdateGCSafePoint(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(2333), curSafePoint)
	}
	{
		// register br service safepoint
		err := utils.UpdateServiceSafePoint(ctx, pdClient, utils.BRServiceSafePoint{
			"BR_SERVICE",
			1,
			2334,
		})
		require.NoError(t, err)
		curSafePoint, find := pdClient.GetServiceSafePoint("BR_SERVICE")
		// update with new safepoint - 1.
		require.Equal(t, uint64(2333), curSafePoint)
		require.True(t, find)
	}
	{
		// remove br service safepoint
		err := utils.UpdateServiceSafePoint(ctx, pdClient, utils.BRServiceSafePoint{
			"BR_SERVICE",
			0,
			math.MaxUint64,
		})
		require.NoError(t, err)
		_, find := pdClient.GetServiceSafePoint("BR_SERVICE")
		require.False(t, find)
	}
}

type mockSafePoint struct {
	sync.Mutex
	pd.Client
	services            map[string]uint64
	safepoint           uint64
	minServiceSafepoint uint64
}

func (m *mockSafePoint) GetServiceSafePoint(serviceID string) (uint64, bool) {
	m.Lock()
	defer m.Unlock()
	safepoint, ok := m.services[serviceID]
	return safepoint, ok
}

func (m *mockSafePoint) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	if ttl <= 0 {
		delete(m.services, serviceID)
		return 0, nil
	}

	if m.safepoint > safePoint {
		return m.safepoint, nil
	}
	if m.minServiceSafepoint == 0 || m.minServiceSafepoint > safePoint {
		m.minServiceSafepoint = safePoint
	}
	m.services[serviceID] = safePoint
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

func TestStartServiceSafePointKeeper(t *testing.T) {
	pdClient := &mockSafePoint{safepoint: 2333, services: make(map[string]uint64)}

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
		if cs.ok {
			require.NoErrorf(t, err, "case #%d, %v", i, cs)
		} else {
			require.Errorf(t, err, "case #%d, %v", i, cs)
		}
		cancel()
	}
}

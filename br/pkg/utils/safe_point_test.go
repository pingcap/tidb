// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils_test

import (
	"context"
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

// mockGCManager implements utils.GCSafePointManager for testing
type mockGCManager struct {
	pdClient *mockSafePoint
}

func (m *mockGCManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	return m.pdClient.UpdateGCSafePoint(ctx, 0)
}

func (m *mockGCManager) SetServiceSafePoint(ctx context.Context, sp utils.BRServiceSafePoint) error {
	_, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, sp.TTL, sp.BackupTS-1)
	return err
}

func (m *mockGCManager) DeleteServiceSafePoint(ctx context.Context, sp utils.BRServiceSafePoint) error {
	_, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, 0, 0)
	return err
}

func TestStartServiceSafePointKeeper(t *testing.T) {
	pdClient := &mockSafePoint{safepoint: 2333, services: make(map[string]uint64)}
	mgr := &mockGCManager{pdClient: pdClient}

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
		err := utils.StartServiceSafePointKeeperInner(ctx, cs.sp, mgr)
		if cs.ok {
			require.NoErrorf(t, err, "case #%d, %v", i, cs)
		} else {
			require.Errorf(t, err, "case #%d, %v", i, cs)
		}
		cancel()
	}
}

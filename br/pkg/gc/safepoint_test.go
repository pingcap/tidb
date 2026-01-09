// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gc_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tidb/br/pkg/gc"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestCheckGCSafepoint(t *testing.T) {
	ctx := context.Background()
	pdClient := &mockSafePoint{safepoint: 2333, services: make(map[string]uint64)}
	{
		err := gc.CheckGCSafePoint(ctx, pdClient, nil, 2333+1)
		require.NoError(t, err)
	}
	{
		err := gc.CheckGCSafePoint(ctx, pdClient, nil, 2333)
		require.Error(t, err)
	}
	{
		err := gc.CheckGCSafePoint(ctx, pdClient, nil, 2333-1)
		require.Error(t, err)
	}
	{
		err := gc.CheckGCSafePoint(ctx, pdClient, nil, 0)
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

// mockGCManager implements gc.Manager for testing
type mockGCManager struct {
	pdClient *mockSafePoint
}

func (m *mockGCManager) GetGCSafePoint(ctx context.Context) (uint64, error) {
	return m.pdClient.UpdateGCSafePoint(ctx, 0)
}

func (m *mockGCManager) SetServiceSafePoint(ctx context.Context, sp gc.BRServiceSafePoint) error {
	_, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, sp.TTL, sp.BackupTS-1)
	return err
}

func (m *mockGCManager) DeleteServiceSafePoint(ctx context.Context, sp gc.BRServiceSafePoint) error {
	_, err := m.pdClient.UpdateServiceGCSafePoint(ctx, sp.ID, 0, 0)
	return err
}

func TestStartKeeperWithManager(t *testing.T) {
	pdClient := &mockSafePoint{safepoint: 2333, services: make(map[string]uint64)}
	mgr := &mockGCManager{pdClient: pdClient}

	cases := []struct {
		sp gc.BRServiceSafePoint
		ok bool
	}{
		{
			gc.BRServiceSafePoint{
				ID:       "br",
				TTL:      10,
				BackupTS: 2333 + 1,
			},
			true,
		},

		// Invalid TTL.
		{
			gc.BRServiceSafePoint{
				ID:       "br",
				TTL:      0,
				BackupTS: 2333 + 1,
			}, false,
		},

		// Invalid ID.
		{
			gc.BRServiceSafePoint{
				ID:       "",
				TTL:      0,
				BackupTS: 2333 + 1,
			},
			false,
		},

		// BackupTS is too small.
		{
			gc.BRServiceSafePoint{
				ID:       "br",
				TTL:      10,
				BackupTS: 2333,
			}, false,
		},
		{
			gc.BRServiceSafePoint{
				ID:       "br",
				TTL:      10,
				BackupTS: 2333 - 1,
			},
			false,
		},
	}
	for i, cs := range cases {
		ctx, cancel := context.WithCancel(context.Background())
		err := gc.StartKeeperWithManager(ctx, cs.sp, mgr)
		if cs.ok {
			require.NoErrorf(t, err, "case #%d, %v", i, cs)
		} else {
			require.Errorf(t, err, "case #%d, %v", i, cs)
		}
		cancel()
	}
}

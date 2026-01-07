// Copyright 2026 PingCAP, Inc.
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

package tikv

import (
	"context"

	pdgc "github.com/tikv/pd/client/clients/gc"
)

// GCStatesManagerForTest is an exported wrapper of gcStatesManagerSimulator
// for use in external package tests (e.g., BR tests).
//
// This wrapper provides a complete, validated mock implementation of PD GC APIs,
// including proper parameter validation and business logic that matches real PD behavior.
type GCStatesManagerForTest struct {
	inner *gcStatesManagerSimulator
}

// NewGCStatesManagerForTest creates a new test GC states manager.
// This manager includes complete validation logic for GC barriers and safe points,
// making it suitable for testing code that depends on PD GC behavior.
func NewGCStatesManagerForTest() *GCStatesManagerForTest {
	return &GCStatesManagerForTest{
		inner: newGCStatesManager(),
	}
}

// GetGCStatesClient returns a GC states client for the specified keyspace.
// This client provides keyspace-specific GC barrier operations (SetGCBarrier, DeleteGCBarrier, GetGCState).
func (m *GCStatesManagerForTest) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return m.inner.GetGCStatesClient(keyspaceID)
}

// UpdateServiceGCSafePoint wraps the legacy UpdateServiceGCSafePoint API
// for unified (global) GC manager testing.
//
// This is the deprecated API that uses global service safe points instead of
// per-keyspace GC barriers. It's maintained for backward compatibility testing.
func (m *GCStatesManagerForTest) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return m.inner.UpdateServiceGCSafePoint(ctx, serviceID, ttl, safePoint)
}

// UpdateGCSafePoint returns the current GC safe point for the default (null) keyspace.
// This is used by CheckGCSafePoint to verify that backup timestamps haven't been GC'd.
func (m *GCStatesManagerForTest) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	m.inner.mu.Lock()
	defer m.inner.mu.Unlock()
	return m.inner.getGCState(0).gcSafePoint, nil
}

// GetGCInternalController returns an internal controller for the specified keyspace.
// This controller provides GC advancement operations (AdvanceTxnSafePoint, AdvanceGCSafePoint).
//
// This is primarily used for simulating GC progression in tests.
func (m *GCStatesManagerForTest) GetGCInternalController(keyspaceID uint32) pdgc.InternalController {
	return m.inner.GetGCInternalController(keyspaceID)
}

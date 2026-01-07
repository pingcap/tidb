// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"

	pd "github.com/tikv/pd/client"
)

// unifiedGCManager implements GCSafePointManager using the global GC safepoint mechanism.
// It uses the deprecated pd.Client.UpdateServiceGCSafePoint API for backward compatibility.
//
// This implementation is used when --keyspace-name parameter is NOT provided.
type unifiedGCManager struct {
	pdClient pd.Client
}

// Ensure unifiedGCManager implements GCSafePointManager interface.
var _ GCSafePointManager = (*unifiedGCManager)(nil)

// newUnifiedGCManager creates a new UnifiedGCManager instance.
func newUnifiedGCManager(pdClient pd.Client) *unifiedGCManager {
	return &unifiedGCManager{
		pdClient: pdClient,
	}
}

// UpdateServiceSafePoint updates the global service safe point using the deprecated API.
// This maintains backward compatibility with existing BR behavior.
func (m *unifiedGCManager) UpdateServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error {
	// Reuse existing UpdateServiceSafePoint implementation
	return UpdateServiceSafePoint(ctx, m.pdClient, sp)
}

// StartServiceSafePointKeeper starts a goroutine to periodically update the global service safe point.
// This maintains backward compatibility with existing BR behavior.
func (m *unifiedGCManager) StartServiceSafePointKeeper(ctx context.Context, sp BRServiceSafePoint) error {
	// Reuse existing StartServiceSafePointKeeper implementation
	return StartServiceSafePointKeeper(ctx, m.pdClient, sp)
}

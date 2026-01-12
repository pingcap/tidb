// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package gc

import (
	"context"

	pd "github.com/tikv/pd/client"
	tikv "github.com/tikv/client-go/v2/tikv"
)

// Manager abstracts GC operations, supporting both global and keyspace-level GC.
type Manager interface {
	// GetGCSafePoint returns the current GC safe point.
	GetGCSafePoint(ctx context.Context) (uint64, error)

	// SetServiceSafePoint sets the service safe point with TTL.
	// If TTL <= 0, it removes the service safe point.
	SetServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error

	// DeleteServiceSafePoint removes the service safe point.
	DeleteServiceSafePoint(ctx context.Context, sp BRServiceSafePoint) error
}

// NewManager creates a GC Manager.
// Pass keyspaceID = tikv.NullspaceID for global mode, or actual keyspaceID for keyspace mode.
func NewManager(pdClient pd.Client, keyspaceID uint32) Manager {
	if keyspaceID == uint32(tikv.NullspaceID) {
		return newGlobalManager(pdClient)
	}
	return newKeyspaceManager(pdClient, keyspaceID)
}

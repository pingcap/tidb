// Copyright 2025 PingCAP, Inc.
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

package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestTableGroupLocationResolver_Basic(t *testing.T) {
	cfg := TableGroupLocationResolverConfig{
		LocalAddr:        "127.0.0.1:4000",
		RefreshInterval:  1 * time.Second,
		InfoSchemaGetter: nil,
	}

	resolver := NewTableGroupLocationResolver(cfg)
	require.NotNil(t, resolver)

	// Test default behavior - should return local address
	addr := resolver.ResolveTableLocation(100)
	require.Equal(t, "127.0.0.1:4000", addr)

	addr = resolver.ResolvePartitionLocation(100, 200)
	require.Equal(t, "127.0.0.1:4000", addr)

	// Test IsLocalStore
	require.True(t, resolver.IsLocalStore("127.0.0.1:4000"))
	require.True(t, resolver.IsLocalStore(""))
	require.False(t, resolver.IsLocalStore("192.168.1.100:4000"))

	// Test GetLocalStoreAddr
	require.Equal(t, "127.0.0.1:4000", resolver.GetLocalStoreAddr())
}

func TestTableGroupLocationResolver_WithStoreAddrs(t *testing.T) {
	rc := tikv.NewRegionCache(nil)
	defer rc.Close()
	rc.SetRegionCacheStore(1, "192.168.1.1:20160", "", tikvrpc.TiKV, uint64(1), nil)
	rc.SetRegionCacheStore(2, "192.168.1.2:20160", "", tikvrpc.TiKV, uint64(1), nil)
	rc.SetRegionCacheStore(3, "192.168.1.3:20160", "", tikvrpc.TiKV, uint64(1), nil)

	cfg := TableGroupLocationResolverConfig{
		LocalAddr:   "127.0.0.1:4000",
		RegionCache: rc,
	}

	resolver := NewTableGroupLocationResolver(cfg)

	// Manually set some physical table locations for testing
	resolver.mu.Lock()
	resolver.physicalTableLocations[100] = 1 // table 100 -> store 1
	resolver.physicalTableLocations[200] = 2 // partition 200 -> store 2
	resolver.tikvToStatusAddr["192.168.1.1:20160"] = "192.168.1.1:10080"
	resolver.tikvToStatusAddr["192.168.1.2:20160"] = "192.168.1.2:10080"
	resolver.mu.Unlock()

	// Test ResolveTableLocation
	addr := resolver.ResolveTableLocation(100)
	require.Equal(t, "192.168.1.1:10080", addr)

	// Test ResolvePartitionLocation with partition ID
	addr = resolver.ResolvePartitionLocation(100, 200)
	require.Equal(t, "192.168.1.2:10080", addr)

	// Test ResolvePartitionLocation falling back to table ID
	addr = resolver.ResolvePartitionLocation(100, 0)
	require.Equal(t, "192.168.1.1:10080", addr)

	// Test unknown table - should return local
	addr = resolver.ResolveTableLocation(999)
	require.Equal(t, "127.0.0.1:4000", addr)
}

func TestTableGroupLocationResolver_StoreAddrPlaceholder(t *testing.T) {
	cfg := TableGroupLocationResolverConfig{
		LocalAddr: "127.0.0.1:4000",
	}

	resolver := NewTableGroupLocationResolver(cfg)

	// Set physical table location without store address mapping
	resolver.mu.Lock()
	resolver.physicalTableLocations[100] = 5 // store 5 not in storeAddrs
	resolver.mu.Unlock()

	// Should return placeholder with store ID
	addr := resolver.ResolveTableLocation(100)
	require.Equal(t, "store://5", addr)
}

func TestTableGroupLocationResolver_TriggerRefresh(t *testing.T) {
	cfg := TableGroupLocationResolverConfig{
		LocalAddr:        "127.0.0.1:4000",
		RefreshInterval:  1 * time.Hour,
		InfoSchemaGetter: nil,
	}

	resolver := NewTableGroupLocationResolver(cfg)

	// TriggerRefresh should not block
	resolver.TriggerRefresh()
	resolver.TriggerRefresh()

	// Verify initial state
	require.Equal(t, 0, resolver.GetLocationCount())
}

func TestTableGroupLocationResolver_GetPhysicalTableStoreID(t *testing.T) {
	cfg := TableGroupLocationResolverConfig{
		LocalAddr: "127.0.0.1:4000",
	}

	resolver := NewTableGroupLocationResolver(cfg)

	// Set physical table location
	resolver.mu.Lock()
	resolver.physicalTableLocations[100] = 3
	resolver.mu.Unlock()

	// Test GetPhysicalTableStoreID
	storeID := resolver.GetPhysicalTableStoreID(100)
	require.Equal(t, uint64(3), storeID)

	// Unknown table should return 0
	storeID = resolver.GetPhysicalTableStoreID(999)
	require.Equal(t, uint64(0), storeID)
}

func TestTableGroupLocationResolver_SetLocalAddr(t *testing.T) {
	cfg := TableGroupLocationResolverConfig{
		LocalAddr: "127.0.0.1:4000",
	}

	resolver := NewTableGroupLocationResolver(cfg)
	require.Equal(t, "127.0.0.1:4000", resolver.GetLocalStoreAddr())

	resolver.SetLocalAddr("192.168.1.50:4000")
	require.Equal(t, "192.168.1.50:4000", resolver.GetLocalStoreAddr())

	require.True(t, resolver.IsLocalStore("192.168.1.50:4000"))
	require.False(t, resolver.IsLocalStore("127.0.0.1:4000"))
}

func TestTableGroupLocationResolver_StartStop(t *testing.T) {
	cfg := TableGroupLocationResolverConfig{
		LocalAddr:        "127.0.0.1:4000",
		RefreshInterval:  100 * time.Millisecond,
		InfoSchemaGetter: nil,
	}

	resolver := NewTableGroupLocationResolver(cfg)

	// Start should work
	resolver.Start()
	require.True(t, resolver.isRunning.Load())

	// Double start should be no-op
	resolver.Start()
	require.True(t, resolver.isRunning.Load())

	// Wait a bit for refresh to happen
	time.Sleep(150 * time.Millisecond)

	// Stop should work
	resolver.Stop()
	require.False(t, resolver.isRunning.Load())

	// Double stop should be no-op
	resolver.Stop()
	require.False(t, resolver.isRunning.Load())
}

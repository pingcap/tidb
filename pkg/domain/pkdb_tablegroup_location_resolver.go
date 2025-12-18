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
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// TableGroupLocationResolver is a real implementation of LocationResolver
// that uses table group information from PD to resolve table/partition locations.
type TableGroupLocationResolver struct {
	mu          sync.RWMutex
	lifecycleMu sync.Mutex

	localAddr   string
	regionCache *tikv.RegionCache

	// physicalTableLocations maps physicalTableID -> storeID (LeaderStoreID)
	physicalTableLocations map[int64]uint64

	// affinityGroupStates stores the latest affinity group states from PD
	affinityGroupStates map[string]*pdhttp.AffinityGroupState

	// tikvToStatusAddr maps TiKV store addr to TiDB status addr.
	tikvToStatusAddr map[string]string

	// infoSchemaGetter is a function to get the current InfoSchema
	infoSchemaGetter func() infoschema.InfoSchema

	refreshChan       chan struct{}
	refreshInterval   time.Duration
	lastRefreshTime   atomic.Value
	isRunning         atomic.Bool
	refreshCtxCancel  context.CancelFunc
	refreshLoopExited chan struct{}
}

// TableGroupLocationResolverConfig contains configuration
type TableGroupLocationResolverConfig struct {
	LocalAddr        string
	RefreshInterval  time.Duration
	InfoSchemaGetter func() infoschema.InfoSchema
	RegionCache      *tikv.RegionCache
}

// NewTableGroupLocationResolver creates a new TableGroupLocationResolver
func NewTableGroupLocationResolver(cfg TableGroupLocationResolverConfig) *TableGroupLocationResolver {
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 30 * time.Second
	}

	r := &TableGroupLocationResolver{
		localAddr:              cfg.LocalAddr,
		regionCache:            cfg.RegionCache,
		physicalTableLocations: make(map[int64]uint64),
		affinityGroupStates:    make(map[string]*pdhttp.AffinityGroupState),
		tikvToStatusAddr:       make(map[string]string),
		infoSchemaGetter:       cfg.InfoSchemaGetter,
		refreshChan:            make(chan struct{}, 1),
		refreshInterval:        cfg.RefreshInterval,
	}
	r.lastRefreshTime.Store(time.Time{})
	return r
}

// Start starts the background refresh goroutine
func (r *TableGroupLocationResolver) Start() {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()

	if r.isRunning.Load() {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	r.refreshCtxCancel = cancel
	r.refreshLoopExited = make(chan struct{})
	r.isRunning.Store(true)

	go func() {
		defer close(r.refreshLoopExited)
		r.refreshLoop(ctx)
	}()

	logutil.BgLogger().Info("TableGroupLocationResolver started",
		zap.Duration("refreshInterval", r.refreshInterval))
}

// Stop stops the background refresh goroutine
func (r *TableGroupLocationResolver) Stop() {
	r.lifecycleMu.Lock()
	if !r.isRunning.Load() {
		r.lifecycleMu.Unlock()
		return
	}

	cancel := r.refreshCtxCancel
	exited := r.refreshLoopExited
	r.refreshCtxCancel = nil
	r.refreshLoopExited = nil
	r.isRunning.Store(false)
	r.lifecycleMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if exited != nil {
		<-exited
	}

	logutil.BgLogger().Info("TableGroupLocationResolver stopped")
}

// TriggerRefresh triggers an immediate refresh
func (r *TableGroupLocationResolver) TriggerRefresh() {
	select {
	case r.refreshChan <- struct{}{}:
	default:
	}
}

func (r *TableGroupLocationResolver) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(r.refreshInterval)
	defer ticker.Stop()

	r.doRefresh(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.doRefresh(ctx)
		case <-r.refreshChan:
			r.doRefresh(ctx)
		}
	}
}

func (r *TableGroupLocationResolver) doRefresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	affinityGroups, err := infosync.GetAllAffinityGroups(ctx)
	if err != nil {
		logutil.BgLogger().Warn("Failed to get affinity groups", zap.Error(err))
		return
	}
	if affinityGroups == nil {
		affinityGroups = make(map[string]*pdhttp.AffinityGroupState)
	}

	var tableGroups []*model.TableGroupInfo
	if r.infoSchemaGetter != nil {
		if is := r.infoSchemaGetter(); is != nil {
			tableGroups = is.AllTableGroups()
		}
	}

	newLocations := make(map[int64]uint64)
	for _, tgInfo := range tableGroups {
		for _, ag := range tgInfo.AffinityGroups {
			state := affinityGroups[ag.Name]
			if state == nil || state.LeaderStoreID == 0 {
				continue
			}
			for _, pt := range ag.Tables {
				newLocations[pt.PhysicalTableID] = state.LeaderStoreID
			}
		}
	}

	var tikvStatusMap map[string]string
	serverInfos, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		logutil.BgLogger().Warn("Failed to get server info", zap.Error(err))
	} else {
		tikvStatusMap = make(map[string]string, len(serverInfos))
		for _, info := range serverInfos {
			if info == nil || info.LocalTiKVAddr == "" {
				continue
			}
			tikvStatusMap[info.LocalTiKVAddr] = fmt.Sprintf("%s:%d", info.IP, info.StatusPort)
		}
	}

	r.mu.Lock()
	r.physicalTableLocations = newLocations
	r.affinityGroupStates = affinityGroups
	if tikvStatusMap != nil {
		r.tikvToStatusAddr = tikvStatusMap
	}
	r.mu.Unlock()

	r.lastRefreshTime.Store(time.Now())
	logutil.BgLogger().Debug("TableGroupLocationResolver refreshed",
		zap.Int("locationCount", len(newLocations)))
}

func (r *TableGroupLocationResolver) getStoreAddr(storeID uint64) string {
	if storeID == 0 {
		return ""
	}
	if r.regionCache != nil {
		if addr, ok := tikv.GetStoreAddrByID(r.regionCache, storeID); ok && addr != "" {
			if statusAddr, ok := r.tikvToStatusAddr[addr]; ok && statusAddr != "" {
				return statusAddr
			}
			return addr
		}
	}
	// Fall back to placeholder when cache miss.
	return "store://" + strconv.FormatUint(storeID, 10)
}

// SetLocalAddr sets the local TiDB node address
func (r *TableGroupLocationResolver) SetLocalAddr(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.localAddr = addr
}

// ResolveTableLocation returns the store address for a given table
func (r *TableGroupLocationResolver) ResolveTableLocation(tableID int64) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if storeID, ok := r.physicalTableLocations[tableID]; ok {
		return r.getStoreAddr(storeID)
	}
	return r.localAddr
}

// ResolvePartitionLocation returns the store address for a given partition
func (r *TableGroupLocationResolver) ResolvePartitionLocation(tableID int64, partitionID int64) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if partitionID != 0 {
		if storeID, ok := r.physicalTableLocations[partitionID]; ok {
			return r.getStoreAddr(storeID)
		}
	}
	if storeID, ok := r.physicalTableLocations[tableID]; ok {
		return r.getStoreAddr(storeID)
	}
	return r.localAddr
}

// ResolveKeyLocation returns the store address for a specific key
func (r *TableGroupLocationResolver) ResolveKeyLocation(tableID int64, partitionID int64, key []byte) string {
	return r.ResolvePartitionLocation(tableID, partitionID)
}

// IsLocalStore checks if the given store address is the local TiDB node
func (r *TableGroupLocationResolver) IsLocalStore(storeAddr string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return storeAddr == r.localAddr || storeAddr == ""
}

// GetLocalStoreAddr returns the address of the local TiDB node
func (r *TableGroupLocationResolver) GetLocalStoreAddr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.localAddr
}

// GetLastRefreshTime returns the last successful refresh time
func (r *TableGroupLocationResolver) GetLastRefreshTime() time.Time {
	return r.lastRefreshTime.Load().(time.Time)
}

// GetLocationCount returns the number of physical table locations in the cache
func (r *TableGroupLocationResolver) GetLocationCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.physicalTableLocations)
}

// GetPhysicalTableStoreID returns the store ID for a physical table
func (r *TableGroupLocationResolver) GetPhysicalTableStoreID(physicalTableID int64) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.physicalTableLocations[physicalTableID]
}

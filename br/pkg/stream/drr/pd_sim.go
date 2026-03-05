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

package drr

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/oracle"
)

type regionSim struct {
	id         uint64
	epoch      uint64
	storeID    uint64
	startKey   []byte
	endKey     []byte
	checkpoint uint64
}

type storeSim struct {
	id         uint64
	bootstrap  uint64
	regionByID map[uint64]*regionSim
}

// PDSim simulates PD interfaces used by the checkpoint advancer.
// Region layout is static: no split/merge in this simulator.
type PDSim struct {
	mu sync.Mutex

	taskName   string
	taskStart  uint64
	currentTS  uint64
	nextRegion uint64

	stores map[uint64]*storeSim
	// regions are always kept sorted by StartKey.
	regions []*regionSim

	taskCh             chan<- streamhelper.TaskEvent
	globalCheckpoint   uint64
	serviceGCSafePoint uint64
	// keyed by store id.
	subscribers map[uint64]map[chan *logbackup.SubscribeFlushEventResponse]struct{}
}

var _ streamhelper.Env = (*PDSim)(nil)

// NewPDSim creates a deterministic PD simulator with predefined regions.
func NewPDSim(boundaries []RegionBoundary, taskName string) (*PDSim, error) {
	if len(boundaries) == 0 {
		boundaries = []RegionBoundary{{StoreID: 1}}
	}
	if taskName == "" {
		taskName = defaultTaskName
	}
	if err := validateBoundaries(boundaries); err != nil {
		return nil, err
	}

	p := &PDSim{
		taskName:    taskName,
		stores:      make(map[uint64]*storeSim),
		subscribers: make(map[uint64]map[chan *logbackup.SubscribeFlushEventResponse]struct{}),
	}
	p.taskStart = oracle.ComposeTS(time.Now().UnixMilli(), 0)
	p.currentTS = p.taskStart

	for _, b := range boundaries {
		store, ok := p.stores[b.StoreID]
		if !ok {
			store = &storeSim{
				id:         b.StoreID,
				bootstrap:  1,
				regionByID: make(map[uint64]*regionSim),
			}
			p.stores[b.StoreID] = store
		}
		p.nextRegion++
		r := &regionSim{
			id:         p.nextRegion,
			epoch:      1,
			storeID:    b.StoreID,
			startKey:   bytes.Clone(b.StartKey),
			endKey:     bytes.Clone(b.EndKey),
			checkpoint: p.taskStart,
		}
		store.regionByID[r.id] = r
		p.regions = append(p.regions, r)
	}
	return p, nil
}

func validateBoundaries(boundaries []RegionBoundary) error {
	for i, b := range boundaries {
		if b.StoreID == 0 {
			return fmt.Errorf("region[%d] has empty store id", i)
		}
		if i == 0 {
			if len(b.StartKey) != 0 {
				return fmt.Errorf("region[0] must start from empty key for full-range scan")
			}
			continue
		}
		prev := boundaries[i-1]
		if bytes.Compare(prev.StartKey, b.StartKey) > 0 {
			return fmt.Errorf("region[%d] start key is not sorted", i)
		}
		if !bytes.Equal(prev.EndKey, b.StartKey) {
			return fmt.Errorf("region[%d] end key does not connect region[%d] start key", i-1, i)
		}
	}
	if len(boundaries[len(boundaries)-1].EndKey) != 0 {
		return fmt.Errorf("last region must end with empty key for full-range scan")
	}
	return nil
}

// AllocTSO allocates a monotonically increasing TSO.
func (p *PDSim) AllocTSO() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	physical := oracle.ExtractPhysical(p.currentTS) + 1
	p.currentTS = oracle.ComposeTS(physical, 0)
	return p.currentTS
}

// CurrentTSO returns the latest allocated TSO.
func (p *PDSim) CurrentTSO() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentTS
}

// RegionIDs returns all known region IDs in key order.
func (p *PDSim) RegionIDs() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	ids := make([]uint64, 0, len(p.regions))
	for _, r := range p.regions {
		ids = append(ids, r.id)
	}
	return ids
}

// RegionSnapshot gets one region snapshot.
func (p *PDSim) RegionSnapshot(regionID uint64) (RegionState, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r := p.findRegionByID(regionID)
	if r == nil {
		return RegionState{}, false
	}
	return toRegionState(r), true
}

// RegionSnapshotsOnStore returns region snapshots hosted on the store.
func (p *PDSim) RegionSnapshotsOnStore(storeID uint64) ([]RegionState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	store, ok := p.stores[storeID]
	if !ok {
		return nil, fmt.Errorf("store %d not found", storeID)
	}
	result := make([]RegionState, 0, len(store.regionByID))
	for _, r := range store.regionByID {
		result = append(result, toRegionState(r))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// GlobalCheckpoint returns the latest checkpoint uploaded by advancer.
func (p *PDSim) GlobalCheckpoint() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.globalCheckpoint
}

func (p *PDSim) findRegionByID(regionID uint64) *regionSim {
	for _, r := range p.regions {
		if r.id == regionID {
			return r
		}
	}
	return nil
}

func toRegionState(r *regionSim) RegionState {
	return RegionState{
		ID:         r.id,
		Epoch:      r.epoch,
		StoreID:    r.storeID,
		StartKey:   bytes.Clone(r.startKey),
		EndKey:     bytes.Clone(r.endKey),
		Checkpoint: r.checkpoint,
	}
}

func (p *PDSim) flushRegionByStore(ctx context.Context, storeID, regionID, checkpoint uint64) (RegionState, error) {
	p.mu.Lock()
	r := p.findRegionByID(regionID)
	if r == nil {
		p.mu.Unlock()
		return RegionState{}, fmt.Errorf("region %d not found", regionID)
	}
	if r.storeID != storeID {
		p.mu.Unlock()
		return RegionState{}, fmt.Errorf(
			"region %d belongs to store %d, but flush comes from store %d",
			regionID,
			r.storeID,
			storeID,
		)
	}
	if checkpoint <= r.checkpoint {
		p.mu.Unlock()
		return RegionState{}, fmt.Errorf(
			"region %d checkpoint %d must be greater than current %d",
			regionID,
			checkpoint,
			r.checkpoint,
		)
	}
	r.checkpoint = checkpoint
	if checkpoint > p.currentTS {
		p.currentTS = checkpoint
	}
	state := toRegionState(r)
	receivers := p.subscriberChannelsLocked(storeID)
	p.mu.Unlock()

	flushEvent := &logbackup.FlushEvent{
		StartKey:   codec.EncodeBytes(nil, state.StartKey),
		EndKey:     codec.EncodeBytes(nil, state.EndKey),
		Checkpoint: checkpoint,
	}
	resp := &logbackup.SubscribeFlushEventResponse{Events: []*logbackup.FlushEvent{flushEvent}}
	for _, ch := range receivers {
		select {
		case ch <- resp:
		case <-ctx.Done():
			return RegionState{}, fmt.Errorf(
				"send flush event for region %d to subscribers: %w",
				regionID, ctx.Err(),
			)
		}
	}
	return state, nil
}

func (p *PDSim) subscriberChannelsLocked(storeID uint64) []chan *logbackup.SubscribeFlushEventResponse {
	set, ok := p.subscribers[storeID]
	if !ok || len(set) == 0 {
		return nil
	}
	out := make([]chan *logbackup.SubscribeFlushEventResponse, 0, len(set))
	for ch := range set {
		out = append(out, ch)
	}
	return out
}

// Scatter reassigns regions to random stores. Regions moved to another store
// will have checkpoint reset to current global checkpoint.
func (p *PDSim) Scatter() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.stores) <= 1 {
		return nil
	}
	storeIDs := make([]uint64, 0, len(p.stores))
	for id := range p.stores {
		storeIDs = append(storeIDs, id)
	}
	sort.Slice(storeIDs, func(i, j int) bool { return storeIDs[i] < storeIDs[j] })

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	affected := make([]uint64, 0, len(p.regions))
	for _, r := range p.regions {
		nextStoreID := storeIDs[rng.Intn(len(storeIDs))]
		if nextStoreID == r.storeID {
			continue
		}
		oldStore := p.stores[r.storeID]
		newStore := p.stores[nextStoreID]
		delete(oldStore.regionByID, r.id)
		newStore.regionByID[r.id] = r

		r.storeID = nextStoreID
		r.epoch++
		r.checkpoint = p.globalCheckpoint
		affected = append(affected, r.id)
	}
	sort.Slice(affected, func(i, j int) bool { return affected[i] < affected[j] })
	return affected
}

func overlapsRange(start, end, rgStart, rgEnd []byte) bool {
	if len(end) != 0 && bytes.Compare(rgStart, end) >= 0 {
		return false
	}
	if len(rgEnd) != 0 && bytes.Compare(start, rgEnd) >= 0 {
		return false
	}
	return true
}

// RegionScan scans static region information.
func (p *PDSim) RegionScan(
	ctx context.Context,
	key, endKey []byte,
	limit int,
) ([]streamhelper.RegionWithLeader, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()

	if limit <= 0 {
		return nil, nil
	}

	result := make([]streamhelper.RegionWithLeader, 0, limit)
	for _, r := range p.regions {
		if len(result) >= limit {
			break
		}
		if !overlapsRange(key, endKey, r.startKey, r.endKey) {
			if len(endKey) != 0 && bytes.Compare(r.startKey, endKey) >= 0 {
				break
			}
			continue
		}
		result = append(result, streamhelper.RegionWithLeader{
			Region: &metapb.Region{
				Id:       r.id,
				StartKey: bytes.Clone(r.startKey),
				EndKey:   bytes.Clone(r.endKey),
				RegionEpoch: &metapb.RegionEpoch{
					Version: r.epoch,
				},
			},
			Leader: &metapb.Peer{StoreId: r.storeID},
		})
	}
	return result, nil
}

// Stores returns all simulated stores.
func (p *PDSim) Stores(ctx context.Context) ([]streamhelper.Store, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()

	stores := make([]streamhelper.Store, 0, len(p.stores))
	for _, st := range p.stores {
		stores = append(stores, streamhelper.Store{ID: st.id, BootAt: st.bootstrap})
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].ID < stores[j].ID
	})
	return stores, nil
}

// BlockGCUntil records the service GC safe point update.
func (p *PDSim) BlockGCUntil(ctx context.Context, at uint64) (uint64, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.serviceGCSafePoint > at {
		return p.serviceGCSafePoint, fmt.Errorf("minimal safe point %d is greater than target %d", p.serviceGCSafePoint, at)
	}
	p.serviceGCSafePoint = at
	return at, nil
}

func (p *PDSim) UnblockGC(ctx context.Context) error {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	p.serviceGCSafePoint = 0
	return nil
}

func (p *PDSim) FetchCurrentTS(ctx context.Context) (uint64, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentTS, nil
}

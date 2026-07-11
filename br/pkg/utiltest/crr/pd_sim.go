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

package testutil

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/utiltest/fakecluster"
	"github.com/tikv/client-go/v2/oracle"
)

// PDSim simulates PD interfaces used by the checkpoint advancer.
// Region layout is static: no split/merge in this simulator.
type PDSim struct {
	*fakecluster.Cluster

	mu sync.Mutex

	taskName          string
	taskStart         uint64
	taskCh            chan<- streamhelper.TaskEvent
	globalCheckpoint  uint64
	checkpointWaiters []chan struct{}
	rng               *deterministicRNG
}

var _ streamhelper.Env = (*PDSim)(nil)

func NewPDSimWithTestContext(boundaries []RegionBoundary, taskName string, tc *TestContext) (*PDSim, error) {
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
		Cluster:  fakecluster.New(),
		taskName: taskName,
		rng:      tc.RNG("pd-sim"),
	}
	p.taskStart = oracle.ComposeTS(defaultTaskStartPhysical+p.rng.Int63n(1<<20), 0)
	p.SetCurrentTS(p.taskStart)

	for _, b := range boundaries {
		store := p.EnsureStore(b.StoreID, 1)
		store.SupportsSub = true
		store.LegacyRegionCheckpointRPCEnabled = false
		store.FlushTaskName = "drr"

		region := fakecluster.NewRegion(
			p.AllocID(),
			b.StartKey,
			b.EndKey,
			b.StoreID,
			1,
			p.taskStart,
			false,
		)
		p.AddRegion(region, []uint64{b.StoreID})
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
	return p.Cluster.AllocTSO()
}

// CurrentTSO returns the latest allocated TSO.
func (p *PDSim) CurrentTSO() uint64 {
	return p.Cluster.CurrentTSO()
}

// RegionIDs returns all known region IDs in key order.
func (p *PDSim) RegionIDs() []uint64 {
	return p.Cluster.RegionIDs()
}

// RegionSnapshot gets one region snapshot.
func (p *PDSim) RegionSnapshot(regionID uint64) (RegionState, bool) {
	state, ok := p.Cluster.RegionSnapshot(regionID)
	if !ok {
		return RegionState{}, false
	}
	return toRegionState(state), true
}

// RegionSnapshotsOnStore returns region snapshots hosted on the store.
func (p *PDSim) RegionSnapshotsOnStore(storeID uint64) ([]RegionState, error) {
	states, err := p.Cluster.RegionSnapshotsOnStore(storeID)
	if err != nil {
		return nil, err
	}
	result := make([]RegionState, 0, len(states))
	for _, state := range states {
		result = append(result, toRegionState(state))
	}
	return result, nil
}

// GlobalCheckpoint returns the latest checkpoint uploaded by advancer.
func (p *PDSim) GlobalCheckpoint() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.globalCheckpoint
}

func toRegionState(r fakecluster.RegionState) RegionState {
	return RegionState{
		ID:         r.ID,
		Epoch:      r.Epoch,
		StoreID:    r.StoreID,
		StartKey:   bytes.Clone(r.StartKey),
		EndKey:     bytes.Clone(r.EndKey),
		Checkpoint: r.Checkpoint,
	}
}

func (p *PDSim) flushStore(ctx context.Context, storeID, checkpoint uint64) ([]RegionState, error) {
	states, err := p.Cluster.ApplyCheckpointToStore(ctx, storeID, checkpoint)
	if err != nil {
		return nil, err
	}
	result := make([]RegionState, 0, len(states))
	for _, state := range states {
		result = append(result, toRegionState(state))
	}
	return result, nil
}

// Scatter reassigns regions to random stores. Regions moved to another store
// will have checkpoint reset to current global checkpoint.
func (p *PDSim) Scatter() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	storeIDs := p.StoreIDs()
	if len(storeIDs) <= 1 {
		return nil
	}

	affected := make([]uint64, 0, len(p.Regions))
	for _, regionID := range p.RegionIDs() {
		state, ok := p.Cluster.RegionSnapshot(regionID)
		if !ok {
			continue
		}
		nextStoreID := storeIDs[p.rng.IntN(len(storeIDs))]
		if nextStoreID == state.StoreID {
			continue
		}
		p.TransferRegionTo(regionID, []uint64{nextStoreID})
		p.SetRegionLeader(regionID, nextStoreID)
		p.BumpRegionEpoch(regionID)
		p.SetRegionCheckpoint(regionID, p.globalCheckpoint)
		affected = append(affected, regionID)
	}
	slices.Sort(affected)
	return affected
}

func (p *PDSim) Stores(ctx context.Context) ([]streamhelper.Store, error) {
	return p.Cluster.Stores(ctx)
}

func (p *PDSim) BlockGCUntil(ctx context.Context, at uint64) (uint64, error) {
	return p.Cluster.BlockGCUntil(ctx, at)
}

func (p *PDSim) UnblockGC(ctx context.Context) error {
	if err := p.Cluster.UnblockGC(ctx); err != nil {
		return err
	}
	p.Mu.Lock()
	p.ServiceGCSafePoint = 0
	p.Mu.Unlock()
	return nil
}

func (p *PDSim) FetchCurrentTS(ctx context.Context) (uint64, error) {
	return p.Cluster.FetchCurrentTS(ctx)
}

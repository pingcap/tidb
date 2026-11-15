// Copyright 2019 PingCAP, Inc.
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

package backend

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
)

const maxSPFreshBuildShards = 1024

// SPFreshBuildRoutePlan is the durable build-level route plan for SPFresh
// read-index PUT chunks.
type SPFreshBuildRoutePlan struct {
	KeyspaceID      uint32   `json:"keyspace_id"`
	JobID           int64    `json:"job_id"`
	PhysicalTableID int64    `json:"physical_table_id"`
	IndexID         int64    `json:"index_id"`
	BaseSnapshotTS  uint64   `json:"base_snapshot_ts"`
	OwnerEndpoints  []string `json:"owner_endpoints"`
}

// Clone returns an independent copy of the route plan.
func (p *SPFreshBuildRoutePlan) Clone() *SPFreshBuildRoutePlan {
	if p == nil {
		return nil
	}
	clone := *p
	clone.OwnerEndpoints = append([]string(nil), p.OwnerEndpoints...)
	return &clone
}

// Validate checks that the route plan contains the complete build identity and
// endpoint set needed for deterministic routing.
func (p *SPFreshBuildRoutePlan) Validate() error {
	if p == nil {
		return errors.New("SPFresh build route plan is not set")
	}
	if p.JobID == 0 {
		return errors.New("SPFresh build route plan job ID is not set")
	}
	if p.PhysicalTableID == 0 {
		return errors.New("SPFresh build route plan physical table ID is not set")
	}
	if p.IndexID == 0 {
		return errors.New("SPFresh build route plan index ID is not set")
	}
	if p.BaseSnapshotTS == 0 {
		return errors.New("SPFresh build route plan base snapshot ts is not set")
	}
	if len(p.OwnerEndpoints) == 0 {
		return errors.New("SPFresh build route plan owner endpoints are not set")
	}
	if len(p.OwnerEndpoints) > maxSPFreshBuildShards {
		return errors.Errorf("SPFresh build route plan has %d owner endpoints, max %d", len(p.OwnerEndpoints), maxSPFreshBuildShards)
	}
	seen := make(map[string]struct{}, len(p.OwnerEndpoints))
	for _, endpoint := range p.OwnerEndpoints {
		if endpoint == "" {
			return errors.New("SPFresh build route plan contains an empty owner endpoint")
		}
		if _, ok := seen[endpoint]; ok {
			return errors.Errorf("SPFresh build route plan contains duplicate owner endpoint %s", endpoint)
		}
		seen[endpoint] = struct{}{}
	}
	return nil
}

// SPFreshIndexBuildID returns the stable SPFresh index build identifier.
func SPFreshIndexBuildID(keyspaceID uint32, jobID, physicalTableID, indexID int64) string {
	return fmt.Sprintf("%d-%d-%d-%d", keyspaceID, jobID, physicalTableID, indexID)
}

// IndexBuildID returns the stable build identifier described by the route plan.
func (p *SPFreshBuildRoutePlan) IndexBuildID() string {
	return SPFreshIndexBuildID(p.KeyspaceID, p.JobID, p.PhysicalTableID, p.IndexID)
}

// AllBuildShardOwners returns every shard owner described by the route plan.
func (p *SPFreshBuildRoutePlan) AllBuildShardOwners() ([]SPFreshBuildShardOwner, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return spfreshBuildShardOwners(p.IndexBuildID(), p.OwnerEndpoints), nil
}

// SPFreshBuildShardOwner describes an SPFresh build shard owner binding.
type SPFreshBuildShardOwner struct {
	BuildShardID string `json:"build_shard_id"`
	Owner        string `json:"owner"`
}

// SPFreshBuildRoutingConfig configures routed SPFresh build PUT chunks.
type SPFreshBuildRoutingConfig struct {
	routes  []SPFreshBuildShardOwner
	hasData atomic.Bool
}

// NewSPFreshBuildRoutingConfig builds deterministic SPFresh build shard routes.
func NewSPFreshBuildRoutingConfig(routePlan *SPFreshBuildRoutePlan) (*SPFreshBuildRoutingConfig, error) {
	routes, err := routePlan.AllBuildShardOwners()
	if err != nil {
		return nil, err
	}
	return &SPFreshBuildRoutingConfig{
		routes: routes,
	}, nil
}

// RouteForWriterSlot returns the deterministic build shard route for a 1-based
// TiDB ingest writer slot.
func (c *SPFreshBuildRoutingConfig) RouteForWriterSlot(writerSlot int) (SPFreshBuildShardOwner, error) {
	if c == nil {
		return SPFreshBuildShardOwner{}, errors.New("SPFresh build routing is not set")
	}
	if writerSlot <= 0 {
		return SPFreshBuildShardOwner{}, errors.Errorf("SPFresh build writer slot must be positive, got %d", writerSlot)
	}
	if len(c.routes) == 0 {
		return SPFreshBuildShardOwner{}, errors.New("SPFresh build route plan owner endpoints are not set")
	}
	return c.routes[(writerSlot-1)%len(c.routes)], nil
}

func spfreshBuildShardOwners(indexBuildID string, ownerEndpoints []string) []SPFreshBuildShardOwner {
	shards := make([]SPFreshBuildShardOwner, 0, len(ownerEndpoints))
	for i, endpoint := range ownerEndpoints {
		endpointOrdinal := int64(i + 1)
		shards = append(shards, SPFreshBuildShardOwner{
			BuildShardID: fmt.Sprintf("%s-endpoint-%d", indexBuildID, endpointOrdinal),
			Owner:        endpoint,
		})
	}
	return shards
}

// MarkHasData records a successfully accepted SPFresh build chunk.
func (c *SPFreshBuildRoutingConfig) MarkHasData() {
	if c == nil {
		return
	}
	c.hasData.Store(true)
}

// HasData reports whether the build accepted any data.
func (c *SPFreshBuildRoutingConfig) HasData() bool {
	return c != nil && c.hasData.Load()
}

// SPFreshBuildRecordWriter appends one encoded SPFresh build record directly.
type SPFreshBuildRecordWriter interface {
	AppendSPFreshBuildRecord(ctx context.Context, handle, vectorPayload, storedPayload []byte) error
}

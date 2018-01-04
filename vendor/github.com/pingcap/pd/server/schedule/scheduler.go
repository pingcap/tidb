// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	RandFollowerRegion(storeID uint64) *core.RegionInfo
	RandLeaderRegion(storeID uint64) *core.RegionInfo

	GetStores() []*core.StoreInfo
	GetStore(id uint64) *core.StoreInfo
	GetRegion(id uint64) *core.RegionInfo
	GetRegionStores(region *core.RegionInfo) []*core.StoreInfo
	GetFollowerStores(region *core.RegionInfo) []*core.StoreInfo
	GetLeaderStore(region *core.RegionInfo) *core.StoreInfo
	GetStoresAverageScore(kind core.ResourceKind) float64
	ScanRegions(startKey []byte, limit int) []*core.RegionInfo

	BlockStore(id uint64) error
	UnblockStore(id uint64)

	IsRegionHot(id uint64) bool
	RegionWriteStats() []*core.RegionStat
	RegionReadStats() []*core.RegionStat

	// get config methods
	GetOpt() NamespaceOptions
	Options

	// TODO: it should be removed. Schedulers don't need to know anything
	// about peers.
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	// GetType should in accordance with the name passing to schedule.RegisterScheduler()
	GetType() string
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster Cluster) error
	Cleanup(cluster Cluster)
	Schedule(cluster Cluster, opInfluence OpInfluence) *Operator
	IsScheduleAllowed(cluster Cluster) bool
}

// CreateSchedulerFunc is for creating scheudler.
type CreateSchedulerFunc func(limiter *Limiter, args []string) (Scheduler, error)

var schedulerMap = make(map[string]CreateSchedulerFunc)

// RegisterScheduler binds a scheduler creator. It should be called in init()
// func of a package.
func RegisterScheduler(name string, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[name]; ok {
		log.Fatalf("duplicated scheduler name: %v", name)
	}
	schedulerMap[name] = createFn
}

// CreateScheduler creates a scheduler with registered creator func.
func CreateScheduler(name string, limiter *Limiter, args ...string) (Scheduler, error) {
	fn, ok := schedulerMap[name]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", name)
	}
	return fn(limiter, args)
}

// Limiter a counter that limits the number of operators
type Limiter struct {
	sync.RWMutex
	counts map[OperatorKind]uint64
}

// NewLimiter create a schedule limiter
func NewLimiter() *Limiter {
	return &Limiter{
		counts: make(map[OperatorKind]uint64),
	}
}

// UpdateCounts updates resouce counts using current pending operators.
func (l *Limiter) UpdateCounts(operators map[uint64]*Operator) {
	l.Lock()
	defer l.Unlock()
	for k := range l.counts {
		delete(l.counts, k)
	}
	for _, op := range operators {
		l.counts[op.Kind()]++
	}
}

// OperatorCount gets the count of operators filtered by mask.
func (l *Limiter) OperatorCount(mask OperatorKind) uint64 {
	l.RLock()
	defer l.RUnlock()
	var total uint64
	for k, count := range l.counts {
		if k&mask != 0 {
			total += count
		}
	}
	return total
}

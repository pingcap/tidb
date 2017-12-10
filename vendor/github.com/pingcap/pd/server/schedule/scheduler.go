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

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
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
	IsScheduleAllowed() bool
}

// CreateSchedulerFunc is for creating scheudler.
type CreateSchedulerFunc func(opt Options, limiter *Limiter, args []string) (Scheduler, error)

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
func CreateScheduler(name string, opt Options, limiter *Limiter, args ...string) (Scheduler, error) {
	fn, ok := schedulerMap[name]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", name)
	}
	return fn(opt, limiter, args)
}

// Limiter a counter that limits the number of operators
type Limiter struct {
	sync.RWMutex
	counts map[core.ResourceKind]uint64
}

// NewLimiter create a schedule limiter
func NewLimiter() *Limiter {
	return &Limiter{
		counts: make(map[core.ResourceKind]uint64),
	}
}

// AddOperator increase the count by kind
func (l *Limiter) AddOperator(op *Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.ResourceKind()]++
}

// RemoveOperator decrease the count by kind
func (l *Limiter) RemoveOperator(op *Operator) {
	l.Lock()
	defer l.Unlock()
	if l.counts[op.ResourceKind()] == 0 {
		log.Fatal("the limiter is already 0, no operators need to remove")
	}
	l.counts[op.ResourceKind()]--
}

// OperatorCount get the count by kind
func (l *Limiter) OperatorCount(kind core.ResourceKind) uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.counts[kind]
}

// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

// Filter is an interface to filter source and target store.
type Filter interface {
	// Return true if the store should not be used as a source store.
	FilterSource(store *core.StoreInfo) bool
	// Return true if the store should not be used as a target store.
	FilterTarget(store *core.StoreInfo) bool
}

// FilterSource checks if store can pass all Filters as source store.
func FilterSource(store *core.StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterSource(store) {
			return true
		}
	}
	return false
}

// FilterTarget checks if store can pass all Filters as target store.
func FilterTarget(store *core.StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterTarget(store) {
			return true
		}
	}
	return false
}

type excludedFilter struct {
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified stores.
func NewExcludedFilter(sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) FilterSource(store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetId()]
	return ok
}

func (f *excludedFilter) FilterTarget(store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetId()]
	return ok
}

type blockFilter struct{}

// NewBlockFilter creates a Filter that filters all stores that are blocked from balance.
func NewBlockFilter() Filter {
	return &blockFilter{}
}

func (f *blockFilter) FilterSource(store *core.StoreInfo) bool {
	return store.IsBlocked()
}

func (f *blockFilter) FilterTarget(store *core.StoreInfo) bool {
	return store.IsBlocked()
}

type stateFilter struct {
	opt Options
}

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter(opt Options) Filter {
	return &stateFilter{opt: opt}
}

func (f *stateFilter) filter(store *core.StoreInfo) bool {
	return !store.IsUp()
}

func (f *stateFilter) FilterSource(store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.filter(store)
}

type healthFilter struct {
	opt Options
}

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter(opt Options) Filter {
	return &healthFilter{opt: opt}
}

func (f *healthFilter) filter(store *core.StoreInfo) bool {
	if store.Stats.GetIsBusy() {
		return true
	}
	return store.DownTime() > f.opt.GetMaxStoreDownTime()
}

func (f *healthFilter) FilterSource(store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *healthFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.filter(store)
}

type pendingPeerCountFilter struct {
	opt Options
}

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pengding peers.
func NewPendingPeerCountFilter(opt Options) Filter {
	return &pendingPeerCountFilter{opt: opt}
}

func (p *pendingPeerCountFilter) filter(store *core.StoreInfo) bool {
	return store.PendingPeerCount > int(p.opt.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) FilterSource(store *core.StoreInfo) bool {
	return p.filter(store)
}

func (p *pendingPeerCountFilter) FilterTarget(store *core.StoreInfo) bool {
	return p.filter(store)
}

type snapshotCountFilter struct {
	opt Options
}

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter(opt Options) Filter {
	return &snapshotCountFilter{opt: opt}
}

func (f *snapshotCountFilter) filter(store *core.StoreInfo) bool {
	return uint64(store.Stats.GetSendingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.Stats.GetReceivingSnapCount()) > f.opt.GetMaxSnapshotCount() ||
		uint64(store.Stats.GetApplyingSnapCount()) > f.opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) FilterSource(store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *snapshotCountFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.filter(store)
}

type cacheFilter struct {
	cache *cache.TTLUint64
}

// NewCacheFilter creates a Filter that filters all stores that are in the cache.
func NewCacheFilter(cache *cache.TTLUint64) Filter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) FilterSource(store *core.StoreInfo) bool {
	return f.cache.Exists(store.GetId())
}

func (f *cacheFilter) FilterTarget(store *core.StoreInfo) bool {
	return false
}

type storageThresholdFilter struct{}

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter(opt Options) Filter {
	return &storageThresholdFilter{}
}

func (f *storageThresholdFilter) FilterSource(store *core.StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(store *core.StoreInfo) bool {
	return store.IsLowSpace()
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	labels    []string
	stores    []*core.StoreInfo
	safeScore float64
}

// NewDistinctScoreFilter creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewDistinctScoreFilter(labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetId() == source.GetId() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		labels:    labels,
		stores:    newStores,
		safeScore: DistinctScore(labels, newStores, source),
	}
}

func (f *distinctScoreFilter) FilterSource(store *core.StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(store *core.StoreInfo) bool {
	return DistinctScore(f.labels, f.stores, store) < f.safeScore
}

type namespaceFilter struct {
	classifier namespace.Classifier
	namespace  string
}

// NewNamespaceFilter creates a Filter that filters all stores that are not
// belong to a namespace.
func NewNamespaceFilter(classifier namespace.Classifier, namespace string) Filter {
	return &namespaceFilter{
		classifier: classifier,
		namespace:  namespace,
	}
}

func (f *namespaceFilter) filter(store *core.StoreInfo) bool {
	return f.classifier.GetStoreNamespace(store) != f.namespace
}

func (f *namespaceFilter) FilterSource(store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *namespaceFilter) FilterTarget(store *core.StoreInfo) bool {
	return f.filter(store)
}

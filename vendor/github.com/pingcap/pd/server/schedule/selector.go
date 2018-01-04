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
	"math/rand"

	"github.com/pingcap/pd/server/core"
)

// Selector is an interface to select source and target store to schedule.
type Selector interface {
	SelectSource(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo
	SelectTarget(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo
}

type balanceSelector struct {
	kind    core.ResourceKind
	filters []Filter
}

// NewBalanceSelector creates a Selector that select source/target store by their
// resource scores.
func NewBalanceSelector(kind core.ResourceKind, filters []Filter) Selector {
	return &balanceSelector{
		kind:    kind,
		filters: filters,
	}
}

func (s *balanceSelector) SelectSource(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var result *core.StoreInfo
	for _, store := range stores {
		if FilterSource(opt, store, filters) {
			continue
		}
		if result == nil || result.ResourceScore(s.kind) < store.ResourceScore(s.kind) {
			result = store
		}
	}
	return result
}

func (s *balanceSelector) SelectTarget(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var result *core.StoreInfo
	for _, store := range stores {
		if FilterTarget(opt, store, filters) {
			continue
		}
		if result == nil || result.ResourceScore(s.kind) > store.ResourceScore(s.kind) {
			result = store
		}
	}
	return result
}

type replicaSelector struct {
	regionStores []*core.StoreInfo
	labels       []string
	filters      []Filter
}

// NewReplicaSelector creates a Selector that select source/target store by their
// distinct scores based on a region's peer stores.
func NewReplicaSelector(regionStores []*core.StoreInfo, labels []string, filters ...Filter) Selector {
	return &replicaSelector{
		regionStores: regionStores,
		labels:       labels,
		filters:      filters,
	}
}

func (s *replicaSelector) SelectSource(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		if FilterSource(opt, store, filters) {
			continue
		}
		score := DistinctScore(s.labels, s.regionStores, store)
		if best == nil || compareStoreScore(store, score, best, bestScore) < 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || FilterSource(opt, best, s.filters) {
		return nil
	}
	return best
}

func (s *replicaSelector) SelectTarget(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	var (
		best      *core.StoreInfo
		bestScore float64
	)
	for _, store := range stores {
		if FilterTarget(opt, store, filters) {
			continue
		}
		score := DistinctScore(s.labels, s.regionStores, store)
		if best == nil || compareStoreScore(store, score, best, bestScore) > 0 {
			best, bestScore = store, score
		}
	}
	if best == nil || FilterTarget(opt, best, s.filters) {
		return nil
	}
	return best
}

type randomSelector struct {
	filters []Filter
}

// NewRandomSelector creates a selector that select store randomly.
func NewRandomSelector(filters []Filter) Selector {
	return &randomSelector{filters: filters}
}

func (s *randomSelector) Select(stores []*core.StoreInfo) *core.StoreInfo {
	if len(stores) == 0 {
		return nil
	}
	return stores[rand.Int()%len(stores)]
}

func (s *randomSelector) SelectSource(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*core.StoreInfo
	for _, store := range stores {
		if FilterSource(opt, store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}

func (s *randomSelector) SelectTarget(opt Options, stores []*core.StoreInfo, filters ...Filter) *core.StoreInfo {
	filters = append(filters, s.filters...)

	var candidates []*core.StoreInfo
	for _, store := range stores {
		if FilterTarget(opt, store, filters) {
			continue
		}
		candidates = append(candidates, store)
	}
	return s.Select(candidates)
}

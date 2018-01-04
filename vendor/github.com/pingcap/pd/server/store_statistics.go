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

package server

import (
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

type storeStatistics struct {
	opt             *scheduleOption
	namespace       string
	Up              int
	Disconnect      int
	Down            int
	Offline         int
	Tombstone       int
	LowSpace        int
	StorageSize     uint64
	StorageCapacity uint64
	RegionCount     int
	LeaderCount     int
}

func newStoreStatistics(opt *scheduleOption, namespace string) *storeStatistics {
	return &storeStatistics{
		opt:       opt,
		namespace: namespace,
	}
}

func (s *storeStatistics) Observe(store *core.StoreInfo) {
	// Store state.
	switch store.GetState() {
	case metapb.StoreState_Up:
		if store.DownTime() >= s.opt.GetMaxStoreDownTime() {
			s.Down++
		} else if store.IsDisconnected() {
			s.Disconnect++
		} else {
			s.Up++
		}
	case metapb.StoreState_Offline:
		s.Offline++
	case metapb.StoreState_Tombstone:
		s.Tombstone++
		return
	}
	if store.IsLowSpace() {
		s.LowSpace++
	}

	// Store stats.
	s.StorageSize += store.StorageSize()
	s.StorageCapacity += store.Stats.GetCapacity()
	s.RegionCount += store.RegionCount
	s.LeaderCount += store.LeaderCount

	id := strconv.FormatUint(store.GetId(), 10)
	storeStatusGauge.WithLabelValues(s.namespace, id, "region_score").Set(store.RegionScore())
	storeStatusGauge.WithLabelValues(s.namespace, id, "leader_score").Set(store.LeaderScore())
	storeStatusGauge.WithLabelValues(s.namespace, id, "region_size").Set(float64(store.RegionSize))
	storeStatusGauge.WithLabelValues(s.namespace, id, "region_count").Set(float64(store.RegionCount))
	storeStatusGauge.WithLabelValues(s.namespace, id, "leader_size").Set(float64(store.LeaderSize))
	storeStatusGauge.WithLabelValues(s.namespace, id, "leader_count").Set(float64(store.LeaderCount))
}

func (s *storeStatistics) Collect() {
	metrics := make(map[string]float64)
	metrics["store_up_count"] = float64(s.Up)
	metrics["store_disconnected_count"] = float64(s.Disconnect)
	metrics["store_down_count"] = float64(s.Down)
	metrics["store_offline_count"] = float64(s.Offline)
	metrics["store_tombstone_count"] = float64(s.Tombstone)
	metrics["store_low_space_count"] = float64(s.LowSpace)
	metrics["region_count"] = float64(s.RegionCount)
	metrics["leader_count"] = float64(s.LeaderCount)
	metrics["storage_size"] = float64(s.StorageSize)
	metrics["storage_capacity"] = float64(s.StorageCapacity)

	for label, value := range metrics {
		clusterStatusGauge.WithLabelValues(label, s.namespace).Set(value)
	}
}

type storeStatisticsMap struct {
	opt        *scheduleOption
	classifier namespace.Classifier
	stats      map[string]*storeStatistics
}

func newStoreStatisticsMap(opt *scheduleOption, classifier namespace.Classifier) *storeStatisticsMap {
	return &storeStatisticsMap{
		opt:        opt,
		classifier: classifier,
		stats:      make(map[string]*storeStatistics),
	}
}

func (m *storeStatisticsMap) Observe(store *core.StoreInfo) {
	namespace := m.classifier.GetStoreNamespace(store)
	stat, ok := m.stats[namespace]
	if !ok {
		stat = newStoreStatistics(m.opt, namespace)
		m.stats[namespace] = stat
	}
	stat.Observe(store)
}

func (m *storeStatisticsMap) Collect() {
	for _, s := range m.stats {
		s.Collect()
	}
}

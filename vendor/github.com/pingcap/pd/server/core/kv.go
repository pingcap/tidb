// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	clusterPath  = "raft"
	configPath   = "config"
	schedulePath = "schedule"
)

// KV wraps all kv operations, keep it stateless.
type KV struct {
	KVBase
}

// NewKV creates KV instance with KVBase.
func NewKV(base KVBase) *KV {
	return &KV{
		KVBase: base,
	}
}

func (kv *KV) storePath(storeID uint64) string {
	return path.Join(clusterPath, "s", fmt.Sprintf("%020d", storeID))
}

func (kv *KV) regionPath(regionID uint64) string {
	return path.Join(clusterPath, "r", fmt.Sprintf("%020d", regionID))
}

// ClusterStatePath returns the path to save an option.
func (kv *KV) ClusterStatePath(option string) string {
	return path.Join(clusterPath, "status", option)
}

func (kv *KV) storeLeaderWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "leader")
}

func (kv *KV) storeRegionWeightPath(storeID uint64) string {
	return path.Join(schedulePath, "store_weight", fmt.Sprintf("%020d", storeID), "region")
}

// LoadMeta loads cluster meta from KV store.
func (kv *KV) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return kv.loadProto(clusterPath, meta)
}

// SaveMeta save cluster meta to KV store.
func (kv *KV) SaveMeta(meta *metapb.Cluster) error {
	return kv.saveProto(clusterPath, meta)
}

// LoadStore loads one store from KV.
func (kv *KV) LoadStore(storeID uint64, store *metapb.Store) (bool, error) {
	return kv.loadProto(kv.storePath(storeID), store)
}

// SaveStore saves one store to KV.
func (kv *KV) SaveStore(store *metapb.Store) error {
	return kv.saveProto(kv.storePath(store.GetId()), store)
}

// LoadRegion loads one regoin from KV.
func (kv *KV) LoadRegion(regionID uint64, region *metapb.Region) (bool, error) {
	return kv.loadProto(kv.regionPath(regionID), region)
}

// SaveRegion saves one region to KV.
func (kv *KV) SaveRegion(region *metapb.Region) error {
	return kv.saveProto(kv.regionPath(region.GetId()), region)
}

// SaveConfig stores marshalable cfg to the configPath.
func (kv *KV) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.Save(configPath, string(value))
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (kv *KV) LoadConfig(cfg interface{}) (bool, error) {
	value, err := kv.Load(configPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

// LoadStores loads all stores from KV to StoresInfo.
func (kv *KV) LoadStores(stores *StoresInfo, rangeLimit int) error {
	nextID := uint64(0)
	endKey := kv.storePath(math.MaxUint64)
	for {
		key := kv.storePath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			return errors.Trace(err)
		}
		for _, s := range res {
			store := &metapb.Store{}
			if err := store.Unmarshal([]byte(s)); err != nil {
				return errors.Trace(err)
			}
			storeInfo := NewStoreInfo(store)
			leaderWeight, err := kv.loadFloatWithDefaultValue(kv.storeLeaderWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return errors.Trace(err)
			}
			storeInfo.LeaderWeight = leaderWeight
			regionWeight, err := kv.loadFloatWithDefaultValue(kv.storeRegionWeightPath(storeInfo.GetId()), 1.0)
			if err != nil {
				return errors.Trace(err)
			}
			storeInfo.RegionWeight = regionWeight

			nextID = store.GetId() + 1
			stores.SetStore(storeInfo)
		}
		if len(res) < rangeLimit {
			return nil
		}
	}
}

// SaveStoreWeight saves a store's leader and region weight to KV.
func (kv *KV) SaveStoreWeight(storeID uint64, leader, region float64) error {
	leaderValue := strconv.FormatFloat(leader, 'f', -1, 64)
	if err := kv.Save(kv.storeLeaderWeightPath(storeID), leaderValue); err != nil {
		return errors.Trace(err)
	}
	regionValue := strconv.FormatFloat(region, 'f', -1, 64)
	if err := kv.Save(kv.storeRegionWeightPath(storeID), regionValue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (kv *KV) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := kv.Load(path)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

// LoadRegions loads all regions from KV to RegionsInfo.
func (kv *KV) LoadRegions(regions *RegionsInfo, rangeLimit int) error {
	nextID := uint64(0)
	endKey := kv.regionPath(math.MaxUint64)

	for {
		key := kv.regionPath(nextID)
		res, err := kv.LoadRange(key, endKey, rangeLimit)
		if err != nil {
			return errors.Trace(err)
		}

		for _, s := range res {
			region := &metapb.Region{}
			if err := region.Unmarshal([]byte(s)); err != nil {
				return errors.Trace(err)
			}

			nextID = region.GetId() + 1
			regions.SetRegion(NewRegionInfo(region, nil))
		}

		if len(res) < int(rangeLimit) {
			return nil
		}
	}
}

func (kv *KV) loadProto(key string, msg proto.Message) (bool, error) {
	value, err := kv.Load(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if value == "" {
		return false, nil
	}
	return true, proto.Unmarshal([]byte(value), msg)
}

func (kv *KV) saveProto(key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.Trace(err)
	}
	return kv.Save(key, string(value))
}

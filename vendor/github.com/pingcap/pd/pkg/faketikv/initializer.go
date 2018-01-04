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

package faketikv

import (
	"context"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

type localAlloc struct {
	id uint64
}

func (l *localAlloc) AllocID() (uint64, error) {
	l.id++
	return l.id, nil
}

// Initializer defines an Init interface.
// we can implement different case to initialize cluster.
type Initializer interface {
	Init(args ...string) *ClusterInfo
}

// TiltCase will initailize cluster with all regions distributed in 3 node.
type TiltCase struct {
	NodeNumber   int `toml:"node-number" json:"node-number"`
	RegionNumber int `toml:"region-number" json:"region-number"`
	alloc        *localAlloc
}

// NewTiltCase returns tiltCase.
func NewTiltCase() *TiltCase {
	return &TiltCase{alloc: &localAlloc{}}
}

// Init implement Initializer.
func (c *TiltCase) Init(addr string, args ...string) *ClusterInfo {
	path := args[0]
	err := c.parser(path)
	if err != nil {
		log.Fatal("initalize failed: ", err)
	}
	nodes := make(map[uint64]*Node)
	regions := core.NewRegionsInfo()
	var ids []uint64
	for i := 0; i < c.NodeNumber; i++ {
		id, err1 := c.alloc.AllocID()
		if err1 != nil {
			log.Fatal("alloc failed", err1)
		}
		node, err1 := NewNode(id, fmt.Sprintf("mock://tikv-%d", id), addr)
		if err != nil {
			log.Fatal("New node failed", err1)
		}
		nodes[id] = node
		if len(ids) < 3 {
			ids = append(ids, id)
		}
	}
	var firstRegion *core.RegionInfo
	for i := 0; i < c.RegionNumber; i++ {
		start := i * 1000
		region := c.genRegion(ids, start)
		regions.SetRegion(region)
		if i == 0 {
			firstRegion = region.Clone()
			firstRegion.StartKey = []byte("")
			firstRegion.EndKey = []byte("")
			firstRegion.Peers = firstRegion.Peers[:1]
		}
	}
	// TODO: remove this
	client := nodes[firstRegion.Leader.GetStoreId()].client
	for i := 0; i < c.NodeNumber+c.RegionNumber+10; i++ {
		_, err = client.AllocID(context.Background())
		if err != nil {
			log.Fatal("initalize failed when alloc ID: ", err)
		}
	}

	cluster := &ClusterInfo{
		regions,
		nodes,
		firstRegion,
	}
	for _, n := range nodes {
		n.clusterInfo = cluster
	}
	return cluster
}

func (c *TiltCase) parser(path string) error {
	_, err := toml.DecodeFile(path, c)
	return err
}

func (c *TiltCase) genRegion(ids []uint64, start int) *core.RegionInfo {
	if len(ids) == 0 {
		return nil
	}
	regionID, _ := c.alloc.AllocID()
	peers := make([]*metapb.Peer, 0, len(ids))
	for _, storeID := range ids {
		id, err := c.alloc.AllocID()
		if err != nil {
			log.Fatal("initalize failed when alloc ID: ", err)
		}
		peer := &metapb.Peer{
			Id:      id,
			StoreId: storeID,
		}
		peers = append(peers, peer)
	}
	regionMeta := &metapb.Region{
		Id:          regionID,
		StartKey:    []byte(fmt.Sprintf("zt_%020d", start)),
		EndKey:      []byte(fmt.Sprintf("zt_%020d", start+1000)),
		Peers:       peers,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	region := core.NewRegionInfo(regionMeta, peers[0])
	region.ApproximateSize = 96 * 1000 * 1000
	return region
}

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

package mocktikv

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pd-client"
)

// Use global variables to prevent pdClients from creating duplicate timestamps.
var (
	tsMu       sync.Mutex
	physicalTS int64
	logicalTS  int64
)

type pdClient struct {
	cluster *Cluster
}

// NewPDClient creates a mock pd.Client that uses local timestamp and meta data
// from a Cluster.
func NewPDClient(cluster *Cluster) pd.Client {
	return &pdClient{
		cluster: cluster,
	}
}

func (c *pdClient) GetTS() (int64, int64, error) {
	tsMu.Lock()
	defer tsMu.Unlock()

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	if physicalTS >= ts {
		logicalTS++
	} else {
		physicalTS = ts
		logicalTS = 0
	}
	return physicalTS, logicalTS, nil
}

func (c *pdClient) GetRegion(key []byte) (*metapb.Region, error) {
	region := c.cluster.GetRegionByKey(key)
	if region == nil {
		return nil, errors.New("not found")
	}
	return region, nil
}

func (c *pdClient) GetStore(storeID uint64) (*metapb.Store, error) {
	store := c.cluster.GetStore(storeID)
	if store == nil {
		return nil, errors.New("not found")
	}
	return store, nil
}

func (c *pdClient) Close() {
}

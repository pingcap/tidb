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

package pd

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	// GetClusterID gets the cluster ID from PD.
	GetClusterID() uint64
	// GetTS gets a timestamp from PD.
	GetTS() (int64, int64, error)
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(key []byte) (*metapb.Region, *metapb.Peer, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(storeID uint64) (*metapb.Store, error)
	// Close closes the client.
	Close()
}

type client struct {
	worker *rpcWorker
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string) (Client, error) {
	log.Infof("[pd] create pd client with endpoints %v", pdAddrs)
	worker, err := newRPCWorker(pdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &client{worker: worker}, nil
}

func (c *client) Close() {
	c.worker.stop(errors.New("[pd] pd-client closing"))
}

func (c *client) GetClusterID() uint64 {
	return c.worker.clusterID
}

func (c *client) GetTS() (int64, int64, error) {
	req := &tsoRequest{
		done: make(chan error, 1),
	}

	start := time.Now()
	c.worker.requests <- req
	err := <-req.done
	requestDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds())

	return req.physical, req.logical, err
}

func (c *client) GetRegion(key []byte) (*metapb.Region, *metapb.Peer, error) {
	req := &regionRequest{
		pbReq: &pdpb.GetRegionRequest{
			RegionKey: key,
		},
		done: make(chan error, 1),
	}

	start := time.Now()
	c.worker.requests <- req
	err := <-req.done
	requestDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())

	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return req.pbResp.GetRegion(), req.pbResp.GetLeader(), nil
}

func (c *client) GetStore(storeID uint64) (*metapb.Store, error) {
	req := &storeRequest{
		pbReq: &pdpb.GetStoreRequest{
			StoreId: storeID,
		},
		done: make(chan error, 1),
	}

	start := time.Now()
	c.worker.requests <- req
	err := <-req.done
	requestDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())

	if err != nil {
		return nil, errors.Trace(err)
	}
	store := req.pbResp.GetStore()
	if store == nil {
		return nil, errors.New("[pd] store field in rpc response not set")
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return nil, nil
	}
	return store, nil
}

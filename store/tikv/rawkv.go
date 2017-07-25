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

package tikv

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	goctx "golang.org/x/net/context"
)

var (
	// MaxRawKVScanLimit is the maximum scan limit for rawkv Scan.
	MaxRawKVScanLimit = 10240
	// ErrMaxScanLimitExceeded is returned when the limit for rawkv Scan is to large.
	ErrMaxScanLimitExceeded = errors.New("limit should be less than MaxRawKVScanLimit")
	// RawKVBatchSize is the maximum keys in each mget/mput request that sent to tikv.
	RawKVBatchSize = 1024
)

// RawKVClient is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type RawKVClient struct {
	clusterID   uint64
	regionCache *RegionCache
	rpcClient   Client
}

// NewRawKVClient creates a client with PD cluster addrs.
func NewRawKVClient(pdAddrs []string) (*RawKVClient, error) {
	pdCli, err := pd.NewClient(pdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RawKVClient{
		clusterID:   pdCli.GetClusterID(goctx.TODO()),
		regionCache: NewRegionCache(pdCli),
		rpcClient:   newRPCClient(),
	}, nil
}

// ClusterID returns the TiKV cluster ID.
func (c *RawKVClient) ClusterID() uint64 {
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawGet,
		RawGet: &kvrpcpb.RawGetRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cmdResp := resp.RawGet
	if cmdResp == nil {
		return nil, errors.Trace(errBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	if len(cmdResp.Value) == 0 {
		return nil, nil
	}
	return cmdResp.Value, nil
}

// Put stores a key-value pair to TiKV.
func (c *RawKVClient) Put(key, value []byte) error {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("put").Observe(time.Since(start).Seconds()) }()
	rawkvSizeHistogram.WithLabelValues("key").Observe(float64(len(key)))
	rawkvSizeHistogram.WithLabelValues("value").Observe(float64(len(value)))

	if len(value) == 0 {
		return errors.New("empty value is not supported")
	}

	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawPut,
		RawPut: &kvrpcpb.RawPutRequest{
			Key:   key,
			Value: value,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return errors.Trace(err)
	}
	cmdResp := resp.RawPut
	if cmdResp == nil {
		return errors.Trace(errBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// Delete deletes a key-value pair from TiKV.
func (c *RawKVClient) Delete(key []byte) error {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds()) }()

	req := &tikvrpc.Request{
		Type: tikvrpc.CmdRawDelete,
		RawDelete: &kvrpcpb.RawDeleteRequest{
			Key: key,
		},
	}
	resp, _, err := c.sendReq(key, req)
	if err != nil {
		return errors.Trace(err)
	}
	cmdResp := resp.RawDelete
	if cmdResp == nil {
		return errors.Trace(errBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// Scan queries continuous kv pairs, starts from startKey, up to limit pairs.
// If you want to exclude the startKey, append a '\0' to the key: `Scan(append(startKey, '\0'), limit)`.
func (c *RawKVClient) Scan(startKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("raw_scan").Observe(time.Since(start).Seconds()) }()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit {
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdRawScan,
			RawScan: &kvrpcpb.RawScanRequest{
				StartKey: startKey,
				Limit:    uint32(limit - len(keys)),
			},
		}
		resp, loc, err := c.sendReq(startKey, req)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		cmdResp := resp.RawScan
		if cmdResp == nil {
			return nil, nil, errors.Trace(errBodyMissing)
		}
		for _, pair := range cmdResp.Kvs {
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		startKey = loc.EndKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

// MGet queries multiple values by keys.
func (c *RawKVClient) MGet(keys [][]byte) (map[string][]byte, error) {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("raw_mget").Observe(time.Since(start).Seconds()) }()

	bo := NewBackoffer(rawkvMaxBackoff, goctx.Background())

	// Create a map to collect key-values from region servers.
	var mu sync.Mutex
	m := make(map[string][]byte)
	err := c.doActionByRegions(bo, keys, c.mgetSingleRegion(func(k, v []byte) {
		if len(v) == 0 {
			return
		}
		mu.Lock()
		m[string(k)] = v
		mu.Unlock()
	}))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m, nil
}

func (c *RawKVClient) mgetSingleRegion(f func(k, v []byte)) func(*Backoffer, batchKeys) error {
	return func(bo *Backoffer, batch batchKeys) error {
		sender := NewRegionRequestSender(c.regionCache, c.rpcClient, kvrpcpb.IsolationLevel_SI)
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdRawMGet,
			RawMGet: &kvrpcpb.RawMGetRequest{
				Keys: batch.keys,
			},
		}
		resp, err := sender.SendReq(bo, req, batch.region, readTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.doActionByRegions(bo, batch.keys, c.mgetSingleRegion(f))
			return errors.Trace(err)
		}
		for _, pair := range resp.RawMGet.GetKvs() {
			f(pair.Key, pair.Value)
		}
		return nil
	}
}

// MPut stores multiple values to TiKV.
func (c *RawKVClient) MPut(kvs map[string][]byte) error {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("raw_mput").Observe(time.Since(start).Seconds()) }()

	keys := make([][]byte, len(kvs))
	for k := range kvs {
		keys = append(keys, []byte(k))
	}

	bo := NewBackoffer(rawkvMaxBackoff, goctx.Background())
	err := c.doActionByRegions(bo, keys, c.mputSingleRegion(kvs))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *RawKVClient) mputSingleRegion(kvs map[string][]byte) func(*Backoffer, batchKeys) error {
	return func(bo *Backoffer, batch batchKeys) error {
		sender := NewRegionRequestSender(c.regionCache, c.rpcClient, kvrpcpb.IsolationLevel_SI)
		pairs := make([]*kvrpcpb.KvPair, 0, len(batch.keys))
		for _, k := range batch.keys {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Key:   k,
				Value: kvs[string(k)],
			})
		}
		req := &tikvrpc.Request{
			Type: tikvrpc.CmdRawMPut,
			RawMPut: &kvrpcpb.RawMPutRequest{
				Kvs: pairs,
			},
		}
		resp, err := sender.SendReq(bo, req, batch.region, readTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.doActionByRegions(bo, batch.keys, c.mputSingleRegion(kvs))
			return errors.Trace(err)
		}
		if err := resp.RawMPut.Error; err != "" {
			return errors.New(err)
		}
		return nil
	}
}

func (c *RawKVClient) doActionByRegions(bo *Backoffer, keys [][]byte, f func(*Backoffer, batchKeys) error) error {
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return errors.Trace(err)
	}
	var batches []batchKeys
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, func([]byte) int { return 1 }, RawKVBatchSize)
	}
	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return errors.Trace(f(bo, batches[0]))
	}
	ch := make(chan error)
	for _, batch := range batches {
		go func(batch batchKeys) {
			backoffer, cancel := bo.Fork()
			defer cancel()
			ch <- f(backoffer, batch)
		}(batch)
	}
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			log.Debugf("rawkv batch failed: %v", e)
			err = e
		}
	}
	return errors.Trace(err)
}

func (c *RawKVClient) sendReq(key []byte, req *tikvrpc.Request) (*tikvrpc.Response, *KeyLocation, error) {
	bo := NewBackoffer(rawkvMaxBackoff, goctx.Background())
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient, kvrpcpb.IsolationLevel_SI)
	for {
		loc, err := c.regionCache.LocateKey(bo, key)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, loc, nil
	}
}

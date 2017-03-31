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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/pd-client"
	goctx "golang.org/x/net/context"
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

// Get queries value with the key. When the key does not exist, it returns
// `nil, nil`, while `[]byte{}, nil` means an empty value.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdRawGet,
		CmdRawGetReq: &kvrpcpb.CmdRawGetRequest{
			Key: key,
		},
	}
	resp, err := c.sendKVReq(key, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cmdResp := resp.GetCmdRawGetResp()
	if cmdResp == nil {
		return nil, errors.Trace(errBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	return cmdResp.Value, nil
}

// Put stores a key-value pair to TiKV.
func (c *RawKVClient) Put(key, value []byte) error {
	start := time.Now()
	defer func() { rawkvCmdHistogram.WithLabelValues("put").Observe(time.Since(start).Seconds()) }()
	rawkvSizeHistogram.WithLabelValues("key").Observe(float64(len(key)))
	rawkvSizeHistogram.WithLabelValues("value").Observe(float64(len(value)))

	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdRawPut,
		CmdRawPutReq: &kvrpcpb.CmdRawPutRequest{
			Key:   key,
			Value: value,
		},
	}
	resp, err := c.sendKVReq(key, req)
	if err != nil {
		return errors.Trace(err)
	}
	cmdResp := resp.GetCmdRawPutResp()
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

	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdRawDelete,
		CmdRawDeleteReq: &kvrpcpb.CmdRawDeleteRequest{
			Key: key,
		},
	}
	resp, err := c.sendKVReq(key, req)
	if err != nil {
		return errors.Trace(err)
	}
	cmdResp := resp.GetCmdRawDeleteResp()
	if cmdResp == nil {
		return errors.Trace(errBodyMissing)
	}
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

func (c *RawKVClient) sendKVReq(key []byte, req *kvrpcpb.Request) (*kvrpcpb.Response, error) {
	bo := NewBackoffer(rawkvMaxBackoff, goctx.Background())
	sender := NewRegionRequestSender(bo, c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := sender.SendKVReq(req, loc.Region, readTimeoutShort)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err := bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		return resp, nil
	}
}

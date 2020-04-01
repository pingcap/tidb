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
	"bytes"
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

var (
	// MaxRawKVScanLimit is the maximum scan limit for rawkv Scan.
	MaxRawKVScanLimit = 10240
	// ErrMaxScanLimitExceeded is returned when the limit for rawkv Scan is to large.
	ErrMaxScanLimitExceeded = errors.New("limit should be less than MaxRawKVScanLimit")
)

var (
	tikvRawkvCmdHistogramWithGet           = metrics.TiKVRawkvCmdHistogram.WithLabelValues("get")
	tikvRawkvCmdHistogramWithBatchGet      = metrics.TiKVRawkvCmdHistogram.WithLabelValues("batch_get")
	tikvRawkvCmdHistogramWithBatchPut      = metrics.TiKVRawkvCmdHistogram.WithLabelValues("batch_put")
	tikvRawkvCmdHistogramWithDelete        = metrics.TiKVRawkvCmdHistogram.WithLabelValues("delete")
	tikvRawkvCmdHistogramWithBatchDelete   = metrics.TiKVRawkvCmdHistogram.WithLabelValues("batch_delete")
	tikvRawkvCmdHistogramWithRawScan       = metrics.TiKVRawkvCmdHistogram.WithLabelValues("raw_scan")
	tikvRawkvCmdHistogramWithRawReversScan = metrics.TiKVRawkvCmdHistogram.WithLabelValues("raw_reverse_scan")

	tikvRawkvSizeHistogramWithKey   = metrics.TiKVRawkvSizeHistogram.WithLabelValues("key")
	tikvRawkvSizeHistogramWithValue = metrics.TiKVRawkvSizeHistogram.WithLabelValues("value")
)

const (
	// rawBatchPutSize is the maximum size limit for rawkv each batch put request.
	rawBatchPutSize = 16 * 1024
	// rawBatchPairCount is the maximum limit for rawkv each batch get/delete request.
	rawBatchPairCount = 512
)

// RawKVClient is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type RawKVClient struct {
	clusterID   uint64
	regionCache *RegionCache
	pdClient    pd.Client
	rpcClient   Client
}

// NewRawKVClient creates a client with PD cluster addrs.
func NewRawKVClient(pdAddrs []string, security config.Security, opts ...pd.ClientOption) (*RawKVClient, error) {
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RawKVClient{
		clusterID:   pdCli.GetClusterID(context.TODO()),
		regionCache: NewRegionCache(pdCli, nil),
		pdClient:    pdCli,
		rpcClient:   newRPCClient(security),
	}, nil
}

// Close closes the client.
func (c *RawKVClient) Close() error {
	if c.pdClient != nil {
		c.pdClient.Close()
	}
	if c.regionCache != nil {
		c.regionCache.Close()
	}
	if c.rpcClient == nil {
		return nil
	}
	return c.rpcClient.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *RawKVClient) ClusterID() uint64 {
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() { tikvRawkvCmdHistogramWithGet.Observe(time.Since(start).Seconds()) }()

	req := tikvrpc.NewRequest(tikvrpc.CmdRawGet, &kvrpcpb.RawGetRequest{Key: key})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.Resp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawGetResponse)
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	if len(cmdResp.Value) == 0 {
		return nil, nil
	}
	return cmdResp.Value, nil
}

// BatchGet queries values with the keys.
func (c *RawKVClient) BatchGet(keys [][]byte) ([][]byte, error) {
	start := time.Now()
	defer func() {
		tikvRawkvCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}()

	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, tikvrpc.CmdRawBatchGet)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp.Resp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawBatchGetResponse)

	keyToValue := make(map[string][]byte, len(keys))
	for _, pair := range cmdResp.Pairs {
		keyToValue[string(pair.Key)] = pair.Value
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = keyToValue[string(key)]
	}
	return values, nil
}

// Put stores a key-value pair to TiKV.
func (c *RawKVClient) Put(key, value []byte) error {
	start := time.Now()
	defer func() { tikvRawkvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds()) }()
	tikvRawkvSizeHistogramWithKey.Observe(float64(len(key)))
	tikvRawkvSizeHistogramWithValue.Observe(float64(len(value)))

	if len(value) == 0 {
		return errors.New("empty value is not supported")
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   key,
		Value: value,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawPutResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchPut stores key-value pairs to TiKV.
func (c *RawKVClient) BatchPut(keys, values [][]byte) error {
	start := time.Now()
	defer func() {
		tikvRawkvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds())
	}()

	if len(keys) != len(values) {
		return errors.New("the len of keys is not equal to the len of values")
	}
	for _, value := range values {
		if len(value) == 0 {
			return errors.New("empty value is not supported")
		}
	}
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	err := c.sendBatchPut(bo, keys, values)
	return errors.Trace(err)
}

// Delete deletes a key-value pair from TiKV.
func (c *RawKVClient) Delete(key []byte) error {
	start := time.Now()
	defer func() { tikvRawkvCmdHistogramWithDelete.Observe(time.Since(start).Seconds()) }()

	req := tikvrpc.NewRequest(tikvrpc.CmdRawDelete, &kvrpcpb.RawDeleteRequest{
		Key: key,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawDeleteResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchDelete deletes key-value pairs from TiKV
func (c *RawKVClient) BatchDelete(keys [][]byte) error {
	start := time.Now()
	defer func() {
		tikvRawkvCmdHistogramWithBatchDelete.Observe(time.Since(start).Seconds())
	}()

	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	resp, err := c.sendBatchReq(bo, keys, tikvrpc.CmdRawBatchDelete)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawBatchDeleteResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// DeleteRange deletes all key-value pairs in a range from TiKV
func (c *RawKVClient) DeleteRange(startKey []byte, endKey []byte) error {
	start := time.Now()
	var err error
	defer func() {
		var label = "delete_range"
		if err != nil {
			label += "_error"
		}
		metrics.TiKVRawkvCmdHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds())
	}()

	// Process each affected region respectively
	for !bytes.Equal(startKey, endKey) {
		var resp *tikvrpc.Response
		var actualEndKey []byte
		resp, actualEndKey, err = c.sendDeleteRangeReq(startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawDeleteRangeResponse)
		if cmdResp.GetError() != "" {
			return errors.New(cmdResp.GetError())
		}
		startKey = actualEndKey
	}

	return nil
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, push a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Scan(push(startKey, '\0'), push(endKey, '\0'), limit)`.
func (c *RawKVClient) Scan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() { tikvRawkvCmdHistogramWithRawScan.Observe(time.Since(start).Seconds()) }()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit && (len(endKey) == 0 || bytes.Compare(startKey, endKey) < 0) {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawScan, &kvrpcpb.RawScanRequest{
			StartKey: startKey,
			EndKey:   endKey,
			Limit:    uint32(limit - len(keys)),
		})
		resp, loc, err := c.sendReq(startKey, req, false)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if resp.Resp == nil {
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawScanResponse)
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

// ReverseScan queries continuous kv pairs in range [endKey, startKey), up to limit pairs.
// Direction is different from Scan, upper to lower.
// If endKey is empty, it means unbounded.
// If you want to include the startKey or exclude the endKey, push a '\0' to the key. For example, to scan
// (endKey, startKey], you can write:
// `ReverseScan(push(startKey, '\0'), push(endKey, '\0'), limit)`.
// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
func (c *RawKVClient) ReverseScan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() {
		tikvRawkvCmdHistogramWithRawReversScan.Observe(time.Since(start).Seconds())
	}()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit && bytes.Compare(startKey, endKey) > 0 {
		req := tikvrpc.NewRequest(tikvrpc.CmdRawScan, &kvrpcpb.RawScanRequest{
			StartKey: startKey,
			EndKey:   endKey,
			Limit:    uint32(limit - len(keys)),
			Reverse:  true,
		})
		resp, loc, err := c.sendReq(startKey, req, true)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if resp.Resp == nil {
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawScanResponse)
		for _, pair := range cmdResp.Kvs {
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		startKey = loc.StartKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

func (c *RawKVClient) sendReq(key []byte, req *tikvrpc.Request, reverse bool) (*tikvrpc.Response, *KeyLocation, error) {
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		var loc *KeyLocation
		var err error
		if reverse {
			loc, err = c.regionCache.LocateEndKey(bo, key)
		} else {
			loc, err = c.regionCache.LocateKey(bo, key)
		}
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
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, loc, nil
	}
}

func (c *RawKVClient) sendBatchReq(bo *Backoffer, keys [][]byte, cmdType tikvrpc.CmdType) (*tikvrpc.Response, error) { // split the keys
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var batches []batch
	for regionID, groupKeys := range groups {
		batches = appendKeyBatches(batches, regionID, groupKeys, rawBatchPairCount)
	}
	bo, cancel := bo.Fork()
	ches := make(chan singleBatchResp, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ches <- c.doBatchReq(singleBatchBackoffer, batch1, cmdType)
		}()
	}

	var firstError error
	var resp *tikvrpc.Response
	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		resp = &tikvrpc.Response{Resp: &kvrpcpb.RawBatchGetResponse{}}
	case tikvrpc.CmdRawBatchDelete:
		resp = &tikvrpc.Response{Resp: &kvrpcpb.RawBatchDeleteResponse{}}
	}
	for i := 0; i < len(batches); i++ {
		singleResp, ok := <-ches
		if ok {
			if singleResp.err != nil {
				cancel()
				if firstError == nil {
					firstError = singleResp.err
				}
			} else if cmdType == tikvrpc.CmdRawBatchGet {
				cmdResp := singleResp.resp.Resp.(*kvrpcpb.RawBatchGetResponse)
				resp.Resp.(*kvrpcpb.RawBatchGetResponse).Pairs = append(resp.Resp.(*kvrpcpb.RawBatchGetResponse).Pairs, cmdResp.Pairs...)
			}
		}
	}

	return resp, firstError
}

func (c *RawKVClient) doBatchReq(bo *Backoffer, batch batch, cmdType tikvrpc.CmdType) singleBatchResp {
	var req *tikvrpc.Request
	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		req = tikvrpc.NewRequest(cmdType, &kvrpcpb.RawBatchGetRequest{
			Keys: batch.keys,
		})
	case tikvrpc.CmdRawBatchDelete:
		req = tikvrpc.NewRequest(cmdType, &kvrpcpb.RawBatchDeleteRequest{
			Keys: batch.keys,
		})
	}

	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, readTimeoutShort)

	batchResp := singleBatchResp{}
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	if regionErr != nil {
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			batchResp.err = errors.Trace(err)
			return batchResp
		}
		resp, err = c.sendBatchReq(bo, batch.keys, cmdType)
		batchResp.resp = resp
		batchResp.err = err
		return batchResp
	}

	switch cmdType {
	case tikvrpc.CmdRawBatchGet:
		batchResp.resp = resp
	case tikvrpc.CmdRawBatchDelete:
		if resp.Resp == nil {
			batchResp.err = errors.Trace(ErrBodyMissing)
			return batchResp
		}
		cmdResp := resp.Resp.(*kvrpcpb.RawBatchDeleteResponse)
		if cmdResp.GetError() != "" {
			batchResp.err = errors.New(cmdResp.GetError())
			return batchResp
		}
		batchResp.resp = resp
	}
	return batchResp
}

// sendDeleteRangeReq sends a raw delete range request and returns the response and the actual endKey.
// If the given range spans over more than one regions, the actual endKey is the end of the first region.
// We can't use sendReq directly, because we need to know the end of the region before we send the request
// TODO: Is there any better way to avoid duplicating code with func `sendReq` ?
func (c *RawKVClient) sendDeleteRangeReq(startKey []byte, endKey []byte) (*tikvrpc.Response, []byte, error) {
	bo := NewBackoffer(context.Background(), rawkvMaxBackoff)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, startKey)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		actualEndKey := endKey
		if len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, endKey) < 0 {
			actualEndKey = loc.EndKey
		}

		req := tikvrpc.NewRequest(tikvrpc.CmdRawDeleteRange, &kvrpcpb.RawDeleteRangeRequest{
			StartKey: startKey,
			EndKey:   actualEndKey,
		})

		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, actualEndKey, nil
	}
}

func (c *RawKVClient) sendBatchPut(bo *Backoffer, keys, values [][]byte) error {
	keyToValue := make(map[string][]byte, len(keys))
	for i, key := range keys {
		keyToValue[string(key)] = values[i]
	}
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}
	var batches []batch
	// split the keys by size and RegionVerID
	for regionID, groupKeys := range groups {
		batches = appendBatches(batches, regionID, groupKeys, keyToValue, rawBatchPutSize)
	}
	bo, cancel := bo.Fork()
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ch <- c.doBatchPut(singleBatchBackoffer, batch1)
		}()
	}

	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			cancel()
			// catch the first error
			if err == nil {
				err = e
			}
		}
	}
	return errors.Trace(err)
}

func appendKeyBatches(batches []batch, regionID RegionVerID, groupKeys [][]byte, limit int) []batch {
	var keys [][]byte
	for start, count := 0, 0; start < len(groupKeys); start++ {
		if count > limit {
			batches = append(batches, batch{regionID: regionID, keys: keys})
			keys = make([][]byte, 0, limit)
			count = 0
		}
		keys = append(keys, groupKeys[start])
		count++
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys})
	}
	return batches
}

func appendBatches(batches []batch, regionID RegionVerID, groupKeys [][]byte, keyToValue map[string][]byte, limit int) []batch {
	var start, size int
	var keys, values [][]byte
	for start = 0; start < len(groupKeys); start++ {
		if size >= limit {
			batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			size = 0
		}
		key := groupKeys[start]
		value := keyToValue[string(key)]
		keys = append(keys, key)
		values = append(values, value)
		size += len(key)
		size += len(value)
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
	}
	return batches
}

func (c *RawKVClient) doBatchPut(bo *Backoffer, batch batch) error {
	kvPair := make([]*kvrpcpb.KvPair, 0, len(batch.keys))
	for i, key := range batch.keys {
		kvPair = append(kvPair, &kvrpcpb.KvPair{Key: key, Value: batch.values[i]})
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdRawBatchPut, &kvrpcpb.RawBatchPutRequest{Pairs: kvPair})

	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// recursive call
		return c.sendBatchPut(bo, batch.keys, batch.values)
	}

	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.RawBatchPutResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

type batch struct {
	regionID RegionVerID
	keys     [][]byte
	values   [][]byte
}

type singleBatchResp struct {
	resp *tikvrpc.Response
	err  error
}

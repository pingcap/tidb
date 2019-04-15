// Copyright 2019 PingCAP, Inc.
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

package helper

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	protocol = "http://"
)

// Helper is a middleware to get some information from tikv/pd. It can be used for TiDB's http api or mem table.
type Helper struct {
	Store       tikv.Storage
	RegionCache *tikv.RegionCache
}

// GetMvccByEncodedKey get the MVCC value by the specific encoded key.
func (h *Helper) GetMvccByEncodedKey(encodedKey kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	keyLocation, err := h.RegionCache.LocateKey(tikv.NewBackoffer(context.Background(), 500), encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tikvReq := &tikvrpc.Request{
		Type: tikvrpc.CmdMvccGetByKey,
		MvccGetByKey: &kvrpcpb.MvccGetByKeyRequest{
			Key: encodedKey,
		},
	}
	kvResp, err := h.Store.SendReq(tikv.NewBackoffer(context.Background(), 500), tikvReq, keyLocation.Region, time.Minute)
	if err != nil {
		logutil.Logger(context.Background()).Info("get MVCC by encoded key failed",
			zap.Binary("encodeKey", encodedKey),
			zap.Reflect("region", keyLocation.Region),
			zap.Binary("startKey", keyLocation.StartKey),
			zap.Binary("endKey", keyLocation.EndKey),
			zap.Reflect("kvResp", kvResp),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	return kvResp.MvccGetByKey, nil
}

// StoreHotRegionInfos records all hog region stores.
// it's the response of PD.
type StoreHotRegionInfos struct {
	AsPeer   map[uint64]*hotRegionsStat `json:"as_peer"`
	AsLeader map[uint64]*hotRegionsStat `json:"as_leader"`
}

// hotRegions records echo store's hot region.
// it's the response of PD.
type hotRegionsStat struct {
	RegionsStat []regionStat `json:"statistics"`
}

// regionStat records each hot region's statistics
// it's the response of PD.
type regionStat struct {
	RegionID  uint64 `json:"region_id"`
	FlowBytes uint64 `json:"flow_bytes"`
	HotDegree int    `json:"hot_degree"`
}

// RegionMetric presents the final metric output entry.
type RegionMetric struct {
	FlowBytes    uint64 `json:"flow_bytes"`
	MaxHotDegree int    `json:"max_hot_degree"`
	Count        int    `json:"region_count"`
}

// FetchHotRegion fetches the hot region information from PD's http api.
func (h *Helper) FetchHotRegion(rw string) (map[uint64]RegionMetric, error) {
	etcd, ok := h.Store.(domain.EtcdBackend)
	if !ok {
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	pdHosts := etcd.EtcdAddrs()
	if len(pdHosts) == 0 {
		return nil, errors.New("pd unavailable")
	}
	req, err := http.NewRequest("GET", protocol+pdHosts[0]+rw, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	timeout, cancelFunc := context.WithTimeout(context.Background(), 50*time.Millisecond)
	resp, err := http.DefaultClient.Do(req.WithContext(timeout))
	cancelFunc()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.Logger(context.Background()).Error("close body failed", zap.Error(err))
		}
	}()
	var regionResp StoreHotRegionInfos
	err = json.NewDecoder(resp.Body).Decode(&regionResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metric := make(map[uint64]RegionMetric)
	for _, hotRegions := range regionResp.AsLeader {
		for _, region := range hotRegions.RegionsStat {
			metric[region.RegionID] = RegionMetric{FlowBytes: region.FlowBytes, MaxHotDegree: region.HotDegree}
		}
	}
	return metric, nil
}

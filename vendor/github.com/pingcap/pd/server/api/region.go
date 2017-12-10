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

package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
	"github.com/unrolled/render"
)

type regionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []*metapb.Peer      `json:"peers,omitempty"`

	Leader          *metapb.Peer      `json:"leader,omitempty"`
	DownPeers       []*pdpb.PeerStats `json:"down_peers,omitempty"`
	PendingPeers    []*metapb.Peer    `json:"pending_peers,omitempty"`
	WrittenBytes    uint64            `json:"written_bytes,omitempty"`
	ReadBytes       uint64            `json:"read_bytes,omitempty"`
	ApproximateSize int64             `json:"approximate_size,omitempty"`
}

func newRegionInfo(r *core.RegionInfo) *regionInfo {
	if r == nil {
		return nil
	}
	return &regionInfo{
		ID:          r.Id,
		StartKey:    strings.Trim(fmt.Sprintf("%q", r.StartKey), "\""),
		EndKey:      strings.Trim(fmt.Sprintf("%q", r.EndKey), "\""),
		RegionEpoch: r.RegionEpoch,
		Peers:       r.Peers,

		Leader:          r.Leader,
		DownPeers:       r.DownPeers,
		PendingPeers:    r.PendingPeers,
		WrittenBytes:    r.WrittenBytes,
		ReadBytes:       r.ReadBytes,
		ApproximateSize: r.ApproximateSize,
	}
}

type regionsInfo struct {
	Count   int           `json:"count"`
	Regions []*regionInfo `json:"regions"`
}

type regionHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionHandler(svr *server.Server, rd *render.Render) *regionHandler {
	return &regionHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *regionHandler) GetRegionByID(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	regionInfo := cluster.GetRegionInfoByID(regionID)
	h.rd.JSON(w, http.StatusOK, newRegionInfo(regionInfo))
}

func (h *regionHandler) GetRegionByKey(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	vars := mux.Vars(r)
	key := vars["key"]
	regionInfo := cluster.GetRegionInfoByKey([]byte(key))
	h.rd.JSON(w, http.StatusOK, newRegionInfo(regionInfo))
}

type regionsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newRegionsHandler(svr *server.Server, rd *render.Render) *regionsHandler {
	return &regionsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *regionsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	regions := cluster.GetRegions()
	regionInfos := make([]*regionInfo, len(regions))
	for i, r := range regions {
		regionInfos[i] = newRegionInfo(&core.RegionInfo{Region: r})
	}
	regionsInfo := &regionsInfo{
		Count:   len(regions),
		Regions: regionInfos,
	}
	h.rd.JSON(w, http.StatusOK, regionsInfo)
}

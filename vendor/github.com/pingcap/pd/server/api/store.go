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
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
	"github.com/unrolled/render"
)

type metaStore struct {
	*metapb.Store
	StateName string `json:"state_name"`
}

type storeStatus struct {
	Capacity           typeutil.ByteSize  `json:"capacity,omitempty"`
	Available          typeutil.ByteSize  `json:"available,omitempty"`
	LeaderCount        int                `json:"leader_count,omitempty"`
	LeaderWeight       float64            `json:"leader_weight,omitempty"`
	LeaderScore        float64            `json:"leader_score,omitempty"`
	LeaderSize         int64              `json:"leader_size,omitempty"`
	RegionCount        int                `json:"region_count,omitempty"`
	RegionWeight       float64            `json:"region_weight,omitempty"`
	RegionScore        float64            `json:"region_score,omitempty"`
	RegionSize         int64              `json:"region_size,omitempty"`
	SendingSnapCount   uint32             `json:"sending_snap_count,omitempty"`
	ReceivingSnapCount uint32             `json:"receiving_snap_count,omitempty"`
	ApplyingSnapCount  uint32             `json:"applying_snap_count,omitempty"`
	IsBusy             bool               `json:"is_busy,omitempty"`
	StartTS            *time.Time         `json:"start_ts,omitempty"`
	LastHeartbeatTS    *time.Time         `json:"last_heartbeat_ts,omitempty"`
	Uptime             *typeutil.Duration `json:"uptime,omitempty"`
}

type storeInfo struct {
	Store  *metaStore   `json:"store"`
	Status *storeStatus `json:"status"`
}

const (
	disconnectedName = "Disconnected"
	downStateName    = "Down"
)

func newStoreInfo(store *core.StoreInfo, maxStoreDownTime time.Duration) *storeInfo {
	s := &storeInfo{
		Store: &metaStore{
			Store:     store.Store,
			StateName: store.State.String(),
		},
		Status: &storeStatus{
			Capacity:           typeutil.ByteSize(store.Stats.GetCapacity()),
			Available:          typeutil.ByteSize(store.Stats.GetAvailable()),
			LeaderCount:        store.LeaderCount,
			LeaderWeight:       store.LeaderWeight,
			LeaderScore:        store.LeaderScore(),
			LeaderSize:         store.LeaderSize,
			RegionCount:        store.RegionCount,
			RegionWeight:       store.RegionWeight,
			RegionScore:        store.RegionScore(),
			RegionSize:         store.RegionSize,
			SendingSnapCount:   store.Stats.GetSendingSnapCount(),
			ReceivingSnapCount: store.Stats.GetReceivingSnapCount(),
			ApplyingSnapCount:  store.Stats.GetApplyingSnapCount(),
			IsBusy:             store.Stats.GetIsBusy(),
		},
	}

	if store.Stats != nil {
		startTS := store.GetStartTS()
		s.Status.StartTS = &startTS
	}
	if lastHeartbeat := store.LastHeartbeatTS; !lastHeartbeat.IsZero() {
		s.Status.LastHeartbeatTS = &lastHeartbeat
	}
	if upTime := store.GetUptime(); upTime > 0 {
		duration := typeutil.NewDuration(upTime)
		s.Status.Uptime = &duration
	}

	if store.State == metapb.StoreState_Up {
		if store.DownTime() > maxStoreDownTime {
			s.Store.StateName = downStateName
		} else if store.IsDisconnected() {
			s.Store.StateName = disconnectedName
		}
	}
	return s
}

type storesInfo struct {
	Count  int          `json:"count"`
	Stores []*storeInfo `json:"stores"`
}

type storeHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStoreHandler(svr *server.Server, rd *render.Render) *storeHandler {
	return &storeHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *storeHandler) Get(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	maxStoreDownTime := h.svr.GetScheduleConfig().MaxStoreDownTime.Duration

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	store, err := cluster.GetStore(storeID)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	storeInfo := newStoreInfo(store, maxStoreDownTime)
	h.rd.JSON(w, http.StatusOK, storeInfo)
}

func (h *storeHandler) Delete(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	_, force := r.URL.Query()["force"]
	if force {
		err = cluster.BuryStore(storeID, force)
	} else {
		err = cluster.RemoveStore(storeID)
	}

	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *storeHandler) SetState(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	stateStr := r.URL.Query().Get("state")
	state, ok := metapb.StoreState_value[stateStr]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "invalid state")
		return
	}

	err = cluster.SetStoreState(storeID, metapb.StoreState(state))
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *storeHandler) SetLabels(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	var input map[string]string
	if err := readJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	labels := make([]*metapb.StoreLabel, 0, len(input))
	for k, v := range input {
		labels = append(labels, &metapb.StoreLabel{
			Key:   k,
			Value: v,
		})
	}

	if err := cluster.UpdateStoreLabels(storeID, labels); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *storeHandler) SetWeight(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	var input map[string]interface{}
	if err := readJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	leaderVal, ok := input["leader"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "leader weight unset")
		return
	}
	regionVal, ok := input["region"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, "region weight unset")
		return
	}
	leader, ok := leaderVal.(float64)
	if !ok || leader < 0 {
		h.rd.JSON(w, http.StatusBadRequest, "badformat leader weight")
		return
	}
	region, ok := regionVal.(float64)
	if !ok || region < 0 {
		h.rd.JSON(w, http.StatusBadRequest, "badformat region weight")
		return
	}

	if err := cluster.SetStoreWeight(storeID, leader, region); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

type storesHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStoresHandler(svr *server.Server, rd *render.Render) *storesHandler {
	return &storesHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *storesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	maxStoreDownTime := h.svr.GetScheduleConfig().MaxStoreDownTime.Duration

	stores := cluster.GetStores()
	storesInfo := &storesInfo{
		Stores: make([]*storeInfo, 0, len(stores)),
	}

	urlFilter, err := newStoreStateFilter(r.URL)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	stores = urlFilter.filter(cluster.GetStores())
	for _, s := range stores {
		store, err := cluster.GetStore(s.GetId())
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		storeInfo := newStoreInfo(store, maxStoreDownTime)
		storesInfo.Stores = append(storesInfo.Stores, storeInfo)
	}
	storesInfo.Count = len(storesInfo.Stores)

	h.rd.JSON(w, http.StatusOK, storesInfo)
}

type storeStateFilter struct {
	accepts []metapb.StoreState
}

func newStoreStateFilter(u *url.URL) (*storeStateFilter, error) {
	var acceptStates []metapb.StoreState
	if v, ok := u.Query()["state"]; ok {
		for _, s := range v {
			state, err := strconv.Atoi(s)
			if err != nil {
				return nil, errors.Trace(err)
			}

			storeState := metapb.StoreState(state)
			switch storeState {
			case metapb.StoreState_Up, metapb.StoreState_Offline, metapb.StoreState_Tombstone:
				acceptStates = append(acceptStates, storeState)
			default:
				return nil, errors.Errorf("unknown StoreState: %v", storeState)
			}
		}
	} else {
		// Accepts Up and Offline by default.
		acceptStates = []metapb.StoreState{metapb.StoreState_Up, metapb.StoreState_Offline}
	}

	return &storeStateFilter{
		accepts: acceptStates,
	}, nil
}

func (filter *storeStateFilter) filter(stores []*metapb.Store) []*metapb.Store {
	ret := make([]*metapb.Store, 0, len(stores))
	for _, s := range stores {
		state := s.GetState()
		for _, accept := range filter.accepts {
			if state == accept {
				ret = append(ret, s)
				break
			}
		}
	}
	return ret
}

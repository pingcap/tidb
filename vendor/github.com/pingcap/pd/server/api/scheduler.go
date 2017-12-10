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

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type schedulerHandler struct {
	*server.Handler
	r *render.Render
}

func newSchedulerHandler(handler *server.Handler, r *render.Render) *schedulerHandler {
	return &schedulerHandler{
		Handler: handler,
		r:       r,
	}
}

func (h *schedulerHandler) List(w http.ResponseWriter, r *http.Request) {
	schedulers, err := h.GetSchedulers()
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, schedulers)
}

func (h *schedulerHandler) Post(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSON(r.Body, &input); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	name, ok := input["name"].(string)
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing scheduler name")
		return
	}

	switch name {
	case "balance-leader-scheduler":
		if err := h.AddBalanceLeaderScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "balance-hot-region-scheduler":
		if err := h.AddBalanceHotRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

	case "balance-region-scheduler":
		if err := h.AddBalanceRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "balance-adjacent-region-scheduler":
		var args []string
		leaderLimit, ok := input["leader_limit"].(string)
		if ok {
			args = append(args, leaderLimit)
		}
		peerLimit, ok := input["peer_limit"].(string)
		if ok {
			args = append(args, peerLimit)
		} else {
			args = args[:0]
		}
		if err := h.AddAdjacentRegionScheduler(args...); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "grant-leader-scheduler":
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		if err := h.AddGrantLeaderScheduler(uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "evict-leader-scheduler":
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id")
			return
		}
		if err := h.AddEvictLeaderScheduler(uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "shuffle-leader-scheduler":
		if err := h.AddShuffleLeaderScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "shuffle-region-scheduler":
		if err := h.AddShuffleRegionScheduler(); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown scheduler")
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}

func (h *schedulerHandler) Delete(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]

	if err := h.RemoveScheduler(name); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}

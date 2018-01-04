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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/schedule"
	"github.com/unrolled/render"
)

type operatorHandler struct {
	*server.Handler
	r *render.Render
}

func newOperatorHandler(handler *server.Handler, r *render.Render) *operatorHandler {
	return &operatorHandler{
		Handler: handler,
		r:       r,
	}
}

func (h *operatorHandler) Get(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["region_id"]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	op, err := h.GetOperator(regionID)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, op)
}

func (h *operatorHandler) List(w http.ResponseWriter, r *http.Request) {
	var (
		results []*schedule.Operator
		ops     []*schedule.Operator
		err     error
	)

	kinds, ok := r.URL.Query()["kind"]
	if !ok {
		results, err = h.GetOperators()
		if err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		for _, kind := range kinds {
			switch kind {
			case "admin":
				ops, err = h.GetAdminOperators()
			case "leader":
				ops, err = h.GetLeaderOperators()
			case "region":
				ops, err = h.GetRegionOperators()
			}
			if err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			results = append(results, ops...)
		}
	}

	h.r.JSON(w, http.StatusOK, results)
}

func (h *operatorHandler) Post(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := readJSON(r.Body, &input); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	name, ok := input["name"].(string)
	if !ok {
		h.r.JSON(w, http.StatusInternalServerError, "missing operator name")
		return
	}

	switch name {
	case "transfer-leader":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["to_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id to transfer leader to")
			return
		}
		if err := h.AddTransferLeaderOperator(uint64(regionID), uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "transfer-region":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeIDs, ok := parseStoreIDs(input["to_store_ids"])
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store ids to transfer region to")
			return
		}
		if len(storeIDs) == 0 {
			h.r.JSON(w, http.StatusBadRequest, "missing store ids to transfer region to")
			return
		}
		if err := h.AddTransferRegionOperator(uint64(regionID), storeIDs); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "transfer-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		fromID, ok := input["from_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer from")
			return
		}
		toID, ok := input["to_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer to")
			return
		}
		if err := h.AddTransferPeerOperator(uint64(regionID), uint64(fromID), uint64(toID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "add-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer to")
			return
		}
		if err := h.AddAddPeerOperator(uint64(regionID), uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "remove-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer to")
			return
		}
		if err := h.AddRemovePeerOperator(uint64(regionID), uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown operator")
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}

func (h *operatorHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["region_id"]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err = h.RemoveOperator(regionID); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, nil)
}

func parseStoreIDs(v interface{}) (map[uint64]struct{}, bool) {
	items, ok := v.([]interface{})
	if !ok {
		return nil, false
	}
	ids := make(map[uint64]struct{})
	for _, item := range items {
		id, ok := item.(float64)
		if !ok {
			return nil, false
		}
		ids[uint64(id)] = struct{}{}
	}
	return ids, true
}

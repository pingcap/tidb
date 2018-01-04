// Copyright 2017 PingCAP, Inc.
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

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type statsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newStatsHandler(svr *server.Server, rd *render.Render) *statsHandler {
	return &statsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *statsHandler) Region(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	startKey, endKey := r.URL.Query().Get("start_key"), r.URL.Query().Get("end_key")
	stats := cluster.GetRegionStats([]byte(startKey), []byte(endKey))
	h.rd.JSON(w, http.StatusOK, stats)
}

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
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type confHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newConfHandler(svr *server.Server, rd *render.Render) *confHandler {
	return &confHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *confHandler) Get(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, h.svr.GetConfig())
}

func (h *confHandler) Post(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetConfig()
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	err = json.Unmarshal(data, &config.Schedule)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	err = json.Unmarshal(data, &config.Replication)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.svr.SetScheduleConfig(config.Schedule)
	h.svr.SetReplicationConfig(config.Replication)
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetSchedule(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, &h.svr.GetConfig().Schedule)
}

func (h *confHandler) SetSchedule(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetScheduleConfig()
	err := readJSON(r.Body, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.svr.SetScheduleConfig(*config)
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *confHandler) GetReplication(w http.ResponseWriter, r *http.Request) {
	h.rd.JSON(w, http.StatusOK, &h.svr.GetConfig().Replication)
}

func (h *confHandler) SetReplication(w http.ResponseWriter, r *http.Request) {
	config := h.svr.GetReplicationConfig()
	err := readJSON(r.Body, config)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.svr.SetReplicationConfig(*config)
	h.rd.JSON(w, http.StatusOK, nil)
}

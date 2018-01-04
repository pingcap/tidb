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

type statusHandler struct {
	rd *render.Render
}

type status struct {
	BuildTS string `json:"build_ts"`
	GitHash string `json:"git_hash"`
}

func newStatusHandler(rd *render.Render) *statusHandler {
	return &statusHandler{
		rd: rd,
	}
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	version := status{
		BuildTS: server.PDBuildTS,
		GitHash: server.PDGitHash,
	}

	h.rd.JSON(w, http.StatusOK, version)
}

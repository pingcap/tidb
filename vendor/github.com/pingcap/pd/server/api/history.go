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
	"strconv"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/schedule"
	"github.com/unrolled/render"
)

var errUnknownOperatorKind = errors.New("Unknown operator kind")

type historyHandler struct {
	*server.Handler
	r *render.Render
}

func newHistoryHandler(handler *server.Handler, r *render.Render) *historyHandler {
	return &historyHandler{
		Handler: handler,
		r:       r,
	}
}

func (h *historyHandler) GetOperators(w http.ResponseWriter, r *http.Request) {
	ops, err := h.GetHistoryOperators()
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, ops)
}

func (h *historyHandler) GetOperatorsOfKind(w http.ResponseWriter, r *http.Request) {
	k := mux.Vars(r)["kind"]
	l := mux.Vars(r)["limit"]
	limit, err := strconv.Atoi(l)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if limit <= 0 {
		h.r.JSON(w, http.StatusOK, nil)
		return
	}
	kind, err := schedule.ParseOperatorKind(k)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ops, err := h.GetHistoryOperatorsOfKind(kind)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	if limit > len(ops) {
		limit = len(ops)
	}

	h.r.JSON(w, http.StatusOK, ops[:limit])
}

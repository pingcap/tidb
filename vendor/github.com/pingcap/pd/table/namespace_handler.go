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

package table

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/apiutil"
	"github.com/unrolled/render"
)

func newTableClassifierHandler(classifier *tableNamespaceClassifier) http.Handler {
	h := &tableNamespaceHandler{
		classifier: classifier,
		rd:         render.New(render.Options{IndentJSON: true}),
	}
	router := mux.NewRouter()
	router.HandleFunc("/table/namespaces", h.Get).Methods("GET")
	router.HandleFunc("/table/namespaces", h.Post).Methods("POST")
	router.HandleFunc("/table/namespaces/table", h.Update).Methods("POST")
	router.HandleFunc("/table/namespaces/meta", h.SetMetaNamespace).Methods("POST")
	router.HandleFunc("/table/store_ns/{id}", h.SetNamespace).Methods("POST")
	return router
}

type tableNamespaceHandler struct {
	classifier *tableNamespaceClassifier
	rd         *render.Render
}

func (h *tableNamespaceHandler) Get(w http.ResponseWriter, r *http.Request) {
	type namespacesInfo struct {
		Count      int          `json:"count"`
		Namespaces []*Namespace `json:"namespaces"`
	}

	namespaces := h.classifier.GetNamespaces()
	nsInfo := &namespacesInfo{
		Count:      len(namespaces),
		Namespaces: namespaces,
	}
	h.rd.JSON(w, http.StatusOK, nsInfo)
}

// Post creates a namespace.
func (h *tableNamespaceHandler) Post(w http.ResponseWriter, r *http.Request) {
	var input map[string]string
	if err := apiutil.ReadJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ns := input["namespace"]

	// create namespace
	if err := h.classifier.CreateNamespace(ns); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *tableNamespaceHandler) Update(w http.ResponseWriter, r *http.Request) {
	var input map[string]string
	if err := apiutil.ReadJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	tableIDStr := input["table_id"]
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ns := input["namespace"]
	action, ok := input["action"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, errors.New("missing parameters"))
		return
	}

	switch action {
	case "add":
		// append table id to namespace
		if err := h.classifier.AddNamespaceTableID(ns, tableID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "remove":
		// remove table id from namespace
		if err := h.classifier.RemoveNamespaceTableID(ns, tableID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.rd.JSON(w, http.StatusBadRequest, errors.New("unknown action"))
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *tableNamespaceHandler) SetMetaNamespace(w http.ResponseWriter, r *http.Request) {
	var input map[string]string
	if err := apiutil.ReadJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ns := input["namespace"]
	switch input["action"] {
	case "add":
		if err := h.classifier.AddMetaToNamespace(ns); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "remove":
		if err := h.classifier.RemoveMeta(ns); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.rd.JSON(w, http.StatusBadRequest, errors.New("unknown action"))
		return
	}
	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *tableNamespaceHandler) SetNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	storeIDStr := vars["id"]
	storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	var input map[string]string
	if err := apiutil.ReadJSON(r.Body, &input); err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	ns := input["namespace"]
	action, ok := input["action"]
	if !ok {
		h.rd.JSON(w, http.StatusBadRequest, errors.New("missing parameters"))
	}

	switch action {
	case "add":
		// append store id to namespace
		if err := h.classifier.AddNamespaceStoreID(ns, storeID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "remove":
		// remove store id from namespace
		if err := h.classifier.RemoveNamespaceStoreID(ns, storeID); err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		h.rd.JSON(w, http.StatusBadRequest, errors.New("unknown action"))
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

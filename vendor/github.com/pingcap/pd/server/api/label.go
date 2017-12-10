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
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
)

type labelsHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newLabelsHandler(svr *server.Server, rd *render.Render) *labelsHandler {
	return &labelsHandler{
		svr: svr,
		rd:  rd,
	}
}

func (h *labelsHandler) Get(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}
	var labels []*metapb.StoreLabel
	m := make(map[string]struct{})
	stores := cluster.GetStores()
	for _, s := range stores {
		ls := s.GetLabels()
		for _, l := range ls {
			if _, ok := m[strings.ToLower(l.Key+l.Value)]; !ok {
				m[strings.ToLower(l.Key+l.Value)] = struct{}{}
				labels = append(labels, l)
			}
		}
	}
	h.rd.JSON(w, http.StatusOK, labels)
}

func (h *labelsHandler) GetStores(w http.ResponseWriter, r *http.Request) {
	cluster := h.svr.GetRaftCluster()
	if cluster == nil {
		h.rd.JSON(w, http.StatusInternalServerError, server.ErrNotBootstrapped.Error())
		return
	}

	name := r.URL.Query().Get("name")
	value := r.URL.Query().Get("value")
	filter, err := newStoresLabelFilter(name, value)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	maxDownTime := h.svr.GetScheduleConfig().MaxStoreDownTime.Duration

	stores := cluster.GetStores()
	storesInfo := &storesInfo{
		Stores: make([]*storeInfo, 0, len(stores)),
	}

	stores = filter.filter(stores)
	for _, s := range stores {
		store, err := cluster.GetStore(s.GetId())
		if err != nil {
			h.rd.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}

		storeInfo := newStoreInfo(store, maxDownTime)
		storesInfo.Stores = append(storesInfo.Stores, storeInfo)
	}
	storesInfo.Count = len(storesInfo.Stores)

	h.rd.JSON(w, http.StatusOK, storesInfo)
}

type storesLabelFilter struct {
	keyPattern   *regexp.Regexp
	valuePattern *regexp.Regexp
}

func newStoresLabelFilter(name, value string) (*storesLabelFilter, error) {
	// add (?i) to set a case-insensitive flag
	keyPattern, err := regexp.Compile("(?i)" + name)
	if err != nil {
		return nil, errors.Trace(err)
	}
	valuePattern, err := regexp.Compile("(?i)" + value)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &storesLabelFilter{
		keyPattern:   keyPattern,
		valuePattern: valuePattern,
	}, nil
}

func (filter *storesLabelFilter) filter(stores []*metapb.Store) []*metapb.Store {
	ret := make([]*metapb.Store, 0, len(stores))
	for _, s := range stores {
		ls := s.GetLabels()
		for _, l := range ls {
			isKeyMatch := filter.keyPattern.MatchString(l.Key)
			isValueMatch := filter.valuePattern.MatchString(l.Value)
			if isKeyMatch && isValueMatch {
				ret = append(ret, s)
				break
			}
		}

	}
	return ret
}

// Copyright 2018 PingCAP, Inc.
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

package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/model"
)

// StatsHandler is the handler for dump statistics.
type StatsHandler struct {
	do *domain.Domain
}

func (s *Server) newStatsHandler() *StatsHandler {
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := tidb.GetDomain(store.store)
	if err != nil {
		panic("Failed to get domain")
	}
	return &StatsHandler{do}
}

func (sh StatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)

	is := sh.do.InfoSchema()
	h := sh.do.StatsHandle()

	tbl, err := is.TableByName(model.NewCIStr(params[pDBName]), model.NewCIStr(params[pTableName]))
	if err != nil {
		writeError(w, err)
	} else {
		js, err := h.DumpStatsToJSON(params[pDBName], tbl.Meta())
		if err != nil {
			writeError(w, err)
		} else {
			writeData(w, js)
		}
	}
}

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
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

// StatsHandler is the handler for dumping statistics.
type StatsHandler struct {
	do *domain.Domain
}

func (s *Server) newStatsHandler() *StatsHandler {
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := session.GetDomain(store.store)
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
		js, err := h.DumpStatsToJSON(params[pDBName], tbl.Meta(), nil)
		if err != nil {
			writeError(w, err)
		} else {
			writeData(w, js)
		}
	}
}

// StatsHistoryHandler is the handler for dumping statistics.
type StatsHistoryHandler struct {
	do *domain.Domain
}

func (s *Server) newStatsHistoryHandler() *StatsHistoryHandler {
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := session.GetDomain(store.store)
	if err != nil {
		panic("Failed to get domain")
	}
	return &StatsHistoryHandler{do}
}

func (sh StatsHistoryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)
	se, err := session.CreateSession(sh.do.Store())
	if err != nil {
		writeError(w, err)
		return
	}
	se.GetSessionVars().StmtCtx.TimeZone = time.Local
	t, err := types.ParseTime(se.GetSessionVars().StmtCtx, params[pSnapshot], mysql.TypeTimestamp, 6)
	if err != nil {
		writeError(w, err)
		return
	}
	t1, err := t.GoTime(time.Local)
	if err != nil {
		writeError(w, err)
		return
	}
	snapshot := variable.GoTimeToTS(t1)
	err = gcutil.ValidateSnapshot(se, snapshot)
	if err != nil {
		writeError(w, err)
		return
	}

	is, err := sh.do.GetSnapshotInfoSchema(snapshot)
	if err != nil {
		writeError(w, err)
		return
	}
	h := sh.do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(params[pDBName]), model.NewCIStr(params[pTableName]))
	if err != nil {
		writeError(w, err)
		return
	}
	se.GetSessionVars().SnapshotInfoschema, se.GetSessionVars().SnapshotTS = is, snapshot
	historyStatsExec := se.(sqlexec.RestrictedSQLExecutor)
	js, err := h.DumpStatsToJSON(params[pDBName], tbl.Meta(), historyStatsExec)
	if err != nil {
		writeError(w, err)
	} else {
		writeData(w, js)
	}
}

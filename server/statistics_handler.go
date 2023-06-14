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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/oracle"
)

func compressJSON(w http.ResponseWriter, statsFileName string, js []byte) error {
	zw := zip.NewWriter(w)
	defer func() {
		_ = zw.Close()
	}()

	fw, err := zw.Create(statsFileName)
	if err != nil {
		return err
	}
	_, err = fw.Write(js)
	return err
}

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
	params := mux.Vars(req)

	is := sh.do.InfoSchema()
	h := sh.do.StatsHandle()
	var err error
	dumpPartitionStats := true
	dumpParams := req.URL.Query()[pDumpPartitionStats]
	if len(dumpParams) > 0 && len(dumpParams[0]) > 0 {
		dumpPartitionStats, err = strconv.ParseBool(dumpParams[0])
		if err != nil {
			writeError(w, err)
			return
		}
	}
	tbl, err := is.TableByName(model.NewCIStr(params[pDBName]), model.NewCIStr(params[pTableName]))
	if err != nil {
		writeError(w, err)
		return
	}
	jst, err := h.DumpStatsToJSON(params[pDBName], tbl.Meta(), nil, dumpPartitionStats)
	if err != nil {
		writeError(w, err)
		return
	}
	js, err := json.MarshalIndent(jst, "", " ")
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"tidb_stats_\"%s.zip", time.Now().Format("20060102150405")))
	statsFilename := fmt.Sprintf("%s.%s.json", params[pDBName], params[pTableName])
	err = compressJSON(w, statsFilename, js)
	if err != nil {
		writeError(w, err)
		return
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
	params := mux.Vars(req)
	se, err := session.CreateSession(sh.do.Store())
	if err != nil {
		writeError(w, err)
		return
	}
	defer se.Close()
	enabeld, err := sh.do.StatsHandle().CheckHistoricalStatsEnable()
	if err != nil {
		writeError(w, err)
		return
	}
	if !enabeld {
		writeError(w, fmt.Errorf("%v should be enabled", variable.TiDBEnableHistoricalStats))
		return
	}

	se.GetSessionVars().StmtCtx.TimeZone = time.Local
	t, err := types.ParseTime(se.GetSessionVars().StmtCtx, params[pSnapshot], mysql.TypeTimestamp, 6, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	t1, err := t.GoTime(time.Local)
	if err != nil {
		writeError(w, err)
		return
	}
	snapshot := oracle.GoTimeToTS(t1)
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
	jst, err := h.DumpHistoricalStatsBySnapshot(params[pDBName], tbl.Meta(), snapshot)
	if err != nil {
		writeError(w, err)
		return
	}

	js, err := json.MarshalIndent(jst, "", " ")
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"tidb_stats_timestamp\"%s.zip", time.Now().Format("20060102150405")))
	statsFilename := fmt.Sprintf("%s.%s.%s.json", params[pDBName], params[pTableName], t1.Format("20060102150405"))
	err = compressJSON(w, statsFilename, js)
	if err != nil {
		writeError(w, err)
		return
	}
}

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

package statshandler

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server/constvar"
	"github.com/pingcap/tidb/server/internal/httputil"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// StatsHandler is the handler for dumping statistics.
type StatsHandler struct {
	do *domain.Domain
}

func NewStatsHandler(do *domain.Domain) *StatsHandler {
	return &StatsHandler{do}
}

func (sh *StatsHandler) Domain() *domain.Domain {
	return sh.do
}

func (sh StatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)

	is := sh.do.InfoSchema()
	h := sh.do.StatsHandle()
	var err error
	dumpPartitionStats := true
	dumpParams := req.URL.Query()[constvar.DumpPartitionStatsParam]
	if len(dumpParams) > 0 && len(dumpParams[0]) > 0 {
		dumpPartitionStats, err = strconv.ParseBool(dumpParams[0])
		if err != nil {
			httputil.WriteError(w, err)
			return
		}
	}
	tbl, err := is.TableByName(model.NewCIStr(params[constvar.DBNameParam]), model.NewCIStr(params[constvar.TableNameParam]))
	if err != nil {
		httputil.WriteError(w, err)
	} else {
		js, err := h.DumpStatsToJSON(params[constvar.DBNameParam], tbl.Meta(), nil, dumpPartitionStats)
		if err != nil {
			httputil.WriteError(w, err)
		} else {
			httputil.WriteData(w, js)
		}
	}
}

// StatsHistoryHandler is the handler for dumping statistics.
type StatsHistoryHandler struct {
	do *domain.Domain
}

// NewStatsHistoryHandler creates a new stats history handler.
func NewStatsHistoryHandler(do *domain.Domain) *StatsHistoryHandler {
	return &StatsHistoryHandler{do}
}

func (sh StatsHistoryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)
	se, err := session.CreateSession(sh.do.Store())
	if err != nil {
		httputil.WriteError(w, err)
		return
	}
	defer se.Close()
	enabeld, err := sh.do.StatsHandle().CheckHistoricalStatsEnable()
	if err != nil {
		httputil.WriteError(w, err)
		return
	}
	if !enabeld {
		httputil.WriteError(w, fmt.Errorf("%v should be enabled", variable.TiDBEnableHistoricalStats))
		return
	}

	se.GetSessionVars().StmtCtx.TimeZone = time.Local
	t, err := types.ParseTime(se.GetSessionVars().StmtCtx, params[constvar.SnapshotParam], mysql.TypeTimestamp, 6, nil)
	if err != nil {
		httputil.WriteError(w, err)
		return
	}
	t1, err := t.GoTime(time.Local)
	if err != nil {
		httputil.WriteError(w, err)
		return
	}
	snapshot := oracle.GoTimeToTS(t1)
	tbl, err := getSnapshotTableInfo(sh.do, snapshot, params[constvar.DBNameParam], params[constvar.TableNameParam])
	if err != nil {
		logutil.BgLogger().Info("fail to get snapshot TableInfo in historical stats API, switch to use latest infoschema", zap.Error(err))
		is := sh.do.InfoSchema()
		tbl, err = is.TableByName(model.NewCIStr(params[constvar.DBNameParam]), model.NewCIStr(params[constvar.TableNameParam]))
		if err != nil {
			httputil.WriteError(w, err)
			return
		}
	}

	h := sh.do.StatsHandle()
	js, err := h.DumpHistoricalStatsBySnapshot(params[constvar.DBNameParam], tbl.Meta(), snapshot)
	if err != nil {
		httputil.WriteError(w, err)
	} else {
		httputil.WriteData(w, js)
	}
}

func getSnapshotTableInfo(dom *domain.Domain, snapshot uint64, dbName, tblName string) (table.Table, error) {
	is, err := dom.GetSnapshotInfoSchema(snapshot)
	if err != nil {
		return nil, err
	}
	return is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
}

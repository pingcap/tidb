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

package optimizor

import (
	"context"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/server/handler"
)

// StatsHandler is the handler for dumping statistics.
type StatsHandler struct {
	do *domain.Domain
}

// NewStatsHandler creates a new StatsHandler.
func NewStatsHandler(do *domain.Domain) *StatsHandler {
	return &StatsHandler{do: do}
}

// Domain is to get domain.
func (sh *StatsHandler) Domain() *domain.Domain {
	return sh.do
}

// ServeHTTP dumps the statistics to json.
func (sh StatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)

	is := sh.do.InfoSchema()
	h := sh.do.StatsHandle()
	var err error
	dumpPartitionStats := true
	dumpParams := req.URL.Query()[handler.DumpPartitionStats]
	if len(dumpParams) > 0 && len(dumpParams[0]) > 0 {
		dumpPartitionStats, err = strconv.ParseBool(dumpParams[0])
		if err != nil {
			handler.WriteError(w, err)
			return
		}
	}
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr(params[handler.DBName]), ast.NewCIStr(params[handler.TableName]))
	if err != nil {
		handler.WriteError(w, err)
	} else {
		js, err := h.DumpStatsToJSON(params[handler.DBName], tbl.Meta(), nil, dumpPartitionStats)
		if err != nil {
			handler.WriteError(w, err)
		} else {
			handler.WriteData(w, js)
		}
	}
}

// StatsPriorityQueueHandler is the handler for dumping the stats priority queue snapshot.
type StatsPriorityQueueHandler struct {
	do *domain.Domain
}

// NewStatsPriorityQueueHandler creates a new StatsPriorityQueueHandler.
func NewStatsPriorityQueueHandler(do *domain.Domain) *StatsPriorityQueueHandler {
	return &StatsPriorityQueueHandler{do: do}
}

// ServeHTTP dumps the stats priority queue snapshot to json.
func (sh StatsPriorityQueueHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	h := sh.do.StatsHandle()
	tables, err := h.GetPriorityQueueSnapshot()
	if err != nil {
		handler.WriteError(w, err)
	} else {
		handler.WriteData(w, tables)
	}
}

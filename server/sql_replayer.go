// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"github.com/pingcap/tidb/util/replayutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
)

// SQLRecorderHandler is the handler for dumping plan replayer file.
type SQLRecorderHandler struct {
	address    string
	statusPort uint
}

func (s *Server) newSQLRecorderHandler() *SQLRecorderHandler {
	cfg := config.GetGlobalConfig()
	prh := &SQLRecorderHandler{
		address:    cfg.AdvertiseAddress,
		statusPort: cfg.Status.StatusPort,
	}
	return prh
}

func (h SQLRecorderHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var err error
	cfg := config.GetGlobalConfig()
	params := mux.Vars(req)
	if status, ok := params[pRecordStatus]; ok {
		fmt.Println()
		if status == "on" {
			// set replay meta TS first.
			cfg.ReplayMetaTS, err = strconv.ParseInt(params[pStartTS], 10, 64)
			if err != nil {
				fmt.Println(err.Error())
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			cfg.EnableReplaySQL.Store(true)
			logutil.InitRecord(params[pFileName])
		} else {
			cfg.EnableReplaySQL.Store(false)
			logutil.StopRecord()
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusBadRequest)

}

// SQLReplayHandler Replay handler
type SQLReplayHandler struct {
	Store kv.Storage
}

func (s *Server) newSQLReplayHandler(store kv.Storage) *SQLReplayHandler {
	prh := &SQLReplayHandler{store}
	return prh
}

func (h SQLReplayHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	if status, ok := params[pReplayStatus]; ok {
		if status == "on" {
			replayutil.StartReplay(params[pFileName], h.Store)
		} else {
			replayutil.StopReplay()
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

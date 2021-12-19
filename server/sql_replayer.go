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
	"github.com/pingcap/tidb/util/logutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/config"
)

// SQLReplayerHandler is the handler for dumping plan replayer file.
type SQLReplayerHandler struct {
	address    string
	statusPort uint
}

func (s *Server) newSQLReplayerHandler() *SQLReplayerHandler {
	cfg := config.GetGlobalConfig()
	prh := &SQLReplayerHandler{
		address:    cfg.AdvertiseAddress,
		statusPort: cfg.Status.StatusPort,
	}
	return prh
}

func (prh SQLReplayerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var err error
	cfg := config.GetGlobalConfig()
	params := mux.Vars(req)
	if status, ok := params[pStatus]; ok {
		if status == "on" {
			// set replay meta TS first.
			cfg.ReplayMetaTS, err = strconv.ParseInt(params[pStartTS], 10, 64)
			if err != nil {
				fmt.Println(err.Error())
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			cfg.EnableReplaySQL.Store(true)
			logutil.InitReplay(params[pFileName])
		} else {
			cfg.EnableReplaySQL.Store(false)
			logutil.StopReplay()
		}
		w.WriteHeader(http.StatusOK)
		return
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}
}

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
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// PlanReplayerHandler is the handler for dumping plan replayer file.
type PlanReplayerHandler struct {
	infoGetter *infosync.InfoSyncer
	address    string
	statusPort uint
}

func (s *Server) newPlanReplayerHandler() *PlanReplayerHandler {
	cfg := config.GetGlobalConfig()
	prh := &PlanReplayerHandler{
		address:    cfg.AdvertiseAddress,
		statusPort: cfg.Status.StatusPort,
	}
	if s.dom != nil && s.dom.InfoSyncer() != nil {
		prh.infoGetter = s.dom.InfoSyncer()
	}
	return prh
}

func (prh PlanReplayerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[pFileName]
	path := filepath.Join(domain.GetPlanReplayerDirName(), name)
	if isExists(path) {
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename=\"plan_replayer.zip\"")
		file, err := os.Open(path)
		if err != nil {
			writeError(w, err)
			return
		}
		_, err = io.Copy(w, file)
		if err != nil {
			writeError(w, err)
			return
		}
		err = file.Close()
		if err != nil {
			writeError(w, err)
			return
		}
		err = os.Remove(path)
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if prh.infoGetter == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// we didn't find file for forward request, return 404
	forwarded := req.URL.Query().Get("forward")
	if len(forwarded) > 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// If we didn't find file in origin request, try to broadcast the request to all remote tidb-servers
	topos, err := prh.infoGetter.GetAllTiDBTopology(req.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	// transfer each remote tidb-server and try to find dump file
	for _, topo := range topos {
		if topo.IP == prh.address && topo.StatusPort == prh.statusPort {
			continue
		}
		url := fmt.Sprintf("http://%s:%v/plan_replayer/dump/%s?forward=true", topo.IP, topo.StatusPort, name)
		resp, err := http.Get(url) // nolint #nosec G107
		if err != nil {
			terror.Log(errors.Trace(err))
			logutil.BgLogger().Error("forward request failed", zap.String("addr", topo.IP), zap.Uint("port", topo.StatusPort), zap.Error(err))
			continue
		}
		defer terror.Call(resp.Body.Close)
		if resp.StatusCode != http.StatusOK {
			continue
		}
		// find dump file in one remote tidb-server, return file directly
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", "attachment; filename=\"plan_replayer.zip\"")
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	// we can't find dump file in any tidb-server, return 404 directly
	logutil.BgLogger().Info("can't find dump file in any remote server", zap.String("filename", name))
	w.WriteHeader(http.StatusNotFound)
}

func isExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && !os.IsExist(err) {
		return false
	}
	return true
}

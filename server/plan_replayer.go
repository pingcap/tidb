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
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// PlanReplayerHandler is the handler for dumping plan replayer file.
type PlanReplayerHandler struct {
	infoGetter *infosync.InfoSyncer
	address    string
	statusPort uint
	scheme     string
}

func (s *Server) newPlanReplayerHandler() *PlanReplayerHandler {
	cfg := config.GetGlobalConfig()
	prh := &PlanReplayerHandler{
		address:    cfg.AdvertiseAddress,
		statusPort: cfg.Status.StatusPort,
		scheme:     "http",
	}
	if s.dom != nil && s.dom.InfoSyncer() != nil {
		prh.infoGetter = s.dom.InfoSyncer()
	}
	if len(cfg.Security.ClusterSSLCA) > 0 {
		prh.scheme = "https"
	}
	return prh
}

func (prh PlanReplayerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[pFileName]
	handler := downloadFileHandler{
		filePath:           filepath.Join(domain.GetPlanReplayerDirName(), name),
		fileName:           name,
		infoGetter:         prh.infoGetter,
		address:            prh.address,
		statusPort:         prh.statusPort,
		urlPath:            fmt.Sprintf("plan_replyaer/dump/%s", name),
		downloadedFilename: "plan_replayer",
		scheme:             prh.scheme,
	}
	handleDownloadFile(handler, w, req)
}

func handleDownloadFile(handler downloadFileHandler, w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[pFileName]
	path := handler.filePath
	isForwarded := len(req.URL.Query().Get("forward")) > 0
	localAddr := fmt.Sprintf("%s:%v", handler.address, handler.statusPort)
	exist, err := isExists(path)
	if err != nil {
		writeError(w, err)
		return
	}
	if exist {
		file, err := os.Open(path)
		if err != nil {
			writeError(w, err)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			writeError(w, err)
			return
		}
		err = file.Close()
		if err != nil {
			writeError(w, err)
			return
		}
		_, err = w.Write(content)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", handler.downloadedFilename))
		logutil.BgLogger().Info("return dump file successfully", zap.String("filename", name),
			zap.String("address", localAddr), zap.Bool("forwarded", isForwarded))
		return
	}
	// handler.infoGetter will be nil only in unit test
	// or we couldn't find file for forward request, return 404
	if handler.infoGetter == nil || isForwarded {
		logutil.BgLogger().Info("failed to find dump file", zap.String("filename", name),
			zap.String("address", localAddr), zap.Bool("forwarded", isForwarded))
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// If we didn't find file in origin request, try to broadcast the request to all remote tidb-servers
	topos, err := handler.infoGetter.GetAllTiDBTopology(req.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	// transfer each remote tidb-server and try to find dump file
	for _, topo := range topos {
		if topo.IP == handler.address && topo.StatusPort == handler.statusPort {
			continue
		}
		remoteAddr := fmt.Sprintf("%s/%v", topo.IP, topo.StatusPort)
		url := fmt.Sprintf("%s://%s/%s?forward=true", handler.scheme, remoteAddr, handler.urlPath)
		resp, err := http.Get(url) // #nosec G107
		if err != nil {
			logutil.BgLogger().Error("forward request failed",
				zap.String("remote-addr", remoteAddr), zap.Error(err))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			logutil.BgLogger().Info("can't find file in remote server", zap.String("filename", name),
				zap.String("remote-addr", remoteAddr), zap.Int("status-code", resp.StatusCode))
			continue
		}
		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			writeError(w, err)
			return
		}
		err = resp.Body.Close()
		if err != nil {
			writeError(w, err)
			return
		}
		_, err = w.Write(content)
		if err != nil {
			writeError(w, err)
			return
		}
		// find dump file in one remote tidb-server, return file directly
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", handler.downloadedFilename))
		logutil.BgLogger().Info("return dump file successfully in remote server",
			zap.String("filename", name), zap.String("remote-addr", remoteAddr))
		return
	}
	// we can't find dump file in any tidb-server, return 404 directly
	logutil.BgLogger().Info("can't find dump file in any remote server", zap.String("filename", name))
	w.WriteHeader(http.StatusNotFound)
	_, err = w.Write([]byte(fmt.Sprintf("can't find dump file %s in any remote server", name)))
	writeError(w, err)
}

type downloadFileHandler struct {
	scheme             string
	filePath           string
	fileName           string
	infoGetter         *infosync.InfoSyncer
	address            string
	statusPort         uint
	urlPath            string
	downloadedFilename string
}

func isExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

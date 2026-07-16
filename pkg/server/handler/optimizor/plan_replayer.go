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

package optimizor

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"go.uber.org/zap"
)

// PlanReplayerHandler is the handler for dumping plan replayer file.
type PlanReplayerHandler struct {
	infoGetter *infosync.InfoSyncer
	address    string
	statusPort uint
}

// NewPlanReplayerHandler creates a new PlanReplayerHandler.
func NewPlanReplayerHandler(infoGetter *infosync.InfoSyncer, address string, statusPort uint) *PlanReplayerHandler {
	return &PlanReplayerHandler{
		infoGetter: infoGetter,
		address:    address,
		statusPort: statusPort,
	}
}

// ServeHTTP handles request of dumping plan replayer file.
func (prh PlanReplayerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[handler.FileName]
	if strings.HasPrefix(name, "capture_replayer_") {
		// Files with this prefix were dumped by older TiDB versions with
		// tidb_enable_historical_stats_for_capture enabled and contain no
		// stats/*.json entries: they relied on historical stats being injected
		// at download time, which was removed together with the historical
		// stats feature. Serve them as-is, but leave a trace for diagnosis.
		logutil.BgLogger().Warn("serving a plan replayer capture file created by an older version with historical stats for capture enabled; the bundle contains no statistics",
			zap.String("filename", name))
	}
	handler := downloadFileHandler{
		filePath:           filepath.Join(replayer.GetPlanReplayerDirName(), name),
		infoGetter:         prh.infoGetter,
		address:            prh.address,
		statusPort:         prh.statusPort,
		urlPath:            fmt.Sprintf("plan_replayer/dump/%s", name),
		downloadedFilename: "plan_replayer",
		scheme:             util.InternalHTTPSchema(),
	}
	handleDownloadFile(handler, w, req)
}

func handleDownloadFile(dfHandler downloadFileHandler, w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[handler.FileName]
	path := dfHandler.filePath
	isForwarded := len(req.URL.Query().Get("forward")) > 0
	localAddr := net.JoinHostPort(dfHandler.address, strconv.Itoa(int(dfHandler.statusPort)))

	ctx := req.Context()
	storage, err := extstore.GetGlobalExtStorage(ctx)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	exist, err := storage.FileExists(ctx, path)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	if exist {
		fileReader, err := storage.Open(ctx, path, nil)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		defer fileReader.Close()

		// Set headers BEFORE writing body
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", dfHandler.downloadedFilename))
		// Use streaming io.Copy instead of io.ReadAll to avoid memory bloat.
		_, err = io.Copy(w, fileReader)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		logutil.BgLogger().Info("return dump file successfully", zap.String("filename", name),
			zap.String("address", localAddr), zap.Bool("forwarded", isForwarded))
		return
	}
	// handler.infoGetter will be nil only in unit test
	// or we couldn't find file for forward request, return 404
	if dfHandler.infoGetter == nil || isForwarded {
		logutil.BgLogger().Info("failed to find dump file", zap.String("filename", name),
			zap.String("address", localAddr), zap.Bool("forwarded", isForwarded))
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// If we didn't find file in origin request, try to broadcast the request to all remote tidb-servers
	topos, err := dfHandler.infoGetter.ServerInfoSyncer().GetAllTiDBTopology(req.Context())
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	client := util.InternalHTTPClient()
	// transfer each remote tidb-server and try to find dump file
	for _, topo := range topos {
		if topo.IP == dfHandler.address && topo.StatusPort == dfHandler.statusPort {
			continue
		}
		remoteAddr := net.JoinHostPort(topo.IP, strconv.Itoa(int(topo.StatusPort)))
		url := fmt.Sprintf("%s://%s/%s?forward=true", dfHandler.scheme, remoteAddr, dfHandler.urlPath)
		resp, err := client.Get(url)
		if err != nil {
			logutil.BgLogger().Warn("forward request failed",
				zap.String("remote-addr", remoteAddr), zap.Error(err))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			logutil.BgLogger().Info("can't find file in remote server", zap.String("filename", name),
				zap.String("remote-addr", remoteAddr), zap.Int("status-code", resp.StatusCode))
			continue
		}
		// Set headers BEFORE writing body
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", dfHandler.downloadedFilename))
		// Use streaming io.Copy instead of io.ReadAll to avoid memory bloat
		_, err = io.Copy(w, resp.Body)
		resp.Body.Close()
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		// find dump file in one remote tidb-server, return file directly
		logutil.BgLogger().Info("return dump file successfully in remote server",
			zap.String("filename", name), zap.String("remote-addr", remoteAddr))
		return
	}
	// we can't find dump file in any tidb-server, return 404 directly
	logutil.BgLogger().Info("can't find dump file in any remote server", zap.String("filename", name))
	w.WriteHeader(http.StatusNotFound)
	_, err = fmt.Fprintf(w, "can't find dump file %s in any remote server", name)
	if err != nil {
		handler.WriteError(w, err)
	}
}

type downloadFileHandler struct {
	scheme             string
	filePath           string
	infoGetter         *infosync.InfoSyncer
	address            string
	statusPort         uint
	urlPath            string
	downloadedFilename string
}

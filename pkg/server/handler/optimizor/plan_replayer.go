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
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	util2 "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"go.uber.org/zap"
)

// PlanReplayerHandler is the handler for dumping plan replayer file.
type PlanReplayerHandler struct {
	is          infoschema.InfoSchema
	statsHandle *handle.Handle
	infoGetter  *infosync.InfoSyncer
	address     string
	statusPort  uint
}

// NewPlanReplayerHandler creates a new PlanReplayerHandler.
func NewPlanReplayerHandler(is infoschema.InfoSchema, statsHandle *handle.Handle, infoGetter *infosync.InfoSyncer, address string, statusPort uint) *PlanReplayerHandler {
	return &PlanReplayerHandler{
		is:          is,
		statsHandle: statsHandle,
		infoGetter:  infoGetter,
		address:     address,
		statusPort:  statusPort,
	}
}

// ServeHTTP handles request of dumping plan replayer file.
func (prh PlanReplayerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[handler.FileName]
	handler := downloadFileHandler{
		filePath:           filepath.Join(replayer.GetPlanReplayerDirName(), name),
		fileName:           name,
		infoGetter:         prh.infoGetter,
		address:            prh.address,
		statusPort:         prh.statusPort,
		urlPath:            fmt.Sprintf("plan_replayer/dump/%s", name),
		downloadedFilename: "plan_replayer",
		scheme:             util.InternalHTTPSchema(),
		statsHandle:        prh.statsHandle,
		is:                 prh.is,
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

		// For capture_replayer files, we need to read all content to process it
		if dfHandler.downloadedFilename == "plan_replayer" && strings.HasPrefix(dfHandler.fileName, "capture_replayer") {
			content, err := io.ReadAll(fileReader)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			content, err = handlePlanReplayerCaptureFile(content, dfHandler)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			// Set headers BEFORE writing body
			w.Header().Set("Content-Type", "application/zip")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", dfHandler.downloadedFilename))
			_, err = w.Write(content)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
		} else {
			// Set headers BEFORE writing body
			w.Header().Set("Content-Type", "application/zip")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", dfHandler.downloadedFilename))
			// Use streaming io.Copy instead of io.ReadAll to avoid memory bloat
			_, err = io.Copy(w, fileReader)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
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
	fileName           string
	infoGetter         *infosync.InfoSyncer
	address            string
	statusPort         uint
	urlPath            string
	downloadedFilename string

	statsHandle *handle.Handle
	is          infoschema.InfoSchema
}

// handlePlanReplayerCaptureFile handles capture_replayer files by adding historical stats.
// This function is called only when the file is a capture_replayer file (already checked by caller).
func handlePlanReplayerCaptureFile(content []byte, handler downloadFileHandler) ([]byte, error) {
	b := bytes.NewReader(content)
	zr, err := zip.NewReader(b, int64(len(content)))
	if err != nil {
		return nil, err
	}
	startTS, err := loadSQLMetaFile(zr)
	if err != nil {
		return nil, err
	}
	if startTS == 0 {
		return content, nil
	}
	tbls, err := loadSchemaMeta(zr, handler.is)
	if err != nil {
		return nil, err
	}
	for _, tbl := range tbls {
		jsonStats, _, err := handler.statsHandle.DumpHistoricalStatsBySnapshot(tbl.dbName, tbl.info, startTS)
		if err != nil {
			return nil, err
		}
		tbl.jsonStats = jsonStats
	}
	// Create a new zip with the additional stats in memory instead of writing to local filesystem
	return dumpJSONStatsIntoZipInMemory(tbls, content)
}

func loadSQLMetaFile(z *zip.Reader) (uint64, error) {
	for _, zipFile := range z.File {
		if zipFile.Name == domain.PlanReplayerSQLMetaFile {
			varMap := make(map[string]string)
			v, err := zipFile.Open()
			if err != nil {
				return 0, errors.AddStack(err)
			}
			//nolint: errcheck,all_revive,revive
			defer v.Close()
			_, err = toml.NewDecoder(v).Decode(&varMap)
			if err != nil {
				return 0, errors.AddStack(err)
			}
			startTS, err := strconv.ParseUint(varMap[domain.PlanReplayerSQLMetaStartTS], 10, 64)
			if err != nil {
				return 0, err
			}
			return startTS, nil
		}
	}
	return 0, nil
}

func loadSchemaMeta(z *zip.Reader, is infoschema.InfoSchema) (map[int64]*tblInfo, error) {
	r := make(map[int64]*tblInfo, 0)
	for _, zipFile := range z.File {
		if zipFile.Name == fmt.Sprintf("schema/%v", domain.PlanReplayerSchemaMetaFile) {
			v, err := zipFile.Open()
			if err != nil {
				return nil, errors.AddStack(err)
			}
			//nolint: errcheck,all_revive,revive
			defer v.Close()
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(v)
			if err != nil {
				return nil, errors.AddStack(err)
			}
			rows := strings.Split(buf.String(), "\n")
			for _, row := range rows {
				s := strings.Split(row, ";")
				databaseName := s[0]
				tableName := s[1]
				t, err := is.TableByName(context.Background(), ast.NewCIStr(databaseName), ast.NewCIStr(tableName))
				if err != nil {
					return nil, err
				}
				r[t.Meta().ID] = &tblInfo{
					info:    t.Meta(),
					dbName:  databaseName,
					tblName: tableName,
				}
			}
			break
		}
	}
	return r, nil
}

// dumpJSONStatsIntoZipInMemory creates a new zip with additional stats in memory.
func dumpJSONStatsIntoZipInMemory(tbls map[int64]*tblInfo, content []byte) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, err
	}
	// Create new zip in memory
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range zr.File {
		err = zw.Copy(f)
		if err != nil {
			logutil.BgLogger().Warn("copy plan replayer zip file failed", zap.Error(err))
			return nil, err
		}
	}
	for _, tbl := range tbls {
		w, err := zw.Create(fmt.Sprintf("stats/%v.%v.json", tbl.dbName, tbl.tblName))
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(tbl.jsonStats)
		if err != nil {
			return nil, err
		}
		_, err = w.Write(data)
		if err != nil {
			return nil, err
		}
	}
	err = zw.Close()
	if err != nil {
		logutil.BgLogger().Warn("Closing zip writer failed", zap.Error(err))
		return nil, err
	}
	return buf.Bytes(), nil
}

type tblInfo struct {
	info      *model.TableInfo
	jsonStats *util2.JSONTable
	dbName    string
	tblName   string
}

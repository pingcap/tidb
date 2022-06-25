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
	"net/http"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
)

// OptimizeTraceHandler serve http
type OptimizeTraceHandler struct {
	infoGetter *infosync.InfoSyncer
	address    string
	statusPort uint
	scheme     string
}

func (s *Server) newOptimizeTraceHandler() *OptimizeTraceHandler {
	cfg := config.GetGlobalConfig()
	oth := &OptimizeTraceHandler{
		address:    cfg.AdvertiseAddress,
		statusPort: cfg.Status.StatusPort,
		scheme:     "http",
	}
	if s.dom != nil && s.dom.InfoSyncer() != nil {
		oth.infoGetter = s.dom.InfoSyncer()
	}
	if len(cfg.Security.ClusterSSLCA) > 0 {
		oth.scheme = "https"
	}
	return oth
}

func (oth OptimizeTraceHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[pFileName]
	handler := downloadFileHandler{
		filePath:           filepath.Join(domain.GetOptimizerTraceDirName(), name),
		fileName:           name,
		infoGetter:         oth.infoGetter,
		address:            oth.address,
		statusPort:         oth.statusPort,
		urlPath:            fmt.Sprintf("optimize_trace/dump/%s", name),
		downloadedFilename: "optimize_trace",
		scheme:             oth.scheme,
	}
	handleDownloadFile(handler, w, req)
}

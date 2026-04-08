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
	"net/http"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/util"
)

// OptimizeTraceHandler serve http
type OptimizeTraceHandler struct {
	infoGetter *infosync.InfoSyncer
	address    string
	statusPort uint
}

// NewOptimizeTraceHandler returns a new OptimizeTraceHandler
func NewOptimizeTraceHandler(infoGetter *infosync.InfoSyncer, address string, statusPort uint) *OptimizeTraceHandler {
	return &OptimizeTraceHandler{
		infoGetter: infoGetter,
		address:    address,
		statusPort: statusPort,
	}
}

func (oth OptimizeTraceHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	name := params[handler.FileName]
	handler := downloadFileHandler{
		filePath:           filepath.Join(domain.GetOptimizerTraceDirName(), name),
		fileName:           name,
		infoGetter:         oth.infoGetter,
		address:            oth.address,
		statusPort:         oth.statusPort,
		urlPath:            fmt.Sprintf("optimize_trace/dump/%s", name),
		downloadedFilename: "optimize_trace",
		scheme:             util.InternalHTTPSchema(),
	}
	handleDownloadFile(handler, w, req)
}

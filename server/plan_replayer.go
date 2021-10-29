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
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/terror"
)

// PlanReplayerHandler is the handler for dumping plan replayer file.
type PlanReplayerHandler struct {
}

func (s *Server) newPlanReplayerHandler() *PlanReplayerHandler {
	return &PlanReplayerHandler{}
}

func (prh PlanReplayerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=\"plan_replayer.zip\"")

	params := mux.Vars(req)

	name := params[pFileName]
	path := filepath.Join(domain.GetPlanReplayerDirName(), name)
	file, err := os.Open(path)
	if err != nil {
		writeError(w, err)
	} else {
		_, err := io.Copy(w, file)
		if err != nil {
			terror.Log(errors.Trace(err))
		}
	}
	err = file.Close()
	if err != nil {
		terror.Log(errors.Trace(err))
	}
	err = os.Remove(path)
	if err != nil {
		terror.Log(errors.Trace(err))
	}
}

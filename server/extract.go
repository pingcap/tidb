// Copyright 2023 PingCAP, Inc.
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
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	extractPlanTaskType = "plan"
)

// ExtractTaskServeHandler is the http serve handler for extract task handler
type ExtractTaskServeHandler struct {
	extractHandler *domain.ExtractHandle
}

// newExtractServeHandler returns extractTaskServeHandler
func (s *Server) newExtractServeHandler() *ExtractTaskServeHandler {
	esh := &ExtractTaskServeHandler{}
	if s.dom != nil {
		esh.extractHandler = s.dom.GetExtractHandle()
	}
	return esh
}

// ServeHTTP serves http
func (eh ExtractTaskServeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	task, isDump, err := buildExtractTask(req)
	if err != nil {
		logutil.BgLogger().Error("build extract task failed", zap.Error(err))
		writeError(w, err)
		return
	}
	failpoint.Inject("extractTaskServeHandler", func(val failpoint.Value) {
		if val.(bool) {
			w.WriteHeader(http.StatusOK)
			_, err = w.Write([]byte("mock"))
			if err != nil {
				writeError(w, err)
			}
			failpoint.Return()
		}
	})

	name, err := eh.extractHandler.ExtractTask(context.Background(), task)
	if err != nil {
		logutil.BgLogger().Error("extract task failed", zap.Error(err))
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	if !isDump {
		_, err = w.Write([]byte(name))
		if err != nil {
			logutil.BgLogger().Error("extract handler failed", zap.Error(err))
		}
		return
	}
	content, err := loadExtractResponse(name)
	if err != nil {
		logutil.BgLogger().Error("load extract task failed", zap.Error(err))
		writeError(w, err)
		return
	}
	_, err = w.Write(content)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", name))
}

func loadExtractResponse(name string) ([]byte, error) {
	path := filepath.Join(domain.GetExtractTaskDirName(), name)
	//nolint: gosec
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func buildExtractTask(req *http.Request) (*domain.ExtractTask, bool, error) {
	extractTaskType := req.URL.Query().Get(pType)
	switch strings.ToLower(extractTaskType) {
	case extractPlanTaskType:
		return buildExtractPlanTask(req)
	}
	logutil.BgLogger().Error("unknown extract task type")
	return nil, false, errors.New("unknown extract task type")
}

func buildExtractPlanTask(req *http.Request) (*domain.ExtractTask, bool, error) {
	beginStr := req.URL.Query().Get(pBegin)
	endStr := req.URL.Query().Get(pEnd)
	begin, err := time.Parse(types.TimeFormat, beginStr)
	if err != nil {
		logutil.BgLogger().Error("extract task begin time failed", zap.Error(err), zap.String("begin", beginStr))
		return nil, false, err
	}
	end, err := time.Parse(types.TimeFormat, endStr)
	if err != nil {
		logutil.BgLogger().Error("extract task end time failed", zap.Error(err), zap.String("end", endStr))
		return nil, false, err
	}
	isDumpStr := req.URL.Query().Get(pIsDump)
	isDump, err := strconv.ParseBool(isDumpStr)
	if err != nil {
		isDump = false
	}
	return &domain.ExtractTask{
		ExtractType:     domain.ExtractPlanType,
		IsBackgroundJob: false,
		Begin:           begin,
		End:             end,
	}, isDump, nil
}

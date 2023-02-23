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
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
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
	task, err := buildExtractTask(req)
	if err != nil {
		logutil.BgLogger().Error("build extract task failed", zap.Error(err))
		writeError(w, err)
		return
	}
	_, err = eh.extractHandler.ExtractTask(context.Background(), task)
	if err != nil {
		logutil.BgLogger().Error("extract task failed", zap.Error(err))
		writeError(w, err)
		return
	}
	// TODO: support return zip file directly for non background job later
	w.WriteHeader(http.StatusOK)
	return
}

func buildExtractTask(req *http.Request) (*domain.ExtractTask, error) {
	extractTaskType := req.URL.Query().Get(pType)
	switch strings.ToLower(extractTaskType) {
	case extractPlanTaskType:
		return buildExtractPlanTask(req)
	}
	logutil.BgLogger().Error("unknown extract task type")
	return nil, errors.New("unknown extract task type")
}

func buildExtractPlanTask(req *http.Request) (*domain.ExtractTask, error) {
	beginStr := req.URL.Query().Get(pBegin)
	endStr := req.URL.Query().Get(pEnd)
	begin, err := time.Parse(types.TimeFormat, beginStr)
	if err != nil {
		return nil, err
	}
	end, err := time.Parse(types.TimeFormat, endStr)
	if err != nil {
		return nil, err
	}
	isBackgroundJobStr := req.URL.Query().Get(pIsBackground)
	var isBackgroundJob bool
	isBackgroundJob, err = strconv.ParseBool(isBackgroundJobStr)
	if err != nil {
		isBackgroundJob = false
	}
	return &domain.ExtractTask{
		ExtractType:     domain.ExtractPlanType,
		IsBackgroundJob: isBackgroundJob,
		Begin:           begin,
		End:             end,
	}, nil
}

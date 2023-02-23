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

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/types"
)

const (
	extractPlanTaskType = "plan"
)

// ExtractTaskServeHandler is the http serve handler for extract task handler
type extractTaskServeHandler struct {
	extractHandler *domain.ExtractHandle
}

// newExtractServeHandler returns extractTaskServeHandler
func (s *Server) newExtractServeHandler() *extractTaskServeHandler {
	return &extractTaskServeHandler{
		extractHandler: s.dom.GetExtractHandle(),
	}
}

// ServeHTTP serves http
func (eh *extractTaskServeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	task, err := buildExtractTask(params)
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = eh.extractHandler.ExtractTask(context.Background(), task)
	if err != nil {
		writeError(w, err)
		return
	}
	// TODO: support return zip file directly for non background job later
	w.WriteHeader(http.StatusOK)
	return
}

func buildExtractTask(params map[string]string) (*domain.ExtractTask, error) {
	extractTaskType := params[pType]
	switch strings.ToLower(extractTaskType) {
	case extractPlanTaskType:
		return buildExtractPlanTask(params)
	}
	return nil, errors.New("unknown extract task type")
}

func buildExtractPlanTask(params map[string]string) (*domain.ExtractTask, error) {
	beginStr := params[pBegin]
	endStr := params[pEnd]
	begin, err := time.Parse(types.DateFormat, beginStr)
	if err != nil {
		return nil, err
	}
	end, err := time.Parse(types.DateFormat, endStr)
	if err != nil {
		return nil, err
	}
	isBackgroundJobStr := params[pIsBackground]
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

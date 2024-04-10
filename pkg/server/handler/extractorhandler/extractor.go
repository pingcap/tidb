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

package extractorhandler

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
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	extractPlanTaskType = "plan"
)

// ExtractTaskServeHandler is the http serve handler for extract task handler
type ExtractTaskServeHandler struct {
	ExtractHandler *domain.ExtractHandle
}

// NewExtractTaskServeHandler creates a new extract task serve handler
func NewExtractTaskServeHandler(extractHandler *domain.ExtractHandle) *ExtractTaskServeHandler {
	return &ExtractTaskServeHandler{ExtractHandler: extractHandler}
}

// ServeHTTP serves http
func (eh ExtractTaskServeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	task, isDump, err := buildExtractTask(req)
	if err != nil {
		logutil.BgLogger().Error("build extract task failed", zap.Error(err))
		handler.WriteError(w, err)
		return
	}
	failpoint.Inject("extractTaskServeHandler", func(val failpoint.Value) {
		if val.(bool) {
			w.WriteHeader(http.StatusOK)
			_, err = w.Write([]byte("mock"))
			if err != nil {
				handler.WriteError(w, err)
			}
			failpoint.Return()
		}
	})

	name, err := eh.ExtractHandler.ExtractTask(context.Background(), task)
	if err != nil {
		logutil.BgLogger().Error("extract task failed", zap.Error(err))
		handler.WriteError(w, err)
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
		handler.WriteError(w, err)
		return
	}
	_, err = w.Write(content)
	if err != nil {
		handler.WriteError(w, err)
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
	extractTaskType := req.URL.Query().Get(handler.Type)
	if strings.ToLower(extractTaskType) == extractPlanTaskType {
		return buildExtractPlanTask(req)
	}
	logutil.BgLogger().Error("unknown extract task type")
	return nil, false, errors.New("unknown extract task type")
}

func buildExtractPlanTask(req *http.Request) (*domain.ExtractTask, bool, error) {
	beginStr := req.URL.Query().Get(handler.Begin)
	endStr := req.URL.Query().Get(handler.End)
	var begin time.Time
	var err error
	if len(beginStr) < 1 {
		begin = time.Now().Add(30 * time.Minute)
	} else {
		begin, err = time.Parse(types.TimeFormat, beginStr)
		if err != nil {
			logutil.BgLogger().Error("extract task begin time failed", zap.Error(err), zap.String("begin", beginStr))
			return nil, false, err
		}
	}
	var end time.Time
	if len(endStr) < 1 {
		end = time.Now()
	} else {
		end, err = time.Parse(types.TimeFormat, endStr)
		if err != nil {
			logutil.BgLogger().Error("extract task end time failed", zap.Error(err), zap.String("end", endStr))
			return nil, false, err
		}
	}
	isDump := extractBoolParam(handler.IsDump, false, req)

	return &domain.ExtractTask{
		ExtractType:     domain.ExtractPlanType,
		IsBackgroundJob: false,
		Begin:           begin,
		End:             end,
		SkipStats:       extractBoolParam(handler.IsSkipStats, false, req),
		UseHistoryView:  extractBoolParam(handler.IsHistoryView, true, req),
	}, isDump, nil
}

func extractBoolParam(param string, defaultValue bool, req *http.Request) bool {
	str := req.URL.Query().Get(param)
	if len(str) < 1 {
		return defaultValue
	}
	v, err := strconv.ParseBool(str)
	if err != nil {
		return defaultValue
	}
	return v
}

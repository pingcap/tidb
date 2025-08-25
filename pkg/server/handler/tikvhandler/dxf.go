// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikvhandler

import (
	"context"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/schstatus"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	pauseScaleInAction     = "pause_scale_in"
	resumeScaleInAction    = "resume_scale_in"
	pauseScaleInDefaultTTL = time.Hour
)

// DXFScheduleStatusHandler handles the status of DXF schedule.
type DXFScheduleStatusHandler struct {
	store kv.Storage
}

// NewDXFScheduleStatusHandler creates a new DXFScheduleStatusHandler.
func NewDXFScheduleStatusHandler(store kv.Storage) *DXFScheduleStatusHandler {
	return &DXFScheduleStatusHandler{store}
}

// ServeHTTP handles request of resigning ddl owner.
func (h *DXFScheduleStatusHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		handler.WriteError(w, errors.Errorf("This api only support GET method"))
		return
	}
	if h.store.GetKeyspace() != keyspace.System {
		handler.WriteError(w, errors.Errorf("This api only support SYSTEM keyspace, current keyspace is %s", h.store.GetKeyspace()))
		return
	}

	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	ctx, cancel := context.WithTimeout(ctx, requestDefaultTimeout)
	defer cancel()
	status, err := handle.GetScheduleStatus(ctx)
	if err != nil {
		logutil.BgLogger().Warn("failed to get DXF schedule status", zap.Error(err))
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	logutil.BgLogger().Info("current DXF schedule status", zap.Stringer("status", status))
	handler.WriteData(w, status)
}

// DXFScheduleHandler handles the DXF schedule actions.
type DXFScheduleHandler struct {
	store kv.Storage
}

// NewDXFScheduleHandler creates a new DXFScheduleHandler.
func NewDXFScheduleHandler(store kv.Storage) *DXFScheduleHandler {
	return &DXFScheduleHandler{store: store}
}

func (h *DXFScheduleHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}
	if h.store.GetKeyspace() != keyspace.System {
		handler.WriteError(w, errors.Errorf("This api only support SYSTEM keyspace, current keyspace is %s", h.store.GetKeyspace()))
		return
	}
	name, param, err := parsePauseScaleInFlag(req)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	logutil.BgLogger().Info("DXF schedule flag", zap.String("name", string(name)), zap.Stringer("param", param))
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	ctx, cancel := context.WithTimeout(ctx, requestDefaultTimeout)
	defer cancel()
	if err := handle.UpdatePauseScaleInFlag(ctx, param); err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError,
			errors.Errorf("failed to update pause scale-in flag, error %v", err))
		return
	}
	handler.WriteData(w, param)
}

func parsePauseScaleInFlag(req *http.Request) (schstatus.Flag, *schstatus.TTLFlag, error) {
	actionStr := req.FormValue("action")
	if actionStr != pauseScaleInAction && actionStr != resumeScaleInAction {
		return "", nil, errors.Errorf("invalid action %s", actionStr)
	}
	ttlFlag := &schstatus.TTLFlag{
		Enabled: actionStr == pauseScaleInAction,
	}
	if ttlFlag.Enabled {
		var (
			err error
			ttl = pauseScaleInDefaultTTL
		)
		ttlStr := req.FormValue("ttl")
		if ttlStr != "" {
			ttl, err = time.ParseDuration(ttlStr)
			if err != nil {
				return "", nil, errors.Errorf("invalid ttl %s, error %v", ttlStr, err)
			}
		}
		ttlFlag.TTL = ttl
		ttlFlag.ExpireTime = time.Now().Add(ttl)
	}
	return schstatus.PauseScaleInFlag, ttlFlag, nil
}

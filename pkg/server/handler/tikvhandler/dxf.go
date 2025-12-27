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
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/schstatus"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/naming"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	pauseScaleInAction     = "pause_scale_in"
	resumeScaleInAction    = "resume_scale_in"
	dxfOperationDefaultTTL = time.Hour
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

	ctx, cancel := context.WithTimeout(context.Background(), requestDefaultTimeout)
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
	name, param, err := parsePauseScaleInFlag(req)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	logutil.BgLogger().Info("DXF schedule flag", zap.String("name", string(name)), zap.Stringer("param", param))
	ctx, cancel := context.WithTimeout(context.Background(), requestDefaultTimeout)
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
		ttlInfo, err := parseTTLInfo(req)
		if err != nil {
			return "", nil, err
		}
		ttlFlag.TTLInfo = *ttlInfo
	}
	return schstatus.PauseScaleInFlag, ttlFlag, nil
}

func parseTTLInfo(req *http.Request) (*schstatus.TTLInfo, error) {
	var (
		err error
		ttl = dxfOperationDefaultTTL
	)
	ttlStr := req.FormValue("ttl")
	if ttlStr != "" {
		ttl, err = time.ParseDuration(ttlStr)
		if err != nil {
			return nil, errors.Errorf("invalid ttl %s, error %v", ttlStr, err)
		}
	}
	return &schstatus.TTLInfo{
		TTL:        ttl,
		ExpireTime: time.Now().Add(ttl),
	}, nil
}

// DXFScheduleTuneHandler handles the DXF schedule tuning factors.
type DXFScheduleTuneHandler struct {
	store kv.Storage
}

// NewDXFScheduleTuneHandler creates a new DXFScheduleTuneHandler.
func NewDXFScheduleTuneHandler(storage kv.Storage) http.Handler {
	return &DXFScheduleTuneHandler{store: storage}
}

func (h *DXFScheduleTuneHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	targetKeyspace := req.FormValue("keyspace")
	if targetKeyspace == "" || naming.CheckKeyspaceName(targetKeyspace) != nil {
		handler.WriteError(w, errors.Errorf("invalid or empty target keyspace %s", targetKeyspace))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestDefaultTimeout)
	defer cancel()
	if storageWithPD, ok := h.store.(kv.StorageWithPD); ok {
		_, err := storageWithPD.GetPDClient().LoadKeyspace(ctx, targetKeyspace)
		if err != nil {
			logutil.BgLogger().Warn("failed to load keyspace from PD", zap.String("keyspace", targetKeyspace), zap.Error(err))
			handler.WriteError(w, errors.Annotatef(err, "failed to load keyspace %s from PD", targetKeyspace))
			return
		}
	}
	switch req.Method {
	case http.MethodGet:
		factors, err := handle.GetScheduleTuneFactors(ctx, targetKeyspace)
		if err != nil {
			logutil.BgLogger().Warn("failed to get DXF schedule tune factors", zap.Error(err))
			handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
			return
		}
		handler.WriteData(w, factors)
		return
	case http.MethodPost:
		ttlInfo, err := parseTTLInfo(req)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		factorStr := req.FormValue("amplify_factor")
		factorVal, err := strconv.ParseFloat(factorStr, 64)
		if err != nil {
			handler.WriteError(w, errors.Errorf("invalid amplify_factor %s, error %v", factorStr, err))
			return
		}
		if factorVal < schstatus.MinAmplifyFactor || factorVal > schstatus.MaxAmplifyFactor {
			handler.WriteError(w, errors.Errorf("amplify_factor %f is out of range [%f, %f]",
				factorVal, schstatus.MinAmplifyFactor, schstatus.MaxAmplifyFactor))
			return
		}
		ttlTuneFactors := &schstatus.TTLTuneFactors{
			TTLInfo: *ttlInfo,
			TuneFactors: schstatus.TuneFactors{
				AmplifyFactor: factorVal,
			},
		}
		ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
		if err = kv.RunInNewTxn(ctx, h.store, true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMutator(txn)
			return m.SetDXFScheduleTuneFactors(targetKeyspace, ttlTuneFactors)
		}); err != nil {
			logutil.BgLogger().Warn("failed to set DXF schedule tune factors", zap.Error(err))
			handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
			return
		}
		logutil.BgLogger().Info("set DXF schedule tune factors",
			zap.String("keyspace", targetKeyspace), zap.Stringer("factors", ttlTuneFactors))
		handler.WriteData(w, ttlTuneFactors)
	default:
		handler.WriteError(w, errors.Errorf("This api only support GET and POST method"))
		return
	}
}

// DXFTaskMaxRuntimeSlotsHandler handles changing max runtime slots of DXF task.
type DXFTaskMaxRuntimeSlotsHandler struct {
	store kv.Storage
}

// NewDXFTaskMaxRuntimeSlotsHandler creates a new DXFTaskMaxRuntimeSlotsHandler.
func NewDXFTaskMaxRuntimeSlotsHandler(storage kv.Storage) *DXFTaskMaxRuntimeSlotsHandler {
	return &DXFTaskMaxRuntimeSlotsHandler{store: storage}
}

// ServeHTTP implements http.Handler interface.
func (h *DXFTaskMaxRuntimeSlotsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}
	taskID, maxRuntimeSlots, steps, err := func() (int64, int, []proto.Step, error) {
		params := mux.Vars(req)
		var (
			taskID int64
			steps  []proto.Step
		)
		if val, ok := params["taskID"]; ok {
			intVal, err := strconv.Atoi(val)
			if err != nil {
				return 0, 0, nil, errors.Errorf("invalid task ID %s, error %v", val, err)
			}
			taskID = int64(intVal)
		}
		if taskID <= 0 {
			return 0, 0, nil, errors.New("invalid task ID")
		}

		if err := req.ParseForm(); err != nil {
			return 0, 0, nil, err
		}
		strVal := req.FormValue("value")
		maxRuntimeSlots, err := strconv.Atoi(strVal)
		if err != nil {
			return 0, 0, nil, errors.Errorf("invalid value %s, error %v", strVal, err)
		}
		if maxRuntimeSlots <= 0 {
			return 0, 0, nil, errors.Errorf("invalid value %d", maxRuntimeSlots)
		}

		strSteps := req.Form["target_step"]
		if len(strSteps) > 0 {
			steps = make([]proto.Step, 0, len(strSteps))
			for _, str := range strSteps {
				step, err := strconv.Atoi(str)
				if err != nil {
					return 0, 0, nil, errors.Errorf("invalid target step %s, error %v", str, err)
				}
				steps = append(steps, proto.Step(step))
			}
		}
		return taskID, maxRuntimeSlots, steps, nil
	}()
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestDefaultTimeout)
	defer cancel()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	task, err := taskMgr.GetTaskByID(ctx, taskID)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	if maxRuntimeSlots >= task.RequiredSlots {
		handler.WriteError(w, errors.Errorf("max runtime slots should be less than required slots(%d)", task.RequiredSlots))
		return
	}
	stepStrs := make([]string, 0, len(steps))
	for _, step := range steps {
		if !proto.IsValidStep(task.Type, step) {
			handler.WriteError(w, errors.Errorf("invalid target step %d for task type %s", step, task.Type.String()))
			return
		}
		stepStrs = append(stepStrs, proto.Step2Str(task.Type, step))
	}
	params := proto.ExtraParams{
		MaxRuntimeSlots: maxRuntimeSlots,
		TargetSteps:     steps,
	}
	if err := taskMgr.UpdateTaskExtraParams(ctx, taskID, params); err != nil {
		handler.WriteErrorWithCode(w, http.StatusInternalServerError, err)
		return
	}
	logutil.BgLogger().Info("set DXF task max runtime slots",
		zap.Int64("taskID", taskID), zap.String("taskKey", task.Key),
		zap.Int("maxRuntimeSlots", maxRuntimeSlots), zap.Any("targetSteps", steps))
	handler.WriteData(w, map[string]any{
		"task_id":           taskID,
		"task_key":          task.Key,
		"required_slots":    task.RequiredSlots,
		"max_runtime_slots": maxRuntimeSlots,
		"target_steps":      stepStrs,
	})
}

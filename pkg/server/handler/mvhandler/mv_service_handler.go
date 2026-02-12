// Copyright 2026 PingCAP, Inc.
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

package mvhandler

import (
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/mvs"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
)

const (
	mvServiceTaskMaxConcurrencyKey = "task_max_concurrency"
	mvServiceTaskTimeoutKey        = "task_timeout"

	mvServiceTaskBackpressureEnabledKey      = "task_backpressure_enabled"
	mvServiceTaskBackpressureCPUThresholdKey = "task_backpressure_cpu_threshold"
	mvServiceTaskBackpressureMemThresholdKey = "task_backpressure_mem_threshold"
	mvServiceTaskBackpressureDelayKey        = "task_backpressure_delay"

	mvServiceRetryBaseDelayKey = "retry_base_delay"
	mvServiceRetryMaxDelayKey  = "retry_max_delay"
)

// MVServiceSettingsHandler is the handler for runtime MV service settings.
type MVServiceSettingsHandler struct {
	*handler.TikvHandlerTool
}

// NewMVServiceSettingsHandler creates a new MVServiceSettingsHandler.
func NewMVServiceSettingsHandler(tool *handler.TikvHandlerTool) *MVServiceSettingsHandler {
	return &MVServiceSettingsHandler{TikvHandlerTool: tool}
}

// MVServiceTaskExecConfig is MV service task execution config.
type MVServiceTaskExecConfig struct {
	TaskMaxConcurrency           int     `json:"task_max_concurrency"`
	TaskTimeout                  string  `json:"task_timeout"`
	TaskTimeoutNanos             int64   `json:"task_timeout_nanos"`
	TaskBackpressureEnabled      bool    `json:"task_backpressure_enabled"`
	TaskBackpressureCPUThreshold float64 `json:"task_backpressure_cpu_threshold"`
	TaskBackpressureMemThreshold float64 `json:"task_backpressure_mem_threshold"`
	TaskBackpressureDelay        string  `json:"task_backpressure_delay"`
	TaskBackpressureDelayNanos   int64   `json:"task_backpressure_delay_nanos"`
	RetryBaseDelay               string  `json:"retry_base_delay"`
	RetryBaseDelayNanos          int64   `json:"retry_base_delay_nanos"`
	RetryMaxDelay                string  `json:"retry_max_delay"`
	RetryMaxDelayNanos           int64   `json:"retry_max_delay_nanos"`
}

func writeMVServiceTaskExecConfig(
	w http.ResponseWriter,
	maxConcurrency int,
	timeout time.Duration,
	backpressureCfg mvs.TaskBackpressureConfig,
	retryBase time.Duration,
	retryMax time.Duration,
) {
	handler.WriteData(w, MVServiceTaskExecConfig{
		TaskMaxConcurrency:           maxConcurrency,
		TaskTimeout:                  timeout.String(),
		TaskTimeoutNanos:             int64(timeout),
		TaskBackpressureEnabled:      backpressureCfg.Enabled,
		TaskBackpressureCPUThreshold: backpressureCfg.CPUThreshold,
		TaskBackpressureMemThreshold: backpressureCfg.MemThreshold,
		TaskBackpressureDelay:        backpressureCfg.Delay.String(),
		TaskBackpressureDelayNanos:   int64(backpressureCfg.Delay),
		RetryBaseDelay:               retryBase.String(),
		RetryBaseDelayNanos:          int64(retryBase),
		RetryMaxDelay:                retryMax.String(),
		RetryMaxDelayNanos:           int64(retryMax),
	})
}

// ServeHTTP handles request of get/update MV service settings.
func (h MVServiceSettingsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	do, err := session.GetDomain(h.Store)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	mvService := do.GetMVService()
	if mvService == nil {
		handler.WriteError(w, errors.New("mv service is not enabled"))
		return
	}

	switch req.Method {
	case http.MethodGet:
		maxConcurrency, timeout := mvService.GetTaskExecConfig()
		backpressureCfg := mvService.GetTaskBackpressureConfig()
		retryBase, retryMax := mvService.GetRetryDelayConfig()
		writeMVServiceTaskExecConfig(w, maxConcurrency, timeout, backpressureCfg, retryBase, retryMax)
	case http.MethodPost:
		err = req.ParseForm()
		if err != nil {
			handler.WriteError(w, err)
			return
		}

		changed := false
		maxConcurrency, timeout := mvService.GetTaskExecConfig()
		backpressureCfg := mvService.GetTaskBackpressureConfig()
		retryBase, retryMax := mvService.GetRetryDelayConfig()

		maxConcurrencyText := req.Form.Get(mvServiceTaskMaxConcurrencyKey)
		if maxConcurrencyText != "" {
			maxConcurrency, err = strconv.Atoi(maxConcurrencyText)
			if err != nil || maxConcurrency <= 0 {
				handler.WriteError(w, errors.New("illegal task_max_concurrency"))
				return
			}
			changed = true
		}

		timeoutText := req.Form.Get(mvServiceTaskTimeoutKey)
		if timeoutText != "" {
			timeout, err = time.ParseDuration(timeoutText)
			if err != nil || timeout < 0 {
				handler.WriteError(w, errors.New("illegal task_timeout"))
				return
			}
			changed = true
		}

		backpressureEnabledText := req.Form.Get(mvServiceTaskBackpressureEnabledKey)
		if backpressureEnabledText != "" {
			backpressureCfg.Enabled, err = strconv.ParseBool(backpressureEnabledText)
			if err != nil {
				handler.WriteError(w, errors.New("illegal task_backpressure_enabled"))
				return
			}
			changed = true
		}

		backpressureCPUThresholdText := req.Form.Get(mvServiceTaskBackpressureCPUThresholdKey)
		if backpressureCPUThresholdText != "" {
			backpressureCfg.CPUThreshold, err = strconv.ParseFloat(backpressureCPUThresholdText, 64)
			if err != nil {
				handler.WriteError(w, errors.New("illegal task_backpressure_cpu_threshold"))
				return
			}
			changed = true
		}

		backpressureMemThresholdText := req.Form.Get(mvServiceTaskBackpressureMemThresholdKey)
		if backpressureMemThresholdText != "" {
			backpressureCfg.MemThreshold, err = strconv.ParseFloat(backpressureMemThresholdText, 64)
			if err != nil {
				handler.WriteError(w, errors.New("illegal task_backpressure_mem_threshold"))
				return
			}
			changed = true
		}

		backpressureDelayText := req.Form.Get(mvServiceTaskBackpressureDelayKey)
		if backpressureDelayText != "" {
			backpressureCfg.Delay, err = time.ParseDuration(backpressureDelayText)
			if err != nil || backpressureCfg.Delay < 0 {
				handler.WriteError(w, errors.New("illegal task_backpressure_delay"))
				return
			}
			changed = true
		}

		retryBaseDelayText := req.Form.Get(mvServiceRetryBaseDelayKey)
		if retryBaseDelayText != "" {
			retryBase, err = time.ParseDuration(retryBaseDelayText)
			if err != nil || retryBase <= 0 {
				handler.WriteError(w, errors.New("illegal retry_base_delay"))
				return
			}
			changed = true
		}

		retryMaxDelayText := req.Form.Get(mvServiceRetryMaxDelayKey)
		if retryMaxDelayText != "" {
			retryMax, err = time.ParseDuration(retryMaxDelayText)
			if err != nil || retryMax <= 0 {
				handler.WriteError(w, errors.New("illegal retry_max_delay"))
				return
			}
			changed = true
		}

		if !changed {
			handler.WriteError(w, errors.New("at least one setting field must be provided"))
			return
		}

		mvService.SetTaskExecConfig(maxConcurrency, timeout)
		if err = mvService.SetTaskBackpressureConfig(backpressureCfg); err != nil {
			handler.WriteError(w, err)
			return
		}
		if err = mvService.SetRetryDelayConfig(retryBase, retryMax); err != nil {
			handler.WriteError(w, err)
			return
		}

		maxConcurrency, timeout = mvService.GetTaskExecConfig()
		backpressureCfg = mvService.GetTaskBackpressureConfig()
		retryBase, retryMax = mvService.GetRetryDelayConfig()
		writeMVServiceTaskExecConfig(w, maxConcurrency, timeout, backpressureCfg, retryBase, retryMax)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

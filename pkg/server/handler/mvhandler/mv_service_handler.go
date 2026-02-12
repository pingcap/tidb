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
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/mvs"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
)

const (
	mvServiceTaskMaxConcurrencyFormField = "task_max_concurrency"
	mvServiceTaskTimeoutFormField        = "task_timeout"

	mvServiceBackpressureEnabledFormField      = "backpressure_enabled"
	mvServiceBackpressureCPUThresholdFormField = "backpressure_cpu_threshold"
	mvServiceBackpressureMemThresholdFormField = "backpressure_mem_threshold"
	mvServiceBackpressureDelayFormField        = "backpressure_delay"

	mvServiceTaskFailRetryBaseDelayFormField = "task_fail_retry_base_delay"
	mvServiceTaskFailRetryMaxDelayFormField  = "task_fail_retry_max_delay"
)

// MVServiceSettingsHandler is the handler for runtime MV service settings.
type MVServiceSettingsHandler struct {
	*handler.TikvHandlerTool
}

// NewMVServiceSettingsHandler creates a new MVServiceSettingsHandler.
func NewMVServiceSettingsHandler(tool *handler.TikvHandlerTool) *MVServiceSettingsHandler {
	return &MVServiceSettingsHandler{TikvHandlerTool: tool}
}

type mvServiceRuntimeSettingsAccessor interface {
	GetTaskExecConfig() (maxConcurrency int, timeout time.Duration)
	SetTaskExecConfig(maxConcurrency int, timeout time.Duration)

	GetTaskBackpressureConfig() mvs.TaskBackpressureConfig
	SetTaskBackpressureConfig(cfg mvs.TaskBackpressureConfig) error

	GetRetryDelayConfig() (base, max time.Duration)
	SetRetryDelayConfig(base, max time.Duration) error
}

type mvServiceRuntimeSettings struct {
	maxConcurrency int
	timeout        time.Duration

	backpressureCfg mvs.TaskBackpressureConfig

	retryBase time.Duration
	retryMax  time.Duration
}

type settingsFieldUpdater func(form url.Values, settings *mvServiceRuntimeSettings) (changed bool, err error)

// mvServiceSettingsFieldUpdaters defines how each form field is parsed, validated, and applied.
var mvServiceSettingsFieldUpdaters = []settingsFieldUpdater{
	newSettingsFieldUpdater(mvServiceTaskMaxConcurrencyFormField, strconv.Atoi, func(v int) bool { return v > 0 }, func(settings *mvServiceRuntimeSettings, v int) {
		settings.maxConcurrency = v
	}),
	newSettingsFieldUpdater(mvServiceTaskTimeoutFormField, time.ParseDuration, func(v time.Duration) bool { return v >= 0 }, func(settings *mvServiceRuntimeSettings, v time.Duration) {
		settings.timeout = v
	}),
	newSettingsFieldUpdater(mvServiceBackpressureEnabledFormField, strconv.ParseBool, nil, func(settings *mvServiceRuntimeSettings, v bool) {
		settings.backpressureCfg.Enabled = v
	}),
	newSettingsFieldUpdater(mvServiceBackpressureCPUThresholdFormField, parseFloat64FieldValue, nil, func(settings *mvServiceRuntimeSettings, v float64) {
		settings.backpressureCfg.CPUThreshold = v
	}),
	newSettingsFieldUpdater(mvServiceBackpressureMemThresholdFormField, parseFloat64FieldValue, nil, func(settings *mvServiceRuntimeSettings, v float64) {
		settings.backpressureCfg.MemThreshold = v
	}),
	newSettingsFieldUpdater(mvServiceBackpressureDelayFormField, time.ParseDuration, nil, func(settings *mvServiceRuntimeSettings, v time.Duration) {
		settings.backpressureCfg.Delay = v
	}),
	newSettingsFieldUpdater(mvServiceTaskFailRetryBaseDelayFormField, time.ParseDuration, nil, func(settings *mvServiceRuntimeSettings, v time.Duration) {
		settings.retryBase = v
	}),
	newSettingsFieldUpdater(mvServiceTaskFailRetryMaxDelayFormField, time.ParseDuration, nil, func(settings *mvServiceRuntimeSettings, v time.Duration) {
		settings.retryMax = v
	}),
}

// MVServiceSettingsResponse is MV service runtime settings response.
type MVServiceSettingsResponse struct {
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

// ServeHTTP handles request of get/update MV service settings.
func (h MVServiceSettingsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	mvService, err := h.getMVService()
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	switch req.Method {
	case http.MethodGet:
		h.serveGet(w, mvService)
	case http.MethodPost:
		h.servePost(w, req, mvService)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// getMVService loads the MV service from the domain and verifies that it is enabled.
func (h MVServiceSettingsHandler) getMVService() (mvServiceRuntimeSettingsAccessor, error) {
	do, err := session.GetDomain(h.Store)
	if err != nil {
		return nil, err
	}
	mvService := do.GetMVService()
	if mvService == nil {
		return nil, errors.New("mv service is not enabled")
	}
	return mvService, nil
}

// serveGet returns the current runtime settings.
func (h MVServiceSettingsHandler) serveGet(w http.ResponseWriter, mvService mvServiceRuntimeSettingsAccessor) {
	writeMVServiceSettingsResponse(w, loadMVServiceRuntimeSettings(mvService))
}

// servePost parses form values, applies valid updates, and returns the latest runtime settings.
func (h MVServiceSettingsHandler) servePost(w http.ResponseWriter, req *http.Request, mvService mvServiceRuntimeSettingsAccessor) {
	if err := req.ParseForm(); err != nil {
		handler.WriteError(w, err)
		return
	}

	// Parse updates against current settings so unspecified fields are preserved.
	current := loadMVServiceRuntimeSettings(mvService)
	updated, changed, err := parseMVServiceSettingsUpdateFromForm(req.Form, current)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	if !changed {
		handler.WriteError(w, errors.New("at least one setting field must be provided"))
		return
	}

	// Apply settings through service setters, which perform final validation.
	if err := applyMVServiceSettings(mvService, updated); err != nil {
		handler.WriteError(w, err)
		return
	}

	writeMVServiceSettingsResponse(w, loadMVServiceRuntimeSettings(mvService))
}

// loadMVServiceRuntimeSettings reads all mutable runtime settings from the service.
func loadMVServiceRuntimeSettings(mvService mvServiceRuntimeSettingsAccessor) mvServiceRuntimeSettings {
	maxConcurrency, timeout := mvService.GetTaskExecConfig()
	backpressureCfg := mvService.GetTaskBackpressureConfig()
	retryBase, retryMax := mvService.GetRetryDelayConfig()
	return mvServiceRuntimeSettings{
		maxConcurrency:  maxConcurrency,
		timeout:         timeout,
		backpressureCfg: backpressureCfg,
		retryBase:       retryBase,
		retryMax:        retryMax,
	}
}

// writeMVServiceSettingsResponse writes runtime settings in API response format.
func writeMVServiceSettingsResponse(w http.ResponseWriter, settings mvServiceRuntimeSettings) {
	handler.WriteData(w, MVServiceSettingsResponse{
		TaskMaxConcurrency:           settings.maxConcurrency,
		TaskTimeout:                  settings.timeout.String(),
		TaskTimeoutNanos:             int64(settings.timeout),
		TaskBackpressureEnabled:      settings.backpressureCfg.Enabled,
		TaskBackpressureCPUThreshold: settings.backpressureCfg.CPUThreshold,
		TaskBackpressureMemThreshold: settings.backpressureCfg.MemThreshold,
		TaskBackpressureDelay:        settings.backpressureCfg.Delay.String(),
		TaskBackpressureDelayNanos:   int64(settings.backpressureCfg.Delay),
		RetryBaseDelay:               settings.retryBase.String(),
		RetryBaseDelayNanos:          int64(settings.retryBase),
		RetryMaxDelay:                settings.retryMax.String(),
		RetryMaxDelayNanos:           int64(settings.retryMax),
	})
}

// parseMVServiceSettingsUpdateFromForm parses and validates form updates.
// It returns the merged settings and whether at least one field was changed.
func parseMVServiceSettingsUpdateFromForm(form url.Values, current mvServiceRuntimeSettings) (mvServiceRuntimeSettings, bool, error) {
	updated := current
	changed := false

	for _, updater := range mvServiceSettingsFieldUpdaters {
		fieldChanged, err := updater(form, &updated)
		if err != nil {
			return mvServiceRuntimeSettings{}, false, err
		}
		changed = changed || fieldChanged
	}

	return updated, changed, nil
}

// newSettingsFieldUpdater builds a reusable updater for one form field.
func newSettingsFieldUpdater[T any](
	field string,
	parse func(string) (T, error),
	validate func(T) bool,
	assign func(settings *mvServiceRuntimeSettings, value T),
) settingsFieldUpdater {
	return func(form url.Values, settings *mvServiceRuntimeSettings) (changed bool, err error) {
		value, ok, err := parseOptionalFieldValue(form, field, parse)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if validate != nil && !validate(value) {
			return false, newIllegalMVServiceSettingsFieldError(field)
		}
		assign(settings, value)
		return true, nil
	}
}

// parseOptionalFieldValue parses a field only when it is provided.
func parseOptionalFieldValue[T any](form url.Values, field string, parse func(string) (T, error)) (value T, ok bool, err error) {
	text := form.Get(field)
	if text == "" {
		return value, false, nil
	}
	value, err = parse(text)
	if err != nil {
		return value, false, newIllegalMVServiceSettingsFieldError(field)
	}
	return value, true, nil
}

// parseFloat64FieldValue parses a float64 from text.
func parseFloat64FieldValue(text string) (float64, error) {
	return strconv.ParseFloat(text, 64)
}

// newIllegalMVServiceSettingsFieldError builds a uniform field validation error.
func newIllegalMVServiceSettingsFieldError(field string) error {
	return errors.New("illegal " + field)
}

// applyMVServiceSettings writes merged runtime settings back to the service.
func applyMVServiceSettings(mvService mvServiceRuntimeSettingsAccessor, settings mvServiceRuntimeSettings) error {
	mvService.SetTaskExecConfig(settings.maxConcurrency, settings.timeout)
	if err := mvService.SetTaskBackpressureConfig(settings.backpressureCfg); err != nil {
		return err
	}
	if err := mvService.SetRetryDelayConfig(settings.retryBase, settings.retryMax); err != nil {
		return err
	}
	return nil
}

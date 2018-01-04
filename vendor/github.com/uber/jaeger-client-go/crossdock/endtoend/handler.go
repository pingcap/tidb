// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package endtoend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-client-go/crossdock/common"
	"github.com/uber/jaeger-client-go/crossdock/log"
)

var (
	defaultSamplerType = jaeger.SamplerTypeRemote

	endToEndConfig = config.Configuration{
		Disabled: false,
		Sampler: &config.SamplerConfig{
			Type:                    defaultSamplerType,
			Param:                   1.0,
			SamplingServerURL:       "http://test_driver:5778/sampling",
			SamplingRefreshInterval: 5 * time.Second,
		},
		Reporter: &config.ReporterConfig{
			BufferFlushInterval: time.Second,
			LocalAgentHostPort:  "test_driver:5775",
		},
	}
)

/*Handler handles creating traces from a http request.
 *
 * json: {
 *   "type": "remote",
 *   "operation": "operationName",
 *   "count": 2,
 *   "tags": {
 *     "key": "value"
 *   }
 * }
 *
 * Given the above json payload, the handler will use a tracer with the RemotelyControlledSampler
 * to create 2 traces for "operationName" operation with the tags: {"key":"value"}. These traces
 * are reported to the agent with the hostname "test_driver".
 */
type Handler struct {
	sync.RWMutex

	tracers map[string]opentracing.Tracer
}

type traceRequest struct {
	Type      string            `json:"type"`
	Operation string            `json:"operation"`
	Tags      map[string]string `json:"tags"`
	Count     int               `json:"count"`
}

// NewHandler returns a Handler.
func NewHandler() *Handler {
	return &Handler{
		tracers: make(map[string]opentracing.Tracer),
	}
}

// init initializes the handler with a tracer
func (h *Handler) init(cfg config.Configuration) error {
	tracer, _, err := cfg.New(common.DefaultTracerServiceName)
	if err != nil {
		return err
	}
	h.tracers[cfg.Sampler.Type] = tracer
	return nil
}

func (h *Handler) getTracer(samplerType string) opentracing.Tracer {
	if samplerType == "" {
		samplerType = defaultSamplerType
	}
	h.Lock()
	defer h.Unlock()
	tracer, ok := h.tracers[samplerType]
	if !ok {
		endToEndConfig.Sampler.Type = samplerType
		if err := h.init(endToEndConfig); err != nil {
			log.Printf("Failed to create tracer: %s", err.Error())
			return nil
		}
		tracer, _ = h.tracers[samplerType]
	}
	return tracer
}

// GenerateTraces creates traces given the parameters in the request.
func (h *Handler) GenerateTraces(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var req traceRequest
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("JSON payload is invalid: %s", err.Error()), http.StatusBadRequest)
		return
	}
	tracer := h.getTracer(req.Type)
	if tracer == nil {
		http.Error(w, "Tracer is not initialized", http.StatusInternalServerError)
		return
	}
	generateTraces(tracer, &req)
}

func generateTraces(tracer opentracing.Tracer, r *traceRequest) {
	for i := 0; i < r.Count; i++ {
		span := tracer.StartSpan(r.Operation)
		for k, v := range r.Tags {
			span.SetTag(k, v)
		}
		span.Finish()
	}
}

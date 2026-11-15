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

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
	"go.uber.org/zap"
)

const (
	spfreshAdminBodyLimit      = 1 << 20
	spfreshAdminResponseLimit  = 64 << 10
	spfreshAdminFanoutLimit    = 8
	spfreshAdminRequestTimeout = 5 * time.Second
)

// spfreshAdminNodeResult deliberately keeps the effective configuration opaque
// to TiDB. The worker owns the process-runtime schema and validation rules; the
// TiDB fanout must not grow a second, drifting copy of them.
type spfreshAdminNodeResult struct {
	Address   string          `json:"address"`
	Result    string          `json:"result"`
	Error     string          `json:"error,omitempty"`
	Effective json.RawMessage `json:"effective,omitempty"`
}

type spfreshAdminResponse struct {
	Result string                   `json:"result,omitempty"`
	Config map[string]any           `json:"config,omitempty"`
	Nodes  []spfreshAdminNodeResult `json:"nodes"`
}

// decodeSPFreshAdminBody validates only the transport-level contract. The
// cloud worker is the single owner of the process-runtime field schema,
// accepted units, ranges, and online-update semantics.
func decodeSPFreshAdminBody(data []byte) error {
	if len(data) == 0 {
		return errors.New("request body must not be empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if _, ok := value.(map[string]any); !ok {
		return errors.New("request body must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return err
	}
	return nil
}

func (s *Server) handleSPFreshAdminConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var body []byte
	if r.Method == http.MethodPost {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, spfreshAdminBodyLimit+1))
		if err != nil {
			serveError(w, http.StatusBadRequest, err.Error())
			return
		}
		if len(body) > spfreshAdminBodyLimit {
			serveError(w, http.StatusRequestEntityTooLarge, "SPFresh admin config body is too large")
			return
		}
		if err := decodeSPFreshAdminBody(body); err != nil {
			serveError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	if s.dom == nil || s.dom.GetPDClient() == nil {
		serveError(w, http.StatusInternalServerError, "PD client is unavailable")
		return
	}
	endpoints, err := tikvapi.NormalizeEndpoints(config.GetGlobalConfig().TiKVAPIServiceAddrs, true)
	if err != nil {
		serveError(w, http.StatusInternalServerError, err.Error())
		return
	}
	clusterID := s.dom.GetPDClient().GetClusterID(r.Context())
	ctx, cancel := context.WithTimeout(r.Context(), spfreshAdminRequestTimeout)
	defer cancel()

	method := http.MethodGet
	if r.Method == http.MethodPost {
		method = http.MethodPost
	}
	nodes := fanoutSPFreshAdminConfig(ctx, util.InternalHTTPClient(), clusterID, endpoints, method, body)
	status := spfreshAdminAggregateStatus(method, nodes)
	response := spfreshAdminResponse{Nodes: nodes}
	if r.Method == http.MethodPost {
		if status == http.StatusOK {
			response.Result = "ok"
		} else {
			response.Result = "partial_failure"
		}
		response.Config = effectiveSPFreshAdminConfig(nodes)
	}
	writeSPFreshAdminJSON(w, status, response)
}

func spfreshAdminAggregateStatus(method string, nodes []spfreshAdminNodeResult) int {
	if allSPFreshAdminNodesOK(nodes) {
		return http.StatusOK
	}
	if method == http.MethodPost && len(nodes) > 0 {
		allUnsupported := true
		allInvalid := true
		for _, node := range nodes {
			allUnsupported = allUnsupported && node.Result == "unsupported"
			allInvalid = allInvalid && node.Result == "invalid"
		}
		if allUnsupported {
			return http.StatusUnprocessableEntity
		}
		if allInvalid {
			return http.StatusBadRequest
		}
	}
	return http.StatusServiceUnavailable
}

func writeSPFreshAdminJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		logutil.BgLogger().Warn("write SPFresh admin response failed", zap.Error(err))
	}
}

func allSPFreshAdminNodesOK(nodes []spfreshAdminNodeResult) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, node := range nodes {
		if node.Result != "ok" {
			return false
		}
	}
	return true
}

func effectiveSPFreshAdminConfig(nodes []spfreshAdminNodeResult) map[string]any {
	for _, node := range nodes {
		if node.Result != "ok" || len(node.Effective) == 0 {
			continue
		}
		var effective map[string]any
		if err := json.Unmarshal(node.Effective, &effective); err == nil {
			return effective
		}
	}
	return nil
}

func fanoutSPFreshAdminConfig(
	ctx context.Context,
	client *http.Client,
	clusterID uint64,
	endpoints []string,
	method string,
	body []byte,
) []spfreshAdminNodeResult {
	results := make([]spfreshAdminNodeResult, len(endpoints))
	sem := make(chan struct{}, spfreshAdminFanoutLimit)
	var wg sync.WaitGroup
	for index, endpoint := range endpoints {
		index, endpoint := index, endpoint
		results[index].Address = endpoint
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[index].Result = classifySPFreshAdminTransportError(ctx.Err())
				results[index].Error = ctx.Err().Error()
				return
			}
			defer func() { <-sem }()

			url := util.ComposeURL(endpoint, "/spfresh/admin/config?cluster_id="+strconv.FormatUint(clusterID, 10))
			var requestBody io.Reader
			if method == http.MethodPost {
				requestBody = bytes.NewReader(body)
			}
			request, err := http.NewRequestWithContext(ctx, method, url, requestBody)
			if err != nil {
				results[index].Result = "unreachable"
				results[index].Error = err.Error()
				return
			}
			if method == http.MethodPost {
				request.Header.Set("Content-Type", "application/json")
			}
			response, err := client.Do(request)
			if err != nil {
				results[index].Result = classifySPFreshAdminTransportError(err)
				results[index].Error = err.Error()
				return
			}
			responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, spfreshAdminResponseLimit+1))
			response.Body.Close()
			if readErr != nil {
				results[index].Result = "error"
				results[index].Error = readErr.Error()
				return
			}
			if len(responseBody) > spfreshAdminResponseLimit {
				results[index].Result = "error"
				results[index].Error = "worker response is too large"
				return
			}
			if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
				results[index].Result = classifySPFreshAdminHTTPStatus(response.StatusCode)
				results[index].Error = strings.TrimSpace(string(responseBody))
				return
			}

			var payload struct {
				Effective json.RawMessage `json:"effective"`
			}
			if err := json.Unmarshal(responseBody, &payload); err != nil {
				results[index].Result = "error"
				results[index].Error = err.Error()
				return
			}
			var effective map[string]any
			if len(payload.Effective) == 0 || json.Unmarshal(payload.Effective, &effective) != nil || effective == nil {
				results[index].Result = "error"
				results[index].Error = "worker response has no valid effective config"
				return
			}
			results[index].Result = "ok"
			results[index].Effective = payload.Effective
		}()
	}
	wg.Wait()
	return results
}

func classifySPFreshAdminHTTPStatus(status int) string {
	switch {
	case status == http.StatusUnprocessableEntity:
		return "unsupported"
	case status >= http.StatusBadRequest && status < http.StatusInternalServerError:
		return "invalid"
	default:
		return "error"
	}
}

func classifySPFreshAdminTransportError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "unreachable"
}

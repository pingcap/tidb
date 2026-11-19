// Copyright 2026 PingCAP, Inc.
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
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlsession "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/naming"
)

const maxActiveJobsKeyspacesPerRequest = 1000

type keyspaceJobCounts struct {
	DDLJobs    int64 `json:"ddl_jobs"`
	ImportJobs int64 `json:"import_jobs"`
}

type keyspaceJobResult struct {
	Keyspace string `json:"keyspace"`
	*keyspaceJobCounts
	Error string `json:"error,omitempty"`
}

// KeyspaceActiveJobsHandler counts unfinished DDL and IMPORT INTO jobs in user keyspaces.
type KeyspaceActiveJobsHandler struct {
	*handler.TikvHandlerTool
	checkKeyspace  func(context.Context, string) (keyspaceJobCounts, error)
	acquireRuntime func(context.Context, string, string) (sqlsvrapi.KSRuntimeHandle, error)
}

// NewKeyspaceActiveJobsHandler creates a handler for POST /keyspaces/jobs/active.
func NewKeyspaceActiveJobsHandler(tool *handler.TikvHandlerTool) *KeyspaceActiveJobsHandler {
	return &KeyspaceActiveJobsHandler{TikvHandlerTool: tool}
}

// ServeHTTP implements http.Handler.
func (h *KeyspaceActiveJobsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.Method != http.MethodPost {
		handler.WriteErrorWithCode(w, http.StatusMethodNotAllowed, errors.New("this API only supports POST"))
		return
	}
	var input struct {
		Keyspaces   []string `json:"keyspaces"`
		Concurrency int      `json:"concurrency"`
	}
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&input); err != nil {
		handler.WriteError(w, errors.Annotate(err, "invalid JSON request"))
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		handler.WriteError(w, errors.New("request must contain exactly one JSON object"))
		return
	}
	if len(input.Keyspaces) == 0 {
		handler.WriteError(w, errors.New("keyspaces must not be empty"))
		return
	}
	if len(input.Keyspaces) > maxActiveJobsKeyspacesPerRequest {
		handler.WriteError(w, errors.Errorf("keyspaces must contain at most %d entries", maxActiveJobsKeyspacesPerRequest))
		return
	}
	results := make([]keyspaceJobResult, 0, len(input.Keyspaces))
	seen := make(map[string]struct{}, len(input.Keyspaces))
	for i, ks := range input.Keyspaces {
		if ks == "" || ks == keyspace.System || naming.CheckKeyspaceName(ks) != nil {
			handler.WriteError(w, errors.Errorf("invalid user keyspace at index %d", i))
			return
		}
		if _, ok := seen[ks]; ok {
			continue
		}
		seen[ks] = struct{}{}
		results = append(results, keyspaceJobResult{Keyspace: ks})
	}
	concurrency := min(max(input.Concurrency, 1), max(cpu.GetCPUCount(), 1), len(results))
	ctx, cancel := context.WithTimeout(req.Context(), requestDefaultTimeout)
	defer cancel()
	check := h.checkKeyspace
	if check == nil {
		check = h.countKeyspace
	}
	var next atomic.Int64
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(results) {
					return
				}
				if err := ctx.Err(); err != nil {
					results[i].Error = err.Error()
					continue
				}
				counts, err := check(ctx, results[i].Keyspace)
				if err != nil {
					results[i].Error = err.Error()
				} else {
					results[i].keyspaceJobCounts = &counts
				}
			}
		}()
	}
	workers.Wait()
	handler.WriteData(w, struct {
		Keyspaces []keyspaceJobResult `json:"keyspaces"`
	}{Keyspaces: results})
}

func (h *KeyspaceActiveJobsHandler) countKeyspace(ctx context.Context, targetKS string) (_ keyspaceJobCounts, err error) {
	acquire := h.acquireRuntime
	if acquire == nil {
		dom, domErr := session.GetDomain(h.Store)
		if domErr != nil {
			return keyspaceJobCounts{}, domErr
		}
		acquire = dom.AcquireKSRuntimeWithContext
	}
	runtime, err := acquire(ctx, targetKS, "http/active-jobs/"+uuid.NewString())
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	defer runtime.Release(true)
	pool := runtime.SysSessionPool()
	resource, err := pool.Get()
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	defer func() {
		// A failed or cancelled SQL execution must not return a session with
		// unfinished transaction state to another holder of the runtime.
		if err != nil {
			pool.Destroy(resource)
		} else {
			pool.Put(resource)
		}
	}()
	return countActiveKeyspaceJobs(ctx, resource.(sessionctx.Context))
}

func countActiveKeyspaceJobs(ctx context.Context, se sessionctx.Context) (keyspaceJobCounts, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	jobs, err := ddl.GetAllDDLJobs(ctx, se)
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	var counts keyspaceJobCounts
	for _, job := range jobs {
		// InFinalState also includes paused jobs, which remain active here.
		if !job.IsFinished() && !job.IsSynced() {
			counts.DDLJobs++
		}
	}
	rows, err := ddlsession.NewSession(se).Execute(ctx,
		"select count(1) from mysql.tidb_import_jobs where status in ('pending', 'running')",
		"count_active_import_jobs")
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	counts.ImportJobs = rows[0].GetInt64(0)
	return counts, nil
}

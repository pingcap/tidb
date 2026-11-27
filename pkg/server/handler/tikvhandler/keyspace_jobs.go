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
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/naming"
)

const (
	maxActiveJobsKeyspacesPerRequest = 1000
	maxActiveJobsRequestBytes        = 256 * units.KiB
	activeJobsBatchTimeout           = 2 * time.Minute
)

// Admission is shared by all handlers so overlapping polls cannot multiply
// the per-request cold runtime work and its contention with DXF store opens.
var activeJobsRequestRunning atomic.Bool

type keyspaceJobCounts struct {
	DDLJobs    int64 `json:"ddl_jobs"`
	ImportJobs int64 `json:"import_jobs"`
}

type keyspaceJobResult struct {
	Keyspace string `json:"keyspace"`
	*keyspaceJobCounts
	Error string `json:"error,omitempty"`
}

// KeyspaceActiveJobsHandler counts DDL jobs in the queue and active IMPORT INTO jobs in user keyspaces.
type KeyspaceActiveJobsHandler struct {
	*handler.TikvHandlerTool
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
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxActiveJobsRequestBytes))
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	if err := json.Unmarshal(body, &input); err != nil {
		handler.WriteError(w, errors.Annotate(err, "invalid JSON request"))
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
	if !activeJobsRequestRunning.CompareAndSwap(false, true) {
		handler.WriteErrorWithCode(w, http.StatusTooManyRequests, errors.New("another active-jobs request is in progress"))
		return
	}
	// Keep admission until workers actually exit: a started store open or runtime
	// initialization can outlive request cancellation.
	admissionHeld := true
	defer func() {
		if admissionHeld {
			activeJobsRequestRunning.Store(false)
		}
	}()
	concurrency := min(max(input.Concurrency, 1), max(cpu.GetCPUCount(), 1), len(results))
	ctx, cancel := context.WithTimeout(req.Context(), activeJobsBatchTimeout)
	defer cancel()
	var next atomic.Int64
	var workers util.WaitGroupWrapper
	for range concurrency {
		workers.Run(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(results) {
					return
				}
				counts, err := h.countKeyspace(ctx, results[i].Keyspace)
				if err != nil {
					results[i].Error = err.Error()
				} else {
					results[i].keyspaceJobCounts = &counts
				}
			}
		})
	}
	workers.Wait()
	admissionHeld = false
	activeJobsRequestRunning.Store(false)
	handler.WriteData(w, struct {
		Keyspaces []keyspaceJobResult `json:"keyspaces"`
	}{Keyspaces: results})
}

func (h *KeyspaceActiveJobsHandler) countKeyspace(ctx context.Context, targetKS string) (_ keyspaceJobCounts, resultErr error) {
	if err := ctx.Err(); err != nil {
		return keyspaceJobCounts{}, err
	}
	acquire := h.acquireRuntime
	if acquire == nil {
		// Opening a new store retries missing keyspaces during provisioning.
		// An active-job check can report the missing keyspace immediately.
		if store, ok := h.Store.(kv.StorageWithPD); ok {
			if _, err := store.GetPDClient().LoadKeyspace(ctx, targetKS); err != nil {
				return keyspaceJobCounts{}, errors.Annotatef(err, "failed to load keyspace %s from PD", targetKS)
			}
		}
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
	// Reuse the runtime for later polls and DXF work until normal idle eviction.
	defer runtime.Release(false)
	if err := ctx.Err(); err != nil {
		return keyspaceJobCounts{}, err
	}
	pool := runtime.SysSessionPool()
	resource, err := pool.Get()
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	defer func() {
		if resultErr != nil {
			pool.Destroy(resource)
		} else {
			pool.Put(resource)
		}
	}()
	return countActiveKeyspaceJobs(ctx, resource.(sessionctx.Context))
}

func countActiveKeyspaceJobs(ctx context.Context, se sessionctx.Context) (keyspaceJobCounts, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	ddlJobs, err := queryJobCount(ctx, se, "select count(1) from mysql.tidb_ddl_job")
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	importJobs, err := queryJobCount(ctx, se,
		"select count(1) from mysql.tidb_import_jobs where status in ('pending', 'running')")
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	return keyspaceJobCounts{DDLJobs: ddlJobs, ImportJobs: importJobs}, nil
}

func queryJobCount(ctx context.Context, se sessionctx.Context, query string) (_ int64, resultErr error) {
	rs, err := se.GetSQLExecutor().ExecuteInternal(ctx, query)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := rs.Close(); resultErr == nil {
			resultErr = closeErr
		}
	}()

	chk := rs.NewChunk(nil)
	if err := rs.Next(ctx, chk); err != nil {
		return 0, err
	}
	return chk.GetRow(0).GetInt64(0), nil
}

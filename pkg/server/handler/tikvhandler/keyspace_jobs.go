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
	ddlsession "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/naming"
)

const (
	maxActiveJobsKeyspacesPerRequest = 1000
	maxActiveJobsRequestBytes        = 256 << 10
)

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
	defer runtime.Release(true)
	if err := ctx.Err(); err != nil {
		return keyspaceJobCounts{}, err
	}
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
	ddlJobs, err := countActiveDDLJobs(ctx, se)
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	rows, err := ddlsession.NewSession(se).Execute(ctx,
		"select count(1) from mysql.tidb_import_jobs where status in ('pending', 'running')",
		"count_active_import_jobs")
	if err != nil {
		return keyspaceJobCounts{}, err
	}
	return keyspaceJobCounts{DDLJobs: ddlJobs, ImportJobs: rows[0].GetInt64(0)}, nil
}

func countActiveDDLJobs(ctx context.Context, se sessionctx.Context) (count int64, err error) {
	rs, err := se.GetSQLExecutor().ExecuteInternal(ctx, "select job_meta from mysql.tidb_ddl_job")
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := rs.Close(); err == nil {
			err = closeErr
		}
	}()

	// Job metadata includes the query and DDL arguments. Request a small chunk
	// and reuse it throughout the scan, including when the queue has a backlog.
	chk := rs.NewChunk(nil)
	chk.SetRequiredRows(8, chk.RequiredRows())
	for {
		if err := rs.Next(ctx, chk); err != nil {
			return 0, err
		}
		if chk.NumRows() == 0 {
			return count, nil
		}
		for i := range chk.NumRows() {
			var job model.Job
			if err := job.Decode(chk.GetRow(i).GetBytes(0)); err != nil {
				return 0, err
			}
			// InFinalState also includes paused jobs, which remain active here.
			if !job.IsFinished() && !job.IsSynced() {
				count++
			}
		}
	}
}

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
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceActiveJobs(t *testing.T) {
	unavailable := errors.New("keyspace unavailable")
	t.Run("validation before checks", func(t *testing.T) {
		for _, body := range []string{
			"", "null", "{}", `{"keyspaces":[]}`, `{"keyspaces":[""]}`,
			`{"keyspaces":["SYSTEM"]}`, `{"keyspaces":["bad.name"]}`,
			`{"keyspaces":["https://user:secret@host"]}`,
			`{"keyspaces":["ks"],"concurrency":"2"}`,
			`{"keyspaces":["ks"],"concurrency":1.5}`,
			`{"keyspaces":["ks"]} {}`, `{"keyspaces":["ks"]} trailing`,
		} {
			t.Run(body, func(t *testing.T) {
				h := &KeyspaceActiveJobsHandler{acquireRuntime: func(context.Context, string, string) (sqlsvrapi.KSRuntimeHandle, error) {
					t.Error("invalid request must not check any keyspace")
					return nil, unavailable
				}}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active", strings.NewReader(body)))
				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				require.NotContains(t, w.Body.String(), "secret")
			})
		}
		h := &KeyspaceActiveJobsHandler{}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/keyspaces/jobs/active", nil))
		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
	t.Run("request body size limit", func(t *testing.T) {
		const maxBytes = 256 << 10
		const input = `{"keyspaces":["ks"]}`
		atLimit := input + strings.Repeat(" ", maxBytes-len(input))
		for _, tc := range []struct {
			body string
			code int
		}{
			{atLimit, http.StatusOK},
			{atLimit + " ", http.StatusBadRequest},
			{strings.Repeat(" ", maxBytes) + input, http.StatusBadRequest},
			{input + ` "` + strings.Repeat("x", 2*maxBytes) + `"`, http.StatusBadRequest},
		} {
			body := strings.NewReader(tc.body)
			req := httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active", body)
			req.ContentLength = -1
			w := httptest.NewRecorder()
			var checked bool
			h := &KeyspaceActiveJobsHandler{acquireRuntime: func(context.Context, string, string) (sqlsvrapi.KSRuntimeHandle, error) {
				checked = true
				return nil, unavailable
			}}
			h.ServeHTTP(w, req)
			require.Equal(t, tc.code, w.Code)
			require.Equal(t, tc.code == http.StatusOK, checked)
			require.LessOrEqual(t, len(tc.body)-body.Len(), maxBytes+1)
		}
	})
	t.Run("batch size limit", func(t *testing.T) {
		names := make([]string, 1000)
		for i := range names {
			names[i] = "ks" + strconv.Itoa(i)
		}
		var checked []string
		var deadline time.Time
		h := &KeyspaceActiveJobsHandler{acquireRuntime: func(ctx context.Context, ks, _ string) (sqlsvrapi.KSRuntimeHandle, error) {
			if len(checked) == 0 {
				deadline, _ = ctx.Deadline()
			}
			checked = append(checked, ks)
			return nil, unavailable
		}}
		post := func(names []string) *httptest.ResponseRecorder {
			body, err := json.Marshal(map[string][]string{"keyspaces": names})
			require.NoError(t, err)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active", strings.NewReader(string(body))))
			return w
		}

		started := time.Now()
		require.Equal(t, http.StatusOK, post(names).Code)
		require.Equal(t, names, checked)
		require.GreaterOrEqual(t, deadline.Sub(started), 2*time.Minute)
		require.LessOrEqual(t, time.Until(deadline), 2*time.Minute)

		checked = nil
		w := post(append(names, names[0]))
		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), "at most 1000 entries")
		require.Empty(t, checked)
	})
	t.Run("ordered deduplicated partial results", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		dom.DDL().OwnerManager().CampaignCancel()
		tk := testkit.NewTestKit(t, store)
		var events []string
		runtime := &jobCountTestRuntime{
			pool:   &jobCountTestPool{resource: tk.Session(), events: &events},
			events: &events,
		}
		var calls []string
		h := &KeyspaceActiveJobsHandler{acquireRuntime: func(_ context.Context, ks, _ string) (sqlsvrapi.KSRuntimeHandle, error) {
			calls = append(calls, ks)
			if ks == "unavailable" {
				return nil, unavailable
			}
			return runtime, nil
		}}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active",
			strings.NewReader(`{"keyspaces":["ks2","unavailable","ks2","ks1"]}`)))
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, []string{"ks2", "unavailable", "ks1"}, calls)
		require.JSONEq(t, `{"keyspaces":[{"keyspace":"ks2","ddl_jobs":0,"import_jobs":0},
            {"keyspace":"unavailable","error":"keyspace unavailable"},
            {"keyspace":"ks1","ddl_jobs":0,"import_jobs":0}]}`, w.Body.String())
	})
	t.Run("cancellation stops unchecked work", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls int
		started := make(chan struct{})
		h := &KeyspaceActiveJobsHandler{acquireRuntime: func(ctx context.Context, _, _ string) (sqlsvrapi.KSRuntimeHandle, error) {
			calls++
			if calls == 1 {
				close(started)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		w := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active",
				strings.NewReader(`{"keyspaces":["ks1","ks2","ks3"]}`)).WithContext(ctx))
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("keyspace check did not start")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not join cancelled workers")
		}
		require.Equal(t, 1, calls)
		require.Equal(t, http.StatusOK, w.Code)
		var out struct {
			Keyspaces []map[string]any `json:"keyspaces"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
		require.Len(t, out.Keyspaces, 3)
		for _, result := range out.Keyspaces {
			require.Equal(t, context.Canceled.Error(), result["error"])
			require.NotContains(t, result, "ddl_jobs")
			require.NotContains(t, result, "import_jobs")
		}
	})
}

func TestKeyspaceActiveJobsConcurrency(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(3)")
	unavailable := errors.New("keyspace unavailable")
	for _, tc := range []struct {
		raw  string
		want int
	}{
		{"", 1}, {`,"concurrency":0`, 1}, {`,"concurrency":-2`, 1},
		{`,"concurrency":2`, 2}, {`,"concurrency":100`, 3},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			started := make(chan struct{}, 5)
			unblock := make(chan struct{})
			defer func() {
				if unblock != nil {
					close(unblock)
				}
			}()
			var current, peak atomic.Int32
			h := &KeyspaceActiveJobsHandler{acquireRuntime: func(ctx context.Context, _, _ string) (sqlsvrapi.KSRuntimeHandle, error) {
				n := current.Add(1)
				defer current.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				started <- struct{}{}
				select {
				case <-unblock:
					return nil, unavailable
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active", strings.NewReader(
					`{"keyspaces":["ks1","ks2","ks3","ks4","ks5"]`+tc.raw+`}`)))
			}()
			for range tc.want {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("workers did not reach requested concurrency")
				}
			}
			close(unblock)
			<-done
			unblock = nil
			require.EqualValues(t, tc.want, peak.Load())
			require.Zero(t, current.Load())
			require.Equal(t, http.StatusOK, w.Code)
		})
	}
}

func TestKeyspaceActiveJobsCounts(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	dom.DDL().OwnerManager().CampaignCancel()
	tk := testkit.NewTestKit(t, store)
	counts, err := countActiveKeyspaceJobs(context.Background(), tk.Session())
	require.NoError(t, err)
	require.Equal(t, keyspaceJobCounts{}, counts)
	t.Run("DDL job counts until it leaves the queue", func(t *testing.T) {
		job := &model.Job{ID: 99999, Type: model.ActionCreateTable, State: model.JobStateDone}
		data, err := job.Encode(true)
		require.NoError(t, err)
		tk.MustExec(`insert into mysql.tidb_ddl_job(job_id,reorg,schema_ids,table_ids,job_meta,type,processing)
                values (99999,0,'1','1',?,1,0)`, data)
		counts, err := countActiveKeyspaceJobs(context.Background(), tk.Session())
		tk.MustExec("delete from mysql.tidb_ddl_job where job_id = 99999")
		require.NoError(t, err)
		require.Equal(t, int64(1), counts.DDLJobs)

		counts, err = countActiveKeyspaceJobs(context.Background(), tk.Session())
		require.NoError(t, err)
		require.Zero(t, counts.DDLJobs)
	})
	for _, tc := range []struct {
		status string
		active bool
	}{
		{"pending", true}, {"running", true}, {"finished", false}, {"failed", false}, {"cancelled", false},
	} {
		t.Run("import/"+tc.status, func(t *testing.T) {
			tk.MustExec(`insert into mysql.tidb_import_jobs
                (table_schema,table_name,table_id,created_by,parameters,source_file_size,status,step)
                values ('test','t',1,'root','{}',0,?,'none')`, tc.status)
			counts, err := countActiveKeyspaceJobs(context.Background(), tk.Session())
			require.NoError(t, err)
			want := int64(0)
			if tc.active {
				want = 1
			}
			require.Equal(t, want, counts.ImportJobs)
			tk.MustExec("delete from mysql.tidb_import_jobs")
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = countActiveKeyspaceJobs(ctx, tk.Session())
	require.Error(t, err)
}

type jobCountFailingSession struct {
	sessionapi.Session
	err error
}

func (s *jobCountFailingSession) GetSQLExecutor() sqlexec.SQLExecutor { return s }

func (s *jobCountFailingSession) ExecuteInternal(context.Context, string, ...any) (sqlexec.RecordSet, error) {
	return nil, s.err
}

type jobCountTestPool struct {
	util.DestroyableSessionPool
	resource pools.Resource
	getErr   error
	events   *[]string
}

func (p *jobCountTestPool) Get() (pools.Resource, error) { return p.resource, p.getErr }
func (p *jobCountTestPool) Put(pools.Resource)           { *p.events = append(*p.events, "put") }
func (p *jobCountTestPool) Destroy(pools.Resource)       { *p.events = append(*p.events, "destroy") }

type jobCountTestRuntime struct {
	sqlsvrapi.KSRuntimeHandle
	pool   *jobCountTestPool
	events *[]string
}

func (r *jobCountTestRuntime) SysSessionPool() util.DestroyableSessionPool { return r.pool }
func (r *jobCountTestRuntime) Release(closeIfIdle bool) {
	*r.events = append(*r.events, "release("+strconv.FormatBool(closeIfIdle)+")")
}

func TestKeyspaceActiveJobsRuntimeCleanup(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	dom.DDL().OwnerManager().CampaignCancel()
	tk := testkit.NewTestKit(t, store)
	t.Run("missing keyspace", func(t *testing.T) {
		if !kv.IsSystemKS(store) {
			t.Skip("requires a SYSTEM keyspace")
		}
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore", func(open *func(string) (kv.Storage, error)) {
			*open = func(string) (kv.Storage, error) { return nil, errors.New("unexpected store open") }
		})
		h := NewKeyspaceActiveJobsHandler(handler.NewTikvHandlerTool(store.(helper.Storage)))
		_, err := h.countKeyspace(context.Background(), "missing_ks")
		require.ErrorContains(t, err, "ENTRY_NOT_FOUND")
	})
	queryErr := errors.New("job count query failed")
	for _, tc := range []string{"success", "acquire error", "session error", "query error", "cancellation"} {
		t.Run(tc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var events []string
			pool := &jobCountTestPool{resource: tk.Session(), events: &events}
			runtime := &jobCountTestRuntime{pool: pool, events: &events}
			h := &KeyspaceActiveJobsHandler{acquireRuntime: func(_ context.Context, ks, holder string) (sqlsvrapi.KSRuntimeHandle, error) {
				require.Equal(t, "user_ks", ks)
				require.NotEmpty(t, holder)
				if tc == "acquire error" {
					return nil, errors.New("acquire failed")
				}
				if tc == "cancellation" {
					cancel()
				}
				return runtime, nil
			}}
			if tc == "session error" {
				pool.getErr = errors.New("pool unavailable")
			}
			if tc == "query error" {
				pool.resource = &jobCountFailingSession{Session: tk.Session(), err: queryErr}
			}
			_, err := h.countKeyspace(ctx, "user_ks")
			switch tc {
			case "success":
				require.NoError(t, err)
				require.Equal(t, []string{"put", "release(true)"}, events)
			case "acquire error":
				require.Error(t, err)
				require.Empty(t, events)
			case "session error":
				require.Error(t, err)
				require.Equal(t, []string{"release(true)"}, events)
			case "cancellation":
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, []string{"release(true)"}, events)
			case "query error":
				require.ErrorIs(t, err, queryErr)
				require.Equal(t, []string{"destroy", "release(true)"}, events)
			}
		})
	}
}

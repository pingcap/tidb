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

package ddl_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	ddlsess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

func newStorageClassTransitionPollTest(
	t *testing.T,
	handler http.HandlerFunc,
) (*testkit.TestKit, ddl.DDL, *infoschema.InfoCache) {
	t.Helper()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(metadef.CreateTiDBStorageClassTransitionHistoryTable)
	// Only explicit polls may reconcile the synthetic table below.
	require.NoError(t, dom.DDL().Stop())
	tk.MustExec(`INSERT INTO mysql.tidb_storage_class_transition_history
		(table_schema, table_name, table_id, direction, state, schema_version,
		 start_ts, start_time, physical_targets)
		VALUES ('test', 't', 500, 'TO_IA', 'RUNNING', 1, 100,
		 '2020-01-01 00:00:00', '[{"physical_id":500}]')`)
	tblInfo := &model.TableInfo{
		ID: 500, Name: ast.NewCIStr("t"), StorageClassTier: model.StorageClassTierIA,
	}
	infoCache := infoschema.NewCache(nil, 1)
	infoCache.Insert(infoschema.MockInfoSchemaWithSchemaVer([]*model.TableInfo{tblInfo}, 2), 0)
	d, _ := ddl.NewDDL(context.Background(), ddl.WithStore(store), ddl.WithInfoCache(infoCache))
	t.Cleanup(func() { require.NoError(t, d.Stop()) })

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	previousTiFlash := infosync.GetMockTiFlash()
	infosync.SetMockTiFlash(&infosync.MockTiFlash{StoreInfo: map[uint64]pdhttp.MetaStore{
		1: {ID: 1, StatusAddress: strings.TrimPrefix(server.URL, "http://"), StateName: "Up"},
	}})
	t.Cleanup(func() { infosync.SetMockTiFlash(previousTiFlash) })
	return tk, d, infoCache
}

func TestStorageClassTransitionPollPersistsLastObservation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		initialResponse string
		initialCounters string
		response        string
		counters        string
	}{
		{
			name: "partial progress", initialResponse: `{"ready":1,"total":4}`, initialCounters: "4 1",
			response: `{"ready":3,"total":4}`, counters: "4 3",
		},
		{name: "never observed", counters: "<nil> <nil>"},
		{name: "observed zero replicas", response: `{"ready":0,"total":0}`, counters: "0 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fail atomic.Bool
			var response atomic.Pointer[string]
			fail.Store(tc.response == "")
			response.Store(&tc.response)
			tk, d, infoCache := newStorageClassTransitionPollTest(t, func(w http.ResponseWriter, _ *http.Request) {
				if fail.Load() {
					http.Error(w, "status unavailable", http.StatusServiceUnavailable)
					return
				}
				_, _ = w.Write([]byte(*response.Load()))
			})
			se := ddlsess.NewSession(tk.Session())
			poll := func() {
				_, err := ddl.PollStorageClassTransitionsForTest(context.Background(), d, se)
				require.NoError(t, err)
			}
			checkRunning := func(counters string) {
				tk.MustQuery(`SELECT state, total_replicas, completed_replicas, finish_time, duration
					FROM mysql.tidb_storage_class_transition_history`).Check(
					testkit.Rows("RUNNING " + counters + " <nil> <nil>"))
				statuses := d.StorageClassTransitionStatuses()
				require.Len(t, statuses, 1)
				require.Equal(t, tc.response != "", statuses[0].StatusValid)
			}

			if tc.initialResponse != "" {
				response.Store(&tc.initialResponse)
				poll()
				checkRunning(tc.initialCounters)
				response.Store(&tc.response)
			}
			poll()
			checkRunning(tc.counters)
			// An unchanged observation can affect zero SQL rows without ending
			// the operation. A later failed request must retain these counters.
			poll()
			checkRunning(tc.counters)
			fail.Store(true)
			poll()
			checkRunning(tc.counters)

			// A new manager has no observation cache, as after owner failover.
			// Removing the table must still preserve its durable observation.
			infoCache.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 3), 0)
			newOwner, _ := ddl.NewDDL(context.Background(), ddl.WithStore(tk.Session().GetStore()), ddl.WithInfoCache(infoCache))
			t.Cleanup(func() { require.NoError(t, newOwner.Stop()) })
			_, err := ddl.PollStorageClassTransitionsForTest(context.Background(), newOwner, se)
			require.NoError(t, err)
			tk.MustQuery(`SELECT state, total_replicas, completed_replicas,
				finish_time IS NOT NULL, duration IS NOT NULL
				FROM mysql.tidb_storage_class_transition_history`).Check(
				testkit.Rows("SUPERSEDED " + tc.counters + " 1 1"))
			require.Empty(t, newOwner.StorageClassTransitionStatuses())
		})
	}
}

func TestStorageClassTransitionPollDoesNotOverwriteTerminalHistory(t *testing.T) {
	for _, state := range []string{"SUPERSEDED", "COMPLETED"} {
		t.Run(state, func(t *testing.T) {
			var block atomic.Bool
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			tk, d, _ := newStorageClassTransitionPollTest(t, func(w http.ResponseWriter, _ *http.Request) {
				if block.Load() {
					entered <- struct{}{}
					<-release
					_, _ = w.Write([]byte(`{"ready":2,"total":4}`))
					return
				}
				_, _ = w.Write([]byte(`{"ready":3,"total":4}`))
			})
			se := ddlsess.NewSession(tk.Session())
			_, err := ddl.PollStorageClassTransitionsForTest(context.Background(), d, se)
			require.NoError(t, err)
			tk.MustQuery(`SELECT total_replicas, completed_replicas
				FROM mysql.tidb_storage_class_transition_history`).Check(testkit.Rows("4 3"))

			writer := testkit.NewTestKit(t, tk.Session().GetStore())
			block.Store(true)
			pollDone := make(chan error, 1)
			pollExited := make(chan struct{})
			pollCtx, cancel := context.WithCancel(context.Background())
			// Join the poll before closing its session or store, including when
			// an assertion interrupts the test while the response is blocked.
			t.Cleanup(func() {
				cancel()
				unblock()
				<-pollExited
			})
			go func() {
				defer close(pollExited)
				_, pollErr := ddl.PollStorageClassTransitionsForTest(pollCtx, d, se)
				pollDone <- pollErr
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("poll did not request a storage class observation")
			}
			completedReplicas := 3
			if state == "COMPLETED" {
				completedReplicas = 4
			}
			writer.MustExec(fmt.Sprintf(`UPDATE mysql.tidb_storage_class_transition_history
				SET state = '%s', completed_replicas = %d, finish_time = '2020-01-01 00:00:01', duration = 1
				WHERE table_id = 500 AND start_ts = 100`, state, completedReplicas))
			// A new operation for the same table and direction must also be
			// protected from the old operation's in-flight observation.
			writer.MustExec(`INSERT INTO mysql.tidb_storage_class_transition_history
				(table_schema, table_name, table_id, direction, state, schema_version,
				 start_ts, start_time, physical_targets, total_replicas, completed_replicas)
				VALUES ('test', 't', 500, 'TO_IA', 'RUNNING', 3, 200,
				 '2020-01-01 00:00:01', '[{"physical_id":500}]', 9, 1)`)
			unblock()
			select {
			case err := <-pollDone:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("poll did not finish after its response was released")
			}
			tk.MustQuery(`SELECT start_ts, state, total_replicas, completed_replicas, duration
				FROM mysql.tidb_storage_class_transition_history ORDER BY start_ts`).Check(testkit.Rows(
				fmt.Sprintf("100 %s 4 %d 1", state, completedReplicas),
				"200 RUNNING 9 1 <nil>",
			))
		})
	}

	t.Run("newer owner progress", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		var requestCount atomic.Int32
		tk, oldOwner, infoCache := newStorageClassTransitionPollTest(t, func(w http.ResponseWriter, _ *http.Request) {
			if requestCount.Add(1) == 1 {
				close(entered)
				<-release
				_, _ = w.Write([]byte(`{"ready":2,"total":4}`))
				return
			}
			_, _ = w.Write([]byte(`{"ready":3,"total":4}`))
		})
		oldSession := ddlsess.NewSession(tk.Session())
		oldPollDone := make(chan error, 1)
		oldPollExited := make(chan struct{})
		go func() {
			defer close(oldPollExited)
			_, err := ddl.PollStorageClassTransitionsForTest(context.Background(), oldOwner, oldSession)
			oldPollDone <- err
		}()
		t.Cleanup(func() {
			unblock()
			<-oldPollExited
		})
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("old owner did not request a storage class observation")
		}

		newOwner, _ := ddl.NewDDL(context.Background(), ddl.WithStore(tk.Session().GetStore()), ddl.WithInfoCache(infoCache))
		t.Cleanup(func() { require.NoError(t, newOwner.Stop()) })
		newSession := ddlsess.NewSession(testkit.NewTestKit(t, tk.Session().GetStore()).Session())
		_, err := ddl.PollStorageClassTransitionsForTest(context.Background(), newOwner, newSession)
		require.NoError(t, err)
		tk.MustQuery(`SELECT total_replicas, completed_replicas
			FROM mysql.tidb_storage_class_transition_history`).Check(testkit.Rows("4 3"))

		unblock()
		select {
		case err := <-oldPollDone:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("old owner poll did not finish")
		}
		tk.MustQuery(`SELECT total_replicas, completed_replicas
			FROM mysql.tidb_storage_class_transition_history`).Check(testkit.Rows("4 3"))
	})
}

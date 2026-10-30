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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	ddlsess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestStorageClassTransitionPollWaitsForTopology(t *testing.T) {
	for _, reconciliationFails := range []bool{false, true} {
		name := "partition reorganization"
		if reconciliationFails {
			name = "reconciliation failure"
		}
		t.Run(name, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec(metadef.CreateTiDBStorageClassTransitionHistoryTable)
			// Poll explicitly against controlled schema snapshots. The domain's
			// background owner must not reconcile the synthetic table as orphaned.
			require.NoError(t, dom.DDL().Stop())
			tk.MustExec(`INSERT INTO mysql.tidb_storage_class_transition_history
				(table_schema, table_name, table_id, partition_name, partition_id,
				 direction, state, schema_version, start_ts, start_time, physical_targets)
				VALUES ('test', 't', 500, 'p0', 501, 'TO_IA', 'RUNNING', 1, 100,
				 '2020-01-01 00:00:00',
				 '[{"physical_id":501,"partition_id":501,"partition_name":"p0"}]')`)

			oldPartition := model.PartitionDefinition{
				ID: 501, Name: ast.NewCIStr("p0"), StorageClassTier: model.StorageClassTierIA,
			}
			newPartition := model.PartitionDefinition{
				ID: 502, Name: ast.NewCIStr("p1"), StorageClassTier: model.StorageClassTierIA,
			}
			tblInfo := &model.TableInfo{
				ID: 500, Name: ast.NewCIStr("t"),
				Partition: &model.PartitionInfo{
					Definitions: []model.PartitionDefinition{newPartition},
				},
			}
			if !reconciliationFails {
				// REORGANIZE has switched the public definitions, but retains the
				// old physical range for double writes until the final DDL steps.
				tblInfo.Partition.DDLState = model.StateDeleteReorganization
				tblInfo.Partition.AddingDefinitions = []model.PartitionDefinition{newPartition}
				tblInfo.Partition.DroppingDefinitions = []model.PartitionDefinition{oldPartition}
			}
			infoCache := infoschema.NewCache(nil, 1)
			infoCache.Insert(infoschema.MockInfoSchemaWithSchemaVer([]*model.TableInfo{tblInfo}, 2), 0)
			d, _ := ddl.NewDDL(context.Background(), ddl.WithStore(store), ddl.WithInfoCache(infoCache))
			t.Cleanup(func() { require.NoError(t, d.Stop()) })

			var oldRequests, newRequests atomic.Int32
			var newTargetReady atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Query().Get("table_id") {
				case "501":
					oldRequests.Add(1)
					_, _ = w.Write([]byte(`{"ready":1,"total":1}`))
				case "502":
					newRequests.Add(1)
					if newTargetReady.Load() {
						_, _ = w.Write([]byte(`{"ready":1,"total":1}`))
					} else {
						_, _ = w.Write([]byte(`{"ready":0,"total":1}`))
					}
				default:
					http.Error(w, "unexpected physical target", http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			previousTiFlash := infosync.GetMockTiFlash()
			infosync.SetMockTiFlash(&infosync.MockTiFlash{StoreInfo: map[uint64]pdhttp.MetaStore{
				1: {ID: 1, StatusAddress: strings.TrimPrefix(server.URL, "http://"), StateName: "Up"},
			}})
			t.Cleanup(func() { infosync.SetMockTiFlash(previousTiFlash) })

			const insertFailpoint = "github.com/pingcap/tidb/pkg/ddl/mockInsertStorageClassTransitionError"
			if reconciliationFails {
				testfailpoint.Enable(t, insertFailpoint, "return(true)")
			}
			se := ddlsess.NewSession(tk.Session())
			poll := func() {
				_, err := ddl.PollStorageClassTransitionsForTest(context.Background(), d, se)
				require.NoError(t, err)
			}
			poll()
			tk.MustQuery(`SELECT partition_id, state FROM mysql.tidb_storage_class_transition_history`).Check(
				testkit.Rows("501 RUNNING"))
			require.Zero(t, oldRequests.Load(), "an obsolete physical target must not determine completion")
			require.Zero(t, newRequests.Load())

			if reconciliationFails {
				testfailpoint.Disable(t, insertFailpoint)
			} else {
				stable := tblInfo.Clone()
				stable.Partition.DDLState = model.StateNone
				stable.Partition.AddingDefinitions = nil
				stable.Partition.DroppingDefinitions = nil
				infoCache.Insert(infoschema.MockInfoSchemaWithSchemaVer([]*model.TableInfo{stable}, 3), 0)
			}
			poll()
			tk.MustQuery(`SELECT partition_id, state FROM mysql.tidb_storage_class_transition_history
				ORDER BY partition_id`).Check(testkit.Rows("501 SUPERSEDED", "502 RUNNING"))
			poll()
			tk.MustQuery(`SELECT partition_id, state FROM mysql.tidb_storage_class_transition_history
				ORDER BY partition_id`).Check(testkit.Rows("501 SUPERSEDED", "502 RUNNING"))
			require.EqualValues(t, 1, newRequests.Load())

			newTargetReady.Store(true)
			poll()
			tk.MustQuery(`SELECT partition_id, state FROM mysql.tidb_storage_class_transition_history
				ORDER BY partition_id`).Check(testkit.Rows("501 SUPERSEDED", "502 COMPLETED"))
			require.EqualValues(t, 2, newRequests.Load())
			require.Zero(t, oldRequests.Load())
		})
	}
}

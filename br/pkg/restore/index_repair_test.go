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

package restore_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestRepairGlobalIndexes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		definition string
	}{
		{"integer_handle", "id int primary key clustered, a int, b varchar(30), key local_idx(id), key idx(a) global, unique key uk(a) global"},
		{"common_handle", "id int, a int, b varchar(30), primary key(id,b) clustered, key local_idx(id), key idx(a) global, unique key uk(a) global"},
		{"hidden_handle", "id int, a int, b varchar(30), key local_idx(id), key idx(a) global, unique key uk(a) global"},
		{"global_primary", "id int, a int auto_increment, b varchar(30), primary key(a) nonclustered global, key local_idx(id), key idx(b) global"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := utiltest.CreateRestoreSchemaSuite(t)
			tk := testkit.NewTestKit(t, s.Mock.Storage)
			if tc.name == "hidden_handle" {
				previous := model.GetGlobalIndexV1Supported()
				model.SetGlobalIndexV1Supported(true)
				t.Cleanup(func() { model.SetGlobalIndexV1Supported(previous) })
			}
			tk.MustExec("CREATE TABLE test.t (" + tc.definition + ") PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)")
			tk.MustExec("INSERT INTO test.t VALUES (1,1,'one'),(11,2,'two')")
			if tc.name != "global_primary" {
				tk.MustExec("INSERT INTO test.t VALUES (2,NULL,'null-one'),(12,NULL,'null-two')")
			}
			ctx := context.Background()
			tbl, err := s.Mock.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			before := tbl.Meta()
			if tc.name == "hidden_handle" {
				for _, idx := range before.Indices {
					if idx.Global {
						require.Equal(t, model.GlobalIndexVersionV1, idx.GlobalIndexVersion)
					}
				}
			}
			recorder := ingestrec.New()
			recorder.AddGlobalIndexes(before)
			// Simulate restored index entries still pointing to source partitions.
			require.NoError(t, kv.RunInNewTxn(ctx, s.Mock.Storage, false, func(_ context.Context, txn kv.Transaction) error {
				for _, idx := range before.Indices {
					if !idx.Global {
						continue
					}
					prefix := tablecodec.EncodeTableIndexPrefix(before.ID, idx.ID)
					it, err := txn.Iter(prefix, prefix.PrefixNext())
					if err != nil {
						return err
					}
					for it.Valid() {
						value := append([]byte(nil), it.Value()...)
						segment := tablecodec.SplitIndexValue(value)
						require.Len(t, segment.PartitionID, 8)
						copy(segment.PartitionID, codec.EncodeInt(nil, 99999999))
						key := it.Key().Clone()
						if idx.GlobalIndexVersion == model.GlobalIndexVersionV1 && len(segment.IntHandle) == 0 {
							// V1 nonunique and NULL unique entries also carry the partition ID in the key.
							require.Equal(t, tablecodec.PartitionIDFlag, key[len(key)-18])
							if err := txn.Delete(key); err != nil {
								it.Close()
								return err
							}
							copy(key[len(key)-17:len(key)-9], codec.EncodeInt(nil, 99999999))
						}
						if err := txn.Set(key, value); err != nil {
							it.Close()
							return err
						}
						if err := it.Next(); err != nil {
							it.Close()
							return err
						}
					}
					it.Close()
				}
				return nil
			}))
			tk.MustExec("SET tidb_enable_fast_table_check=0")
			require.Error(t, tk.ExecToErr("ADMIN CHECK TABLE test.t"))
			g := gluetidb.New()
			meta, err := checkpoint.NewSnapshotTableMetaManager(g, s.Mock.Domain, checkpoint.SnapshotRestoreCheckpointDatabaseName, 71052)
			require.NoError(t, err)
			defer meta.Close()
			require.NoError(t, meta.SaveCheckpointMetadata(ctx, nil))
			const failBeforeCreate = "github.com/pingcap/tidb/br/pkg/restore/failed-before-create-ingest-index"
			require.NoError(t, failpoint.Enable(failBeforeCreate, "return(true)"))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(failBeforeCreate)) })
			require.ErrorContains(t, restore.RepairIndexes(ctx, s.Mock.Domain, recorder, meta, g, nil), "failed before create ingest index")
			require.NoError(t, failpoint.Disable(failBeforeCreate))
			require.NoError(t, restore.RepairIndexes(ctx, s.Mock.Domain, nil, meta, g, nil))
			tk.MustExec("ADMIN CHECK TABLE test.t")
			if tc.name == "global_primary" {
				tk.MustQuery("SELECT id,a,b FROM test.t FORCE INDEX(idx) ORDER BY id").Check(testkit.Rows("1 1 one", "11 2 two"))
			} else {
				tk.MustQuery("SELECT id,a,b FROM test.t FORCE INDEX(idx) ORDER BY id").Check(testkit.Rows("1 1 one", "2 <nil> null-one", "11 2 two", "12 <nil> null-two"))
			}
			after, err := s.Mock.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			for _, oldIdx := range before.Indices {
				for _, newIdx := range after.Meta().Indices {
					if oldIdx.Name != newIdx.Name {
						continue
					}
					if oldIdx.Global {
						require.Greater(t, newIdx.ID, oldIdx.ID)
					} else {
						require.Equal(t, oldIdx.ID, newIdx.ID)
					}
				}
			}
			for _, idx := range after.Meta().Indices {
				if idx.Global {
					idx.State = model.StateWriteReorganization
					repairErr := restore.RepairIndexes(ctx, s.Mock.Domain, nil, meta, g, nil)
					idx.State = model.StatePublic
					require.ErrorContains(t, repairErr, "is still being rebuilt")
					break
				}
			}
			// A second invocation must resume persisted SQLs without dropping rebuilt indexes.
			require.NoError(t, restore.RepairIndexes(ctx, s.Mock.Domain, nil, meta, g, nil))
			tk.MustExec("ADMIN CHECK TABLE test.t")
		})
	}
}

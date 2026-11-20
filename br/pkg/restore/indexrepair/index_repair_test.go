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

package indexrepair_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/restore/indexrepair"
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
			const failBeforeCreate = "github.com/pingcap/tidb/br/pkg/restore/indexrepair/failed-before-create-ingest-index"
			require.NoError(t, failpoint.Enable(failBeforeCreate, "return(true)"))
			t.Cleanup(func() { require.NoError(t, failpoint.Disable(failBeforeCreate)) })
			require.ErrorContains(t, indexrepair.RepairIndexes(ctx, s.Mock.Domain, recorder, meta, g, nil), "failed before create ingest index")
			require.NoError(t, failpoint.Disable(failBeforeCreate))
			require.NoError(t, indexrepair.RepairIndexes(ctx, s.Mock.Domain, nil, meta, g, nil))
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
					repairErr := indexrepair.RepairIndexes(ctx, s.Mock.Domain, nil, meta, g, nil)
					idx.State = model.StatePublic
					require.ErrorContains(t, repairErr, "is still being rebuilt")
					break
				}
			}
			// A second invocation must resume persisted SQLs without dropping rebuilt indexes.
			require.NoError(t, indexrepair.RepairIndexes(ctx, s.Mock.Domain, nil, meta, g, nil))
			tk.MustExec("ADMIN CHECK TABLE test.t")
		})
	}
}

func TestRepairForeignKeyQuotedColumns(t *testing.T) {
	for _, tc := range []struct {
		name      string
		childCol  string
		parentCol string
	}{
		{name: "plain", childCol: "pid", parentCol: "id"},
		{name: "reserved_word", childCol: "order", parentCol: "id"},
		{name: "parent_reserved_word", childCol: "pid", parentCol: "order"},
		{name: "space", childCol: "parent id", parentCol: "parent id"},
		{name: "backtick", childCol: "parent`id", parentCol: "parent`id"},
		{name: "percent", childCol: "parent%n", parentCol: "parent%n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := utiltest.CreateRestoreSchemaSuite(t)
			tk := testkit.NewTestKit(t, s.Mock.Storage)
			childCol := strings.ReplaceAll(tc.childCol, "`", "``")
			parentCol := strings.ReplaceAll(tc.parentCol, "`", "``")
			tk.MustExec(fmt.Sprintf("CREATE TABLE test.parent (`%s` int, INDEX i2 (`%s`))", parentCol, parentCol))
			tk.MustExec(fmt.Sprintf("CREATE TABLE test.child (id int, `%s` int, INDEX i1 (`%s`), CONSTRAINT fk FOREIGN KEY (`%s`) REFERENCES test.parent (`%s`) ON DELETE CASCADE ON UPDATE CASCADE)", childCol, childCol, childCol, parentCol))

			ctx := context.Background()
			g := gluetidb.New()
			meta, err := checkpoint.NewSnapshotTableMetaManager(g, s.Mock.Domain, checkpoint.SnapshotRestoreCheckpointDatabaseName, 73534)
			require.NoError(t, err)
			defer meta.Close()
			require.NoError(t, meta.SaveCheckpointMetadata(ctx, nil))

			fakeJob := func(schemaName, tableName string, tableID, indexID int64, indexName, columnName string) *model.Job {
				return &model.Job{
					Version:    model.JobVersion1,
					SchemaName: schemaName,
					TableName:  tableName,
					TableID:    tableID,
					Type:       model.ActionAddIndex,
					State:      model.JobStateSynced,
					RowCount:   1,
					RawArgs:    json.RawMessage(fmt.Sprintf("[%d, false, [], false]", indexID)),
					ReorgMeta:  &model.DDLReorgMeta{ReorgTp: model.ReorgTypeIngest},
					BinlogInfo: &model.HistoryInfo{TableInfo: &model.TableInfo{Indices: []*model.IndexInfo{{
						ID: indexID, Name: ast.NewCIStr(indexName), Columns: []*model.IndexColumn{{Name: ast.NewCIStr(columnName)}},
					}}}},
				}
			}

			infoSchema := s.Mock.InfoSchema()
			child, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("child"))
			require.NoError(t, err)
			parent, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("parent"))
			require.NoError(t, err)
			recorder := ingestrec.New()
			require.NoError(t, recorder.TryAddJob(fakeJob("test", "child", child.ID, child.Indices[0].ID, "i1", tc.childCol), false))
			require.NoError(t, recorder.TryAddJob(fakeJob("test", "parent", parent.ID, parent.Indices[0].ID, "i2", tc.parentCol), false))

			originalFK := child.ForeignKeys[0]
			var repairedFKID int64
			for attempt := range 3 {
				t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
					require.NoError(t, indexrepair.RepairIndexes(ctx, s.Mock.Domain, recorder, meta, g, nil))
					child, err := s.Mock.InfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("child"))
					require.NoError(t, err)
					require.Len(t, child.ForeignKeys, 1)
					fk := child.ForeignKeys[0]
					require.NotEqual(t, originalFK.ID, fk.ID)
					require.Equal(t, originalFK.Cols, fk.Cols)
					require.Equal(t, originalFK.RefCols, fk.RefCols)
					require.Equal(t, originalFK.OnDelete, fk.OnDelete)
					require.Equal(t, originalFK.OnUpdate, fk.OnUpdate)
					require.ErrorContains(t, tk.ExecToErr("INSERT INTO test.child VALUES (1, 42)"), "foreign key constraint fails")
					switch attempt {
					case 0:
						// Resume from a checkpoint with rebuilt indexes but a missing FK.
						tk.MustExec("ALTER TABLE test.child DROP FOREIGN KEY fk")
					case 1:
						repairedFKID = fk.ID
					case 2:
						// A completed retry must preserve the rebuilt FK.
						require.Equal(t, repairedFKID, fk.ID)
					}
				})
				recorder = nil
			}
		})
	}
}

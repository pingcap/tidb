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

package mviewnested

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func involvingSchemaInfoSet(involving []model.InvolvingSchemaInfo) map[string]struct{} {
	got := make(map[string]struct{}, len(involving))
	for _, info := range involving {
		got[info.Database+"\x00"+info.Table] = struct{}{}
	}
	return got
}

func TestNestedMaterializedViewModifyColumn(t *testing.T) {
	for _, multiColumn := range []bool{false, true} {
		t.Run(fmt.Sprintf("multi_column=%t", multiColumn), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t (a int not null, b int not null)")
			tk.MustExec("insert into t values (1, 10)")
			tk.MustExec("create materialized view log on t (a, b)")
			tk.MustExec("create materialized view mv1 (k, l, cnt) as select a, b, count(1) from t group by a, b")
			tk.MustExec("create materialized view log on mv1 (k, l)")
			tk.MustExec("create materialized view mv2 (x, y, cnt) as select k, l, count(1) from mv1 group by k, l")
			tk.MustExec("create materialized view sibling (q, r, cnt) as select k, l, count(1) from mv1 group by k, l")
			tk.MustExec("create materialized view log on mv2 (x, y)")
			tk.MustExec("create materialized view mv3 (i, j, cnt) as select x, y, count(1) from mv2 group by x, y")
			tk.MustExec("create materialized view log on mv3 (i, j)")
			if multiColumn {
				tk.MustExec("alter table t modify column a bigint not null, modify column b bigint not null")
			} else {
				tk.MustExec("alter table t change column a a bigint not null")
			}
			secondType := "int"
			if multiColumn {
				secondType = "bigint"
			}
			tk.MustQuery("select table_name, column_name, data_type from information_schema.columns where table_schema = 'test' and column_name in ('a', 'b', 'k', 'l', 'x', 'y', 'i', 'j', 'q', 'r') order by table_name, ordinal_position").Check(testkit.Rows(
				"$mlog$mv1 k bigint", "$mlog$mv1 l "+secondType,
				"$mlog$mv2 x bigint", "$mlog$mv2 y "+secondType,
				"$mlog$mv3 i bigint", "$mlog$mv3 j "+secondType,
				"$mlog$t a bigint", "$mlog$t b "+secondType,
				"mv1 k bigint", "mv1 l "+secondType,
				"mv2 x bigint", "mv2 y "+secondType,
				"mv3 i bigint", "mv3 j "+secondType,
				"sibling q bigint", "sibling r "+secondType,
				"t a bigint", "t b "+secondType,
			))

			jobID, err := strconv.ParseInt(fmt.Sprint(tk.MustQuery("admin show ddl jobs 1").Rows()[0][0]), 10, 64)
			require.NoError(t, err)
			job, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
			require.NoError(t, err)
			require.Equal(t, map[string]struct{}{
				"test\x00t": {}, "test\x00$mlog$t": {},
				"test\x00mv1": {}, "test\x00$mlog$mv1": {},
				"test\x00mv2": {}, "test\x00$mlog$mv2": {},
				"test\x00mv3": {}, "test\x00$mlog$mv3": {},
				"test\x00sibling": {},
			}, involvingSchemaInfoSet(job.GetInvolvingSchemaInfo()))

			tk.MustExec("insert into t values (2147483648, 20)")
			for _, name := range []string{"mv1", "mv2", "mv3", "sibling"} {
				tk.MustExec("refresh materialized view " + name + " fast")
			}
			tk.MustQuery("select * from mv3 order by i").Check(testkit.Rows("1 10 1", "2147483648 20 1"))
			tk.MustQuery("select * from sibling order by q").Check(testkit.Rows("1 10 1", "2147483648 20 1"))
		})
	}
}

func TestNestedMaterializedViewModifyColumnUnreferencedOutput(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create materialized view mv1 (k, cnt) as select a, count(1) from t group by a")
	tk.MustExec("create materialized view log on mv1 (k, cnt)")
	tk.MustExec("create materialized view mv2 (x, cnt) as select k, count(1) from mv1 group by k")
	tk.MustExec("create materialized view unaffected (c, n) as select cnt, count(1) from mv1 where cnt > 0 group by cnt")
	tk.MustExec("create materialized view log on unaffected (c)")
	tk.MustExec("create materialized view unaffected_nested (z, n) as select c, count(1) from unaffected where c > 0 group by c")
	const unaffectedSQL = "select table_name, column_name, column_type from information_schema.columns where table_schema = 'test' and table_name in ('unaffected', '$mlog$unaffected', 'unaffected_nested') order by table_name, ordinal_position"
	before := tk.MustQuery(unaffectedSQL).Rows()
	tk.MustExec("alter table t modify column a bigint not null")
	tk.MustQuery("select table_name, column_name, data_type from information_schema.columns where table_schema = 'test' and column_name in ('a', 'k', 'x') order by table_name, ordinal_position").Check(testkit.Rows(
		"$mlog$mv1 k bigint", "$mlog$t a bigint", "mv1 k bigint", "mv2 x bigint", "t a bigint",
	))
	require.Equal(t, before, tk.MustQuery(unaffectedSQL).Rows())
}

func TestNestedMaterializedViewModifyColumnNewDescendants(t *testing.T) {
	for _, cacheSize := range []int64{0, 1 << 30} {
		t.Run(fmt.Sprintf("schema_cache_size=%d", cacheSize), func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := newMViewTestKit(t, store)
			originalCacheSize := tk.MustQuery("select @@global.tidb_schema_cache_size").Rows()[0][0]
			defer tk.MustExec(fmt.Sprintf("set @@global.tidb_schema_cache_size = %v", originalCacheSize))
			tk.MustExec(fmt.Sprintf("set @@global.tidb_schema_cache_size = %d", cacheSize))
			tk.MustExec("use test")
			tk.MustExec("create table t (a int not null, b int not null)")
			tk.MustExec("insert into t values (1, 10)")
			tk.MustExec("create materialized view log on t (a, b)")
			tk.MustExec("create materialized view mv1 (k, l, cnt) as select a, b, count(1) from t group by a, b")
			tk.MustExec("create materialized view log on mv1 (k, l)")
			tk.MustExec("create materialized view mv2 (x, y, cnt) as select k, l, count(1) from mv1 group by k, l")
			tk.MustExec("create materialized view log on mv2 (x, y)")

			const alterSQL = "alter table t modify column a bigint not null, modify column b bigint not null"
			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
			require.NoError(t, sessiontxn.NewTxn(ctx, tk.Session()))
			stmt, err := parser.New().ParseOneStmt(alterSQL, "", "")
			require.NoError(t, err)
			is := dom.InfoSchema()
			isV2, _ := infoschema.IsV2(is)
			require.Equal(t, cacheSize > 0, isV2)
			schema, ok := is.SchemaByName(ast.NewCIStr("test"))
			require.True(t, ok)
			base, err := is.TableByName(context.Background(), schema.Name, ast.NewCIStr("t"))
			require.NoError(t, err)
			var jobW *ddl.JobWrapper
			subJobs := make([]*model.SubJob, 0, len(stmt.(*ast.AlterTableStmt).Specs))
			for _, spec := range stmt.(*ast.AlterTableStmt).Specs {
				columnJob, err := ddl.GetModifiableColumnJob(ctx, tk.Session(), is,
					ast.Ident{Schema: schema.Name, Name: base.Meta().Name}, spec.NewColumns[0].Name.Name, schema, base, spec)
				require.NoError(t, err)
				if jobW == nil {
					jobW = columnJob
				}
				subJobs = append(subJobs, &model.SubJob{
					Type: columnJob.Type, JobArgs: columnJob.JobArgs,
					Revertible: true, ReorgTp: columnJob.ReorgMeta.ReorgTp,
					InvolvingSchemaInfo: columnJob.InvolvingSchemaInfo,
				})
			}
			jobW.Type = model.ActionMultiSchemaChange
			jobW.MultiSchemaInfo = &model.MultiSchemaInfo{Revertible: true, SubJobs: subJobs}
			jobW = ddl.NewJobWrapperWithArgs(jobW.Job, &model.EmptyArgs{}, false)

			// These tables are absent from the already constructed job's involving set.
			tk.MustExec("create materialized view mv3 (i, j, cnt) as select x, y, count(1) from mv2 group by x, y")
			tk.MustExec("create materialized view log on mv3 (i, j)")
			// This branch changes only in the second sub-job, which skips schema version generation.
			tk.MustExec("create materialized view mv4 (j, cnt) as select y, count(1) from mv2 group by y")
			tk.MustExec("create materialized view log on mv4 (j)")
			involving := involvingSchemaInfoSet(jobW.GetInvolvingSchemaInfo())
			for _, name := range []string{"mv3", "$mlog$mv3", "mv4", "$mlog$mv4"} {
				require.NotContains(t, involving, "test\x00"+name)
			}
			affectedNames := []string{"$mlog$t", "mv1", "$mlog$mv1", "mv2", "$mlog$mv2", "mv3", "$mlog$mv3", "mv4", "$mlog$mv4"}
			expectedAffectedIDs := make([]int64, 0, len(affectedNames))
			for _, name := range affectedNames {
				// Prime the table cache so a full metadata fetch cannot hide a missing diff entry.
				tbl, err := dom.InfoSchema().TableByName(context.Background(), schema.Name, ast.NewCIStr(name))
				require.NoError(t, err)
				expectedAffectedIDs = append(expectedAffectedIDs, tbl.Meta().ID)
			}
			require.NoError(t, sessiontxn.NewTxn(ctx, tk.Session()))
			tk.Session().SetValue(sessionctx.QueryString, alterSQL)
			require.NoError(t, dom.DDLExecutor().(ddl.ExecutorForTest).DoDDLJobWrapper(tk.Session(), jobW))
			jobID, err := strconv.ParseInt(fmt.Sprint(tk.MustQuery("admin show ddl jobs 1").Rows()[0][0]), 10, 64)
			require.NoError(t, err)
			historyJob, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
			require.NoError(t, err)
			require.Equal(t, model.ActionMultiSchemaChange, historyJob.Type)
			require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
				m := meta.NewMutator(txn)
				for _, tableID := range append(expectedAffectedIDs, base.Meta().ID) {
					tbl, err := m.GetTable(schema.ID, tableID)
					require.NoError(t, err)
					for _, col := range tbl.Columns {
						switch col.Name.L {
						case "a", "b", "k", "l", "x", "y", "i", "j":
							require.Equal(t, mysql.TypeLonglong, col.GetType(), "%s.%s", tbl.Name, col.Name)
						}
					}
				}
				diff, err := m.GetSchemaDiff(historyJob.BinlogInfo.SchemaVersion)
				require.NoError(t, err)
				require.NotNil(t, diff)
				affectedIDs := make([]int64, 0, len(diff.AffectedOpts))
				for _, option := range diff.AffectedOpts {
					affectedIDs = append(affectedIDs, option.TableID)
				}
				require.ElementsMatch(t, expectedAffectedIDs, affectedIDs)
				return nil
			}))
			tk.MustQuery("select table_name, column_name, data_type from information_schema.columns where table_schema = 'test' and column_name in ('a', 'b', 'k', 'l', 'x', 'y', 'i', 'j') order by table_name, ordinal_position").Check(testkit.Rows(
				"$mlog$mv1 k bigint", "$mlog$mv1 l bigint", "$mlog$mv2 x bigint", "$mlog$mv2 y bigint", "$mlog$mv3 i bigint", "$mlog$mv3 j bigint", "$mlog$mv4 j bigint",
				"$mlog$t a bigint", "$mlog$t b bigint", "mv1 k bigint", "mv1 l bigint", "mv2 x bigint", "mv2 y bigint", "mv3 i bigint", "mv3 j bigint", "mv4 j bigint", "t a bigint", "t b bigint",
			))
			tk.MustExec("insert into t values (2147483648, 2147483649)")
			for _, name := range []string{"mv1", "mv2", "mv3", "mv4"} {
				tk.MustExec("refresh materialized view " + name + " fast")
			}
			tk.MustQuery("select * from mv3 order by i").Check(testkit.Rows("1 10 1", "2147483648 2147483649 1"))
			tk.MustQuery("select * from mv4 order by j").Check(testkit.Rows("10 1", "2147483649 1"))
		})
	}
}

func TestNestedMaterializedViewModifyColumnDependencyChangesAfterPrecheck(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv1 (k, l, cnt) as select a, b, count(1) from t group by a, b")
	tk.MustExec("create materialized view log on mv1 (k, l)")
	tk.MustExec("create materialized view mv2 (x, y, cnt) as select k, l, count(1) from mv1 group by k, l")
	tk.MustExec("create materialized view log on mv2 (x, y)")

	const alterSQL = "alter table t modify column a bigint not null, modify column b bigint not null"
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	tkAlter := newMViewTestKit(t, store)
	tkAlter.MustExec("use test")
	require.NoError(t, sessiontxn.NewTxn(ctx, tkAlter.Session()))
	stmt, err := parser.New().ParseOneStmt(alterSQL, "", "")
	require.NoError(t, err)
	is := dom.InfoSchema()
	schema, ok := is.SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	base, err := is.TableByName(context.Background(), schema.Name, ast.NewCIStr("t"))
	require.NoError(t, err)
	var jobW *ddl.JobWrapper
	subJobs := make([]*model.SubJob, 0, len(stmt.(*ast.AlterTableStmt).Specs))
	for _, spec := range stmt.(*ast.AlterTableStmt).Specs {
		columnJob, err := ddl.GetModifiableColumnJob(ctx, tkAlter.Session(), is,
			ast.Ident{Schema: schema.Name, Name: base.Meta().Name}, spec.NewColumns[0].Name.Name, schema, base, spec)
		require.NoError(t, err)
		if jobW == nil {
			jobW = columnJob
		}
		subJobs = append(subJobs, &model.SubJob{
			Type: columnJob.Type, JobArgs: columnJob.JobArgs,
			Revertible: true, ReorgTp: columnJob.ReorgMeta.ReorgTp,
			InvolvingSchemaInfo: columnJob.InvolvingSchemaInfo,
		})
	}
	jobW.Type = model.ActionMultiSchemaChange
	jobW.MultiSchemaInfo = &model.MultiSchemaInfo{Revertible: true, SubJobs: subJobs}
	jobW = ddl.NewJobWrapperWithArgs(jobW.Job, &model.EmptyArgs{}, false)

	tk.MustExec("create materialized view mv3 (i, j, cnt) as select x, y, count(1) from mv2 group by x, y")
	tk.MustExec("create materialized view log on mv3 (i, j)")
	involving := involvingSchemaInfoSet(jobW.GetInvolvingSchemaInfo())
	for _, name := range []string{"mv3", "$mlog$mv3", "blocker"} {
		require.NotContains(t, involving, "test\x00"+name)
	}

	atBoundary := make(chan struct{})
	resume := make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	defer release()
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep", func(job *model.JobW) {
		if job.Type != model.ActionMultiSchemaChange || !job.MultiSchemaInfo.Revertible || job.IsRollingback() {
			return
		}
		for _, sub := range job.MultiSchemaInfo.SubJobs {
			if sub.Revertible || sub.IsFinished() {
				return
			}
		}
		pauseOnce.Do(func() {
			close(atBoundary)
			<-resume
		})
	})
	alterErrCh := make(chan error, 1)
	go func() {
		err := sessiontxn.NewTxn(ctx, tkAlter.Session())
		if err == nil {
			tkAlter.Session().SetValue(sessionctx.QueryString, alterSQL)
			err = dom.DDLExecutor().(ddl.ExecutorForTest).DoDDLJobWrapper(tkAlter.Session(), jobW)
		}
		alterErrCh <- err
	}()
	select {
	case <-atBoundary:
	case <-time.After(15 * time.Second):
		t.Fatal("ALTER did not reach the batch boundary before starting its transaction")
	}

	createErrCh := make(chan error, 1)
	go func() {
		createErrCh <- tk.ExecToErr("create materialized view blocker (z, total, cnt) as select i, sum(j), count(1) from mv3 group by i")
	}()
	select {
	case err := <-createErrCh:
		require.NoError(t, err)
		t.Log("blocker CREATE completed while ALTER was paused before its boundary transaction")
	case <-time.After(15 * time.Second):
		t.Fatal("blocker CREATE was blocked by the paused ALTER")
	}

	names := []string{"t", "$mlog$t", "mv1", "$mlog$mv1", "mv2", "$mlog$mv2", "mv3", "$mlog$mv3", "blocker"}
	tableIDs := make([]int64, 0, len(names))
	for _, name := range names {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), schema.Name, ast.NewCIStr(name))
		require.NoError(t, err)
		tableIDs = append(tableIDs, tbl.Meta().ID)
	}
	release()
	select {
	case err := <-alterErrCh:
		require.ErrorContains(t, err, "non-direct SELECT expressions")
		t.Logf("ALTER failed as expected: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("ALTER did not finish after releasing its boundary")
	}

	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		var changed []string
		for i, tableID := range tableIDs {
			info, err := m.GetTable(schema.ID, tableID)
			require.NoError(t, err)
			for _, col := range info.Columns {
				switch col.Name.L {
				case "a", "b", "k", "l", "x", "y", "i", "j", "z":
					t.Logf("persisted metadata after rejected ALTER: %s.%s type=%d", names[i], col.Name.L, col.GetType())
					if col.GetType() != mysql.TypeLong {
						changed = append(changed, fmt.Sprintf("%s.%s type=%d", names[i], col.Name.L, col.GetType()))
					}
				}
			}
		}
		require.Empty(t, changed, "persisted metadata changed despite rejected ALTER")
		return nil
	}))
}

func TestNestedMaterializedViewModifyColumnRejectsDownstreamUsage(t *testing.T) {
	for _, mode := range []string{"preflight", "worker", "multi_worker"} {
		for _, tc := range []struct {
			name      string
			column    string
			columns   string
			selectSQL string
			err       string
		}{
			{name: "where", column: "a", columns: "(z, cnt)", selectSQL: "select x, count(1) from mv2 where x > 0 group by x", err: "WHERE clause"},
			{name: "aggregate", column: "b", columns: "(z, total, cnt)", selectSQL: "select x, sum(y), count(1) from mv2 group by x", err: "non-direct SELECT expressions"},
		} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				store, dom := testkit.CreateMockStoreAndDomain(t)
				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				tk := newMViewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t (a int not null, b int not null)")
				tk.MustExec("insert into t values (1, 10)")
				tk.MustExec("create materialized view log on t (a, b)")
				tk.MustExec("create materialized view mv1 (k, l, cnt) as select a, b, count(1) from t group by a, b")
				tk.MustExec("create materialized view log on mv1 (k, l)")
				tk.MustExec("create materialized view mv2 (x, y, cnt) as select k, l, count(1) from mv1 group by k, l")
				tk.MustExec("create materialized view log on mv2 (x, y)")
				alterSQL := fmt.Sprintf("alter table t modify column %s bigint not null", tc.column)
				if mode == "multi_worker" {
					alterSQL = "alter table t modify column a bigint not null, modify column b bigint not null"
				}
				var jobW *ddl.JobWrapper
				if mode != "preflight" {
					require.NoError(t, sessiontxn.NewTxn(ctx, tk.Session()))
					stmt, err := parser.New().ParseOneStmt(alterSQL, "", "")
					require.NoError(t, err)
					is := dom.InfoSchema()
					schema, ok := is.SchemaByName(ast.NewCIStr("test"))
					require.True(t, ok)
					base, err := is.TableByName(context.Background(), schema.Name, ast.NewCIStr("t"))
					require.NoError(t, err)
					subJobs := make([]*model.SubJob, 0, len(stmt.(*ast.AlterTableStmt).Specs))
					for _, spec := range stmt.(*ast.AlterTableStmt).Specs {
						columnJob, err := ddl.GetModifiableColumnJob(ctx, tk.Session(), is,
							ast.Ident{Schema: schema.Name, Name: base.Meta().Name}, spec.NewColumns[0].Name.Name, schema, base, spec)
						require.NoError(t, err)
						if jobW == nil {
							jobW = columnJob
						}
						subJobs = append(subJobs, &model.SubJob{
							Type: columnJob.Type, JobArgs: columnJob.JobArgs,
							Revertible: true, ReorgTp: columnJob.ReorgMeta.ReorgTp,
							InvolvingSchemaInfo: columnJob.InvolvingSchemaInfo,
						})
					}
					if mode == "multi_worker" {
						jobW.Type = model.ActionMultiSchemaChange
						jobW.MultiSchemaInfo = &model.MultiSchemaInfo{Revertible: true, SubJobs: subJobs}
						jobW = ddl.NewJobWrapperWithArgs(jobW.Job, &model.EmptyArgs{}, false)
					}
				}
				// Build the worker's job before adding the incompatible downstream dependency.
				tk.MustExec("create materialized view mv3 " + tc.columns + " as " + tc.selectSQL)
				const typesSQL = "select table_name, column_name, column_type from information_schema.columns where table_schema = 'test' order by table_name, ordinal_position"
				before := tk.MustQuery(typesSQL).Rows()
				var err error
				if mode != "preflight" {
					require.NoError(t, sessiontxn.NewTxn(ctx, tk.Session()))
					tk.Session().SetValue(sessionctx.QueryString, alterSQL)
					err = dom.DDLExecutor().(ddl.ExecutorForTest).DoDDLJobWrapper(tk.Session(), jobW)
				} else {
					err = tk.ExecToErr(alterSQL)
				}
				require.ErrorContains(t, err, tc.err)
				require.Equal(t, before, tk.MustQuery(typesSQL).Rows())
				tk.MustQuery("select * from mv2").Check(testkit.Rows("1 10 1"))
			})
		}
	}
}

func TestMaterializedViewNestingDepthLimit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table mv_depth_base (a int not null)")
	tk.MustExec("insert into mv_depth_base values (1), (1), (2)")
	source := "mv_depth_base"
	for depth := 1; depth <= 16; depth++ {
		tk.MustExec(fmt.Sprintf("create materialized view log on %s (a)", source))
		name := fmt.Sprintf("mv_depth_%d", depth)
		tk.MustExec(fmt.Sprintf("create materialized view %s (a, cnt) refresh fast as select a, count(1) from %s group by a", name, source))
		source = name
	}
	tk.MustQuery("select * from mv_depth_16 order by a").Check(testkit.Rows("1 1", "2 1"))
	tk.MustExec("create materialized view log on mv_depth_16 (a)")
	err := tk.ExecToErr("create materialized view mv_depth_17 (a, cnt) refresh fast as select a, count(1) from mv_depth_16 group by a")
	require.ErrorContains(t, err, "materialized view nesting depth exceeds maximum of 16")
	tk.MustQuery("show tables like 'mv_depth_17'").Check(testkit.Rows())
	tk.MustQuery("select * from mv_depth_1 order by a").Check(testkit.Rows("1 2", "2 1"))
	tk.MustQuery("select * from mv_depth_16 order by a").Check(testkit.Rows("1 1", "2 1"))

	parent, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_depth_16"))
	require.NoError(t, err)
	require.Empty(t, parent.Meta().MaterializedViewBase.MViewIDs)
}
